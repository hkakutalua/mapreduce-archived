package mapreduce

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"runtime"
	"strconv"
	"strings"
)

type Node struct {
	Address string
	User    string
}

type MapSpecification struct {
	FilePath string
	M        int
}

type ReduceSpecification struct {
	OutputFile string
	R          int
}

type MapResult struct {
	IntermediateKey string
	Value           string
}

const masterExecName = "master"
const workerExecName = "worker"

func Start(
	masterNode Node,
	workerNodes []Node,
	mapSpec MapSpecification,
	reduceSpec ReduceSpecification,
	functionsPluginSrcPath string,
) {
	_, error := splitInputFile(mapSpec.FilePath, mapSpec.M)

	if error != nil {
		fmt.Printf("Error: %v", error)
		return
	}

	currentDirectory, getWdError := os.Getwd()
	if getWdError != nil {
		fmt.Printf("Could not get current directory, reason: '%v'", getWdError)
		return
	}

	masterSourcePath := currentDirectory + "/master"
	workerSourcePath := currentDirectory + "/worker"
	allNodes := []Node{masterNode}
	copy(allNodes, workerNodes)

	for index, node := range allNodes {
		var programName string
		var programSourcePath string
		var programDestinationPath string
		var programArgs []string
		masterNodeIndex := 0

		if index == masterNodeIndex {
			programSourcePath = masterSourcePath
			programDestinationPath = "$HOME/" + masterExecName
			argsCountWithoutNodes := 3
			programName = masterExecName
			programArgs = make([]string, argsCountWithoutNodes+len(workerNodes)-1)
			programArgs[0], programArgs[1], programArgs[2] =
				strconv.Itoa(mapSpec.M), strconv.Itoa(reduceSpec.R), mapSpec.FilePath

			workerNodesAddresses := make([]string, len(workerNodes))
			for i := 0; i < len(workerNodesAddresses); i++ {
				workerNodesAddresses[i] = workerNodes[i].Address
			}

			copy(programArgs, workerNodesAddresses)
		} else {
			programSourcePath = workerSourcePath
			programDestinationPath = "$HOME/" + workerExecName
			programName = workerExecName
			programArgs = []string{strconv.Itoa(reduceSpec.R)}
		}

		programCopyError := copyFileToNode(node, programDestinationPath, programSourcePath)
		if programCopyError != nil {
			return
		}

		pluginDestinationPath := "$HOME/" + path.Base(functionsPluginSrcPath)
		pluginCopyError := copyFileToNode(node, pluginDestinationPath, functionsPluginSrcPath)
		if pluginCopyError != nil {
			return
		}

		daemonStartError := startDaemonOnNode(
			node, programName, programDestinationPath, programArgs,
		)

		if daemonStartError != nil {
			return
		}
	}
}

func splitInputFile(filePath string, splitsCount int) ([]string, error) {
	splitFilesPaths := make([]string, splitsCount)

	file, fileOpenError := os.Open(filePath)
	if fileOpenError != nil {
		return nil, fileOpenError
	}
	defer file.Close()

	fileInfo, fileStateError := file.Stat()
	if fileStateError != nil {
		return []string{}, fileStateError
	}

	splitSizeInBytes := fileInfo.Size() / int64(splitsCount)
	originalFileReader := bufio.NewReader(file)
	splitFileIndex := 0
	for i := 0; i < splitsCount; i++ {
		splitFileName := strings.Join([]string{filePath, "_" + strconv.Itoa(i)}, "")

		splitFilePath, fileWritingError := createAndWriteToSplitFile(
			splitFileName, originalFileReader, splitSizeInBytes,
		)
		if fileWritingError != nil {
			return []string{}, fileWritingError
		}

		splitFilesPaths[splitFileIndex] = *splitFilePath
		splitFileIndex++
	}

	return splitFilesPaths, nil
}

func createAndWriteToSplitFile(
	splitFileName string,
	originalFileReader *bufio.Reader,
	splitSizeInBytes int64,
) (*string, error) {
	fileSplit, splitCreationError := os.Create(splitFileName)
	if splitCreationError != nil {
		return nil, splitCreationError
	}
	defer fileSplit.Close()

	remainingBytesForSplit := splitSizeInBytes
	for {
		bytes, isPrefix, fileReadError := originalFileReader.ReadLine()
		bytesRead := len(bytes)

		if fileReadError == nil || fileReadError == io.EOF {
			remainingBytesForSplit -= int64(bytesRead)
			_, fileWriteError := fileSplit.Write(bytes[:bytesRead])
			if fileWriteError != nil {
				return nil, fileWriteError
			}

			if isPrefix {
				continue
			} else {
				_, fileWriteError := fileSplit.Write([]byte{'\n'})
				remainingBytesForSplit--
				if fileWriteError != nil {
					return nil, fileWriteError
				}
			}

			if fileReadError == io.EOF {
				break
			}

			if remainingBytesForSplit <= 0 {
				break
			}
		} else {
			return nil, fileReadError
		}
	}

	return &splitFileName, nil
}

func copyFileToNode(node Node, destinationPath string, sourcePath string) error {
	destinationFullPath := fmt.Sprintf("%v@%v:%s", node.User, node.Address, destinationPath)
	command := exec.Command("scp", sourcePath, destinationFullPath)
	fmt.Printf("Command to execute: \"%v\"\n\n", command.String())

	outBytes, copyCommandError := command.CombinedOutput()
	if copyCommandError != nil {
		fmt.Printf("scp execution failed for node %v@%v: %v\n", node.User, node.Address, copyCommandError)
		fmt.Printf("Output: %s\n", outBytes)
		return copyCommandError
	}

	return nil
}

type DaemonError string

func (err DaemonError) Error() string {
	return string(err)
}

func startDaemonOnNode(
	node Node,
	programName,
	programDestinationPath string,
	programArgs []string,
) error {
	switch runtime.GOOS {
	case "linux":
		unitName := programName + ".service"
		systemdAndProgramArgs := []string{
			"--unit=" + unitName,
			fmt.Sprintf("--host=%v@%v", node.User, node.Address),
			programDestinationPath,
		}
		systemdAndProgramArgs = append(systemdAndProgramArgs, programArgs...)

		command := exec.Command("systemd-run", systemdAndProgramArgs...)
		daemonRunCommandError := command.Run()
		if daemonRunCommandError != nil {
			fmt.Printf(
				"'systemd-run' execution failed for node '%v@%v': %v\n",
				node.User,
				node.Address,
				daemonRunCommandError,
			)
			return daemonRunCommandError
		}

	case "darwin":
		command := exec.Command(os.ExpandEnv(programDestinationPath), programArgs...)
		fmt.Printf("Command to execute: \"%v\"\n\n", command.String())
		daemonRunCommandError := command.Run()
		if daemonRunCommandError != nil {
			fmt.Printf(
				"'%v' execution failed for node '%v@%v': %v\n",
				programDestinationPath,
				node.User,
				node.Address,
				daemonRunCommandError,
			)
			return daemonRunCommandError
		}

	default:
		return DaemonError(
			fmt.Sprintf("Failed to start Daemon, operating system"+
				" '%v' not supported", runtime.GOOS),
		)
	}

	return nil
}
