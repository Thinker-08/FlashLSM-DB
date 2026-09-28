package base

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

type FileType int

const (
	FileTypeLog FileType = iota
	FileTypeTable
	FileTypeManifest
	FileTypeCurrent
	FileTypeLock
	FileTypeTemp
	FileTypeInfoLog
)

func (t FileType) String() string {
	switch t {
	case FileTypeLog:
		return "log"
	case FileTypeTable:
		return "table"
	case FileTypeManifest:
		return "manifest"
	case FileTypeCurrent:
		return "current"
	case FileTypeLock:
		return "lock"
	case FileTypeTemp:
		return "temp"
	case FileTypeInfoLog:
		return "info-log"
	}
	return "unknown"
}

const (
	CurrentFilename    = "CURRENT"
	CurrentTmpFilename = "CURRENT.tmp"
	LockFilename       = "LOCK"
	InfoLogFilename    = "LOG"
)

func MakeFilename(fileType FileType, fileNum uint64) string {
	switch fileType {
	case FileTypeLog:
		return fmt.Sprintf("%06d.log", fileNum)
	case FileTypeTable:
		return fmt.Sprintf("%06d.sst", fileNum)
	case FileTypeManifest:
		return fmt.Sprintf("MANIFEST-%06d", fileNum)
	case FileTypeCurrent:
		return CurrentFilename
	case FileTypeLock:
		return LockFilename
	case FileTypeTemp:
		return CurrentTmpFilename
	case FileTypeInfoLog:
		return InfoLogFilename
	}
	panic(fmt.Sprintf("base: unknown file type %d", int(fileType)))
}

func MakeFilepath(dirname string, fileType FileType, fileNum uint64) string {
	return filepath.Join(dirname, MakeFilename(fileType, fileNum))
}

// ParseFilename rejects names the engine did not create; such files are never touched.
func ParseFilename(name string) (fileType FileType, fileNum uint64, ok bool) {
	switch name {
	case CurrentFilename:
		return FileTypeCurrent, 0, true
	case CurrentTmpFilename:
		return FileTypeTemp, 0, true
	case LockFilename:
		return FileTypeLock, 0, true
	case InfoLogFilename:
		return FileTypeInfoLog, 0, true
	}
	if rest, found := strings.CutPrefix(name, "MANIFEST-"); found {
		num, ok := parseFileNum(rest)
		return FileTypeManifest, num, ok
	}
	if rest, found := strings.CutSuffix(name, ".log"); found {
		num, ok := parseFileNum(rest)
		return FileTypeLog, num, ok
	}
	if rest, found := strings.CutSuffix(name, ".sst"); found {
		num, ok := parseFileNum(rest)
		return FileTypeTable, num, ok
	}
	return 0, 0, false
}

func parseFileNum(digits string) (uint64, bool) {
	if digits == "" {
		return 0, false
	}
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return 0, false
		}
	}
	fileNum, err := strconv.ParseUint(digits, 10, 64)
	return fileNum, err == nil
}
