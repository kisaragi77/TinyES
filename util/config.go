package util

import (
	"path"
	"runtime"
)

var (
	RootPath string //Project root path
)

func init() {
	RootPath = path.Dir(GetCurrentPath()+"..") + "/" // get the root path of the project
}

// Get Current Working Directory
func GetCurrentPath() string {
	_, filename, _, _ := runtime.Caller(1) //1 For the function that calls GetCurrentPath,2 for the function that calls the function that calls GetCurrentPath, and so on
	return path.Dir(filename)
}
