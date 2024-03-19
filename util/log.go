package util

import (
	"log"
	"os"
)

// Log is a global logger single instance
var Log = log.New(os.Stdout, "[radic]", log.Lshortfile|log.Ldate|log.Ltime)
