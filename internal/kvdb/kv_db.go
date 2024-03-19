package kvdb

import (
	"os"
	"strings"

	"github.com/kisaragi77/TinyES/util"
)

// Some common KV Storage based on LSM-tree algorithm
const (
	BOLT = iota
	BADGER
)

type IKeyValueDB interface {
	Open() error                              // Initialize Database
	GetDbPath() string                        /// Get path of Storage
	Set(k, v []byte) error                    // Write <key, value>
	BatchSet(keys, values [][]byte) error     // Write multiple <key, value> pairs
	Get(k []byte) ([]byte, error)             // Read Value by Key
	BatchGet(keys [][]byte) ([][]byte, error) // Read multiple values by keys (No order guarantee)
	Delete(k []byte) error                    // Delete by Key
	BatchDelete(keys [][]byte) error          // Delete multiple keys
	Has(k []byte) bool                        // Check if the DB contains the given key
	IterDB(fn func(k, v []byte) error) int64  // Iterate the whole DB with callback function
	IterKey(fn func(k []byte) error) int64    // Iterate all keys with callback function
	Close() error                             // Flush data in memory to disk and release file lock
}

// Factory Of KeyValueDB
func GetKvDb(dbtype int, path string) (IKeyValueDB, error) {
	paths := strings.Split(path, "/")
	parentPath := strings.Join(paths[0:len(paths)-1], "/")

	info, err := os.Stat(parentPath)
	if os.IsNotExist(err) {
		util.Log.Printf("create dir %s", parentPath)
		os.MkdirAll(parentPath, os.ModePerm)
	} else {
		if info.Mode().IsRegular() {
			util.Log.Printf("%s is a regular file, will delete it", parentPath)
			os.Remove(parentPath)
		}
	}

	var db IKeyValueDB
	switch dbtype {
	case BADGER:
		db = new(Badger).WithDataPath(path)
	default: //Default use Bolt
		db = new(Bolt).WithDataPath(path).WithBucket("radic")
	}
	err = db.Open()
	return db, err
}
