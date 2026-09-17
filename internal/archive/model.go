package archive

import (
	"fmt"
	"time"
)

const FormatVersion = 1

type Database struct {
	Version   int               `json:"version"`
	DumpStart time.Time         `json:"dump_start"`
	DumpEnd   time.Time         `json:"dump_end"`
	DB        int               `json:"db"`
	Records   map[string]Record `json:"data"`
}

type Record struct {
	Dump      []byte `json:"dump"`
	TTLMillis int64  `json:"ttl_ms"`
}

// File represents either the historic single-database JSON object or the
// multi-database JSON map. Multiple preserves which representation was read.
type File struct {
	Databases map[int]Database
	Multiple  bool
}

func New(databases map[int]Database) File {
	return File{
		Databases: databases,
		Multiple:  len(databases) > 1,
	}
}

func Validate(database Database) error {
	if database.Version != FormatVersion {
		return fmt.Errorf("unsupported export format version %d (expected %d)", database.Version, FormatVersion)
	}
	for key, record := range database.Records {
		if len(record.Dump) == 0 {
			return fmt.Errorf("key %s has an empty dump", key)
		}
		if record.TTLMillis < 0 {
			return fmt.Errorf("key %s has an invalid ttl: %dms", key, record.TTLMillis)
		}
	}
	return nil
}
