package archive

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
)

func Decode(reader io.Reader) (File, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return File{}, err
	}

	multiple := map[int]Database{}
	if err := json.Unmarshal(data, &multiple); err == nil && len(multiple) > 0 {
		if err := validateDatabases(multiple); err != nil {
			return File{}, err
		}
		return File{Databases: multiple, Multiple: true}, nil
	}

	var single Database
	if err := json.Unmarshal(data, &single); err != nil {
		return File{}, err
	}
	if err := Validate(single); err != nil {
		return File{}, err
	}
	return File{Databases: map[int]Database{single.DB: single}}, nil
}

func Encode(writer io.Writer, file File) error {
	if len(file.Databases) == 0 {
		return fmt.Errorf("cannot encode an empty archive")
	}
	if err := validateDatabases(file.Databases); err != nil {
		return err
	}

	var value interface{} = file.Databases
	if !file.Multiple {
		if len(file.Databases) != 1 {
			return fmt.Errorf("single-database archive contains %d databases", len(file.Databases))
		}
		for _, database := range file.Databases {
			value = database
		}
	}

	encoded, err := json.MarshalIndent(value, "", " ")
	if err != nil {
		return err
	}
	_, err = writer.Write(encoded)
	return err
}

func validateDatabases(databases map[int]Database) error {
	ids := make([]int, 0, len(databases))
	for database := range databases {
		ids = append(ids, database)
	}
	sort.Ints(ids)
	for _, database := range ids {
		if err := Validate(databases[database]); err != nil {
			return fmt.Errorf("database %d: %w", database, err)
		}
	}
	return nil
}
