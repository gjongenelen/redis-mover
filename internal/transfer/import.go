package transfer

import (
	"context"
	"fmt"
	"io"
	"sort"

	"github.com/gjongenelen/redis-mover/internal/archive"
)

type Importer struct {
	Stores   StoreFactory
	Progress io.Writer
}

type ImportResult struct {
	Databases int
	Keys      int
}

func (importer Importer) Import(ctx context.Context, file archive.File, databaseOverride *int) (ImportResult, error) {
	if importer.Stores == nil {
		return ImportResult{}, fmt.Errorf("Redis store factory is required")
	}

	ids := make([]int, 0, len(file.Databases))
	for database := range file.Databases {
		ids = append(ids, database)
	}
	sort.Ints(ids)

	result := ImportResult{Databases: len(ids)}
	for _, archiveDatabase := range ids {
		destinationDatabase := archiveDatabase
		if !file.Multiple && databaseOverride != nil {
			destinationDatabase = *databaseOverride
		}
		data := file.Databases[archiveDatabase]
		if err := importer.importDatabase(ctx, destinationDatabase, data); err != nil {
			return ImportResult{}, err
		}
		result.Keys += len(data.Records)
	}
	return result, nil
}

func (importer Importer) importDatabase(ctx context.Context, database int, data archive.Database) error {
	store, err := importer.Stores.Open(database)
	if err != nil {
		return fmt.Errorf("open Redis database %d: %w", database, err)
	}

	importErr := importer.writeDatabase(ctx, store, database, data)
	closeErr := store.Close()
	if importErr != nil {
		return importErr
	}
	if closeErr != nil {
		return fmt.Errorf("close Redis database %d: %w", database, closeErr)
	}
	return nil
}

func (importer Importer) writeDatabase(ctx context.Context, store Store, database int, data archive.Database) error {
	keys := make([]string, 0, len(data.Records))
	for key := range data.Records {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		exists, err := store.Exists(ctx, key)
		if err != nil {
			return fmt.Errorf("check key %s in Redis database %d: %w", key, database, err)
		}
		if exists {
			return fmt.Errorf("key %s already exists in Redis database %d", key, database)
		}

		record := data.Records[key]
		if importer.Progress != nil {
			fmt.Fprintf(importer.Progress, "Importing key: %s to db %d (dump len: %d, ttl: %dms)\n", key, database, len(record.Dump), record.TTLMillis)
		}
		if err := store.Restore(ctx, key, record); err != nil {
			return fmt.Errorf("restore key %s in Redis database %d: %w", key, database, err)
		}
	}
	return nil
}
