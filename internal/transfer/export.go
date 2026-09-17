package transfer

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/gjongenelen/redis-mover/internal/archive"
)

type Exporter struct {
	Stores   StoreFactory
	Progress io.Writer
	Now      func() time.Time
}

func (exporter Exporter) Export(ctx context.Context, databases []int, pattern string) (archive.File, error) {
	if exporter.Stores == nil {
		return archive.File{}, fmt.Errorf("Redis store factory is required")
	}
	now := exporter.Now
	if now == nil {
		now = time.Now
	}

	exported := make(map[int]archive.Database, len(databases))
	for _, database := range databases {
		data, err := exporter.exportDatabase(ctx, database, pattern, now)
		if err != nil {
			return archive.File{}, err
		}
		exported[database] = data
	}
	return archive.New(exported), nil
}

func (exporter Exporter) exportDatabase(ctx context.Context, database int, pattern string, now func() time.Time) (archive.Database, error) {
	store, err := exporter.Stores.Open(database)
	if err != nil {
		return archive.Database{}, fmt.Errorf("open Redis database %d: %w", database, err)
	}

	data, exportErr := exporter.readDatabase(ctx, store, database, pattern, now)
	closeErr := store.Close()
	if exportErr != nil {
		return archive.Database{}, exportErr
	}
	if closeErr != nil {
		return archive.Database{}, fmt.Errorf("close Redis database %d: %w", database, closeErr)
	}
	return data, nil
}

func (exporter Exporter) readDatabase(ctx context.Context, store Store, database int, pattern string, now func() time.Time) (archive.Database, error) {
	keys, err := store.Keys(ctx, pattern+"*")
	if err != nil {
		return archive.Database{}, fmt.Errorf("list keys in Redis database %d: %w", database, err)
	}

	data := archive.Database{
		Version:   archive.FormatVersion,
		DumpStart: now(),
		DB:        database,
		Records:   make(map[string]archive.Record),
	}
	for _, key := range keys {
		record, found, err := store.Read(ctx, key)
		if err != nil {
			return archive.Database{}, fmt.Errorf("read key %s from Redis database %d: %w", key, database, err)
		}
		if !found {
			continue
		}
		if _, exists := data.Records[key]; exists {
			return archive.Database{}, fmt.Errorf("conflicting key %s in Redis database %d", key, database)
		}
		if exporter.Progress != nil {
			fmt.Fprintf(exporter.Progress, "Exporting key: %s (dump len: %d, ttl: %dms)\n", key, len(record.Dump), record.TTLMillis)
		}
		data.Records[key] = record
	}
	data.DumpEnd = now()
	return data, nil
}
