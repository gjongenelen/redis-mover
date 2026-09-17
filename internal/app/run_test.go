package app

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gjongenelen/redis-mover/internal/archive"
	"github.com/gjongenelen/redis-mover/internal/redisurl"
	"github.com/gjongenelen/redis-mover/internal/transfer"
)

func TestRunRequiresAnOperation(t *testing.T) {
	err := run(context.Background(), nil, strings.NewReader(""), &bytes.Buffer{}, testDependencies(nil))
	if err == nil || !strings.Contains(err.Error(), "export or import") {
		t.Fatalf("expected operation error, got %v", err)
	}
}

func TestRunHelpIsSuccessful(t *testing.T) {
	var output bytes.Buffer
	if err := run(context.Background(), []string{"-h"}, strings.NewReader(""), &output, testDependencies(nil)); err != nil {
		t.Fatalf("help returned an error: %v", err)
	}
	if !strings.Contains(output.String(), "Redis URL") {
		t.Fatalf("unexpected help output: %q", output.String())
	}
}

func TestRunAbortRedactsPasswordAndDoesNotOpenRedis(t *testing.T) {
	opened := false
	deps := dependencies{
		newStoreFactory: func(redisurl.Target) transfer.StoreFactory {
			opened = true
			return &appFactory{}
		},
		now: time.Now,
	}
	var output bytes.Buffer
	err := run(
		context.Background(),
		[]string{"-export", "-file", "dump.json", "-redis", "redis://:very-secret@localhost:6379/0"},
		strings.NewReader("n\n"),
		&output,
		deps,
	)
	if err != nil {
		t.Fatal(err)
	}
	if opened {
		t.Fatal("Redis was opened after the operation was aborted")
	}
	if strings.Contains(output.String(), "very-secret") {
		t.Fatalf("output exposed the password: %q", output.String())
	}
}

func TestRunExportWritesArchive(t *testing.T) {
	store := &appStore{records: map[string]archive.Record{"key": {Dump: []byte("dump"), TTLMillis: 42}}}
	store.keys = []string{"key"}
	path := filepath.Join(t.TempDir(), "dump.json")
	var output bytes.Buffer

	err := run(
		context.Background(),
		[]string{"-export", "-file", path, "-redis", "redis://localhost:6379/2"},
		strings.NewReader("y\n"),
		&output,
		dependencies{
			newStoreFactory: func(redisurl.Target) transfer.StoreFactory {
				return &appFactory{stores: map[int]*appStore{2: store}}
			},
			now: func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) },
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := archive.Decode(file)
	file.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(decoded.Databases[2].Records["key"].Dump) != "dump" {
		t.Fatalf("unexpected archive: %#v", decoded)
	}
	if !strings.Contains(output.String(), "1 keys exported") {
		t.Fatalf("unexpected output: %q", output.String())
	}
}

func TestRunImportUsesExplicitURLDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.json")
	var encoded bytes.Buffer
	if err := archive.Encode(&encoded, archive.File{Databases: map[int]archive.Database{
		2: {
			Version: archive.FormatVersion,
			DB:      2,
			Records: map[string]archive.Record{"key": {Dump: []byte("dump"), TTLMillis: 10}},
		},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}

	store := &appStore{}
	var output bytes.Buffer
	err := run(
		context.Background(),
		[]string{"-import", "-file", path, "-redis", "redis://:secret@localhost:6379/7"},
		strings.NewReader("y\n"),
		&output,
		dependencies{
			newStoreFactory: func(redisurl.Target) transfer.StoreFactory {
				return &appFactory{stores: map[int]*appStore{7: store}}
			},
			now: time.Now,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if string(store.restored["key"].Dump) != "dump" {
		t.Fatalf("key was not restored to the explicit database: %#v", store.restored)
	}
	if strings.Contains(output.String(), "secret") {
		t.Fatalf("output exposed the password: %q", output.String())
	}
}

func testDependencies(factory transfer.StoreFactory) dependencies {
	return dependencies{
		newStoreFactory: func(redisurl.Target) transfer.StoreFactory { return factory },
		now:             time.Now,
	}
}

type appFactory struct {
	stores map[int]*appStore
}

func (factory *appFactory) Open(database int) (transfer.Store, error) {
	if factory.stores == nil {
		return &appStore{}, nil
	}
	return factory.stores[database], nil
}

type appStore struct {
	keys     []string
	records  map[string]archive.Record
	restored map[string]archive.Record
}

func (store *appStore) Keys(context.Context, string) ([]string, error) {
	return store.keys, nil
}

func (store *appStore) Read(_ context.Context, key string) (archive.Record, bool, error) {
	record, found := store.records[key]
	return record, found, nil
}

func (store *appStore) Exists(context.Context, string) (bool, error) {
	return false, nil
}

func (store *appStore) Restore(_ context.Context, key string, record archive.Record) error {
	if store.restored == nil {
		store.restored = make(map[string]archive.Record)
	}
	store.restored[key] = record
	return nil
}

func (store *appStore) Close() error {
	return nil
}
