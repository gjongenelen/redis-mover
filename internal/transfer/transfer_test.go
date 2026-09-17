package transfer

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gjongenelen/redis-mover/internal/archive"
)

func TestExporterExportsMultipleDatabasesAndSkipsMissingKeys(t *testing.T) {
	factory := &fakeFactory{stores: map[int]*fakeStore{
		1: {
			keys: []string{"one", "expired"},
			records: map[string]archive.Record{
				"one": {Dump: []byte("dump-one"), TTLMillis: 2500},
			},
		},
		2: {
			keys: []string{"two"},
			records: map[string]archive.Record{
				"two": {Dump: []byte("dump-two")},
			},
		},
	}}
	times := []time.Time{
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC),
		time.Date(2026, 1, 1, 0, 0, 2, 0, time.UTC),
		time.Date(2026, 1, 1, 0, 0, 3, 0, time.UTC),
	}
	var timeIndex int
	var progress bytes.Buffer
	exporter := Exporter{
		Stores:   factory,
		Progress: &progress,
		Now: func() time.Time {
			value := times[timeIndex]
			timeIndex++
			return value
		},
	}

	file, err := exporter.Export(context.Background(), []int{1, 2}, "prefix:")
	if err != nil {
		t.Fatal(err)
	}
	if !file.Multiple || len(file.Databases) != 2 {
		t.Fatalf("unexpected archive: %#v", file)
	}
	if _, exists := file.Databases[1].Records["expired"]; exists {
		t.Fatal("missing key was exported")
	}
	if factory.stores[1].pattern != "prefix:*" || factory.stores[2].pattern != "prefix:*" {
		t.Fatalf("unexpected key patterns: %q, %q", factory.stores[1].pattern, factory.stores[2].pattern)
	}
	if !factory.stores[1].closed || !factory.stores[2].closed {
		t.Fatal("stores were not closed")
	}
	if !strings.Contains(progress.String(), "Exporting key: one") {
		t.Fatalf("missing progress output: %q", progress.String())
	}
}

func TestExporterClosesStoreOnReadError(t *testing.T) {
	store := &fakeStore{keysErr: errors.New("boom")}
	factory := &fakeFactory{stores: map[int]*fakeStore{0: store}}
	_, err := (Exporter{Stores: factory}).Export(context.Background(), []int{0}, "")
	if err == nil || !store.closed {
		t.Fatalf("expected error and closed store, got err=%v closed=%v", err, store.closed)
	}
}

func TestImporterUsesOverrideForSingleDatabase(t *testing.T) {
	store := &fakeStore{}
	factory := &fakeFactory{stores: map[int]*fakeStore{7: store}}
	file := archive.File{Databases: map[int]archive.Database{
		2: {
			Version: archive.FormatVersion,
			DB:      2,
			Records: map[string]archive.Record{"key": {Dump: []byte("dump"), TTLMillis: 500}},
		},
	}}
	override := 7

	result, err := (Importer{Stores: factory}).Import(context.Background(), file, &override)
	if err != nil {
		t.Fatal(err)
	}
	if result.Databases != 1 || result.Keys != 1 {
		t.Fatalf("unexpected result: %#v", result)
	}
	if !reflect.DeepEqual(store.restored["key"], archive.Record{Dump: []byte("dump"), TTLMillis: 500}) {
		t.Fatalf("unexpected restored record: %#v", store.restored["key"])
	}
	if !store.closed {
		t.Fatal("store was not closed")
	}
}

func TestImporterRejectsExistingKeyAndClosesStore(t *testing.T) {
	store := &fakeStore{existing: map[string]bool{"key": true}}
	factory := &fakeFactory{stores: map[int]*fakeStore{2: store}}
	file := archive.File{Databases: map[int]archive.Database{
		2: {Version: archive.FormatVersion, DB: 2, Records: map[string]archive.Record{"key": {Dump: []byte("dump")}}},
	}}

	_, err := (Importer{Stores: factory}).Import(context.Background(), file, nil)
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("expected existing-key error, got %v", err)
	}
	if !store.closed {
		t.Fatal("store was not closed")
	}
}

type fakeFactory struct {
	stores map[int]*fakeStore
}

func (factory *fakeFactory) Open(database int) (Store, error) {
	store, ok := factory.stores[database]
	if !ok {
		return nil, errors.New("unexpected database")
	}
	if store.records == nil {
		store.records = map[string]archive.Record{}
	}
	if store.existing == nil {
		store.existing = map[string]bool{}
	}
	if store.restored == nil {
		store.restored = map[string]archive.Record{}
	}
	return store, nil
}

type fakeStore struct {
	keys     []string
	keysErr  error
	pattern  string
	records  map[string]archive.Record
	existing map[string]bool
	restored map[string]archive.Record
	closed   bool
}

func (store *fakeStore) Keys(_ context.Context, pattern string) ([]string, error) {
	store.pattern = pattern
	return store.keys, store.keysErr
}

func (store *fakeStore) Read(_ context.Context, key string) (archive.Record, bool, error) {
	record, found := store.records[key]
	return record, found, nil
}

func (store *fakeStore) Exists(_ context.Context, key string) (bool, error) {
	return store.existing[key], nil
}

func (store *fakeStore) Restore(_ context.Context, key string, record archive.Record) error {
	store.restored[key] = record
	return nil
}

func (store *fakeStore) Close() error {
	store.closed = true
	return nil
}
