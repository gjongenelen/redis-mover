package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestDataJSONRoundTripPreservesBinaryDump(t *testing.T) {
	wantDump := []byte{0x00, 0xff, 0x0a, 0x80, 'j', 's', 'o', 'n'}
	want := Data{
		Version:   exportFormatVersion,
		DumpStart: time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC),
		DumpEnd:   time.Date(2026, time.August, 19, 12, 0, 1, 0, time.UTC),
		Db:        2,
		Data: map[string]DumpRecord{
			"json:key": {
				Dump:      wantDump,
				TTLMillis: 12345,
			},
		},
	}

	encoded, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}

	var got Data
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatal(err)
	}

	gotRecord := got.Data["json:key"]
	if !bytes.Equal(gotRecord.Dump, wantDump) {
		t.Fatalf("dump changed during JSON round trip: got %v, want %v", gotRecord.Dump, wantDump)
	}
	if gotRecord.TTLMillis != 12345 {
		t.Fatalf("ttl changed during JSON round trip: got %d, want 12345", gotRecord.TTLMillis)
	}
}

func TestValidateDataRejectsOldExportFormat(t *testing.T) {
	err := validateData(Data{})
	if err == nil || !strings.Contains(err.Error(), "unsupported export format version") {
		t.Fatalf("expected unsupported version error, got %v", err)
	}
}

func TestValidateDataRejectsInvalidRecord(t *testing.T) {
	tests := []struct {
		name   string
		record DumpRecord
	}{
		{name: "empty dump", record: DumpRecord{}},
		{name: "negative ttl", record: DumpRecord{Dump: []byte("dump"), TTLMillis: -1}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := Data{
				Version: exportFormatVersion,
				Data:    map[string]DumpRecord{"key": test.record},
			}
			if err := validateData(data); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestParseRedisTargetSupportsPasswordInURL(t *testing.T) {
	target, err := parseRedisTarget("redis://default:p%40ssword@redis.example.com:6380/4")
	if err != nil {
		t.Fatal(err)
	}

	if target.options.Addr != "redis.example.com:6380" {
		t.Fatalf("unexpected Redis address: %q", target.options.Addr)
	}
	if target.options.Username != "default" {
		t.Fatalf("unexpected Redis username: %q", target.options.Username)
	}
	if target.options.Password != "p@ssword" {
		t.Fatalf("unexpected Redis password: %q", target.options.Password)
	}
	if !target.databaseSpecified || len(target.databases) != 1 || target.databases[0] != 4 {
		t.Fatalf("unexpected databases: specified=%v, databases=%v", target.databaseSpecified, target.databases)
	}
	if strings.Contains(target.displayURL, "p%40ssword") || strings.Contains(target.displayURL, "p@ssword") {
		t.Fatalf("display URL exposes the password: %q", target.displayURL)
	}
}

func TestParseRedisTargetSupportsDatabaseListInURL(t *testing.T) {
	target, err := parseRedisTarget("redis://:secret@localhost:6379/0,2,5")
	if err != nil {
		t.Fatal(err)
	}

	if target.options.Addr != "localhost:6379" {
		t.Fatalf("unexpected Redis address: %q", target.options.Addr)
	}
	wantDatabases := []int{0, 2, 5}
	if len(target.databases) != len(wantDatabases) {
		t.Fatalf("unexpected databases: %v", target.databases)
	}
	for index, want := range wantDatabases {
		if target.databases[index] != want {
			t.Fatalf("unexpected databases: %v", target.databases)
		}
	}
	if target.options.Password != "secret" {
		t.Fatalf("unexpected password: %q", target.options.Password)
	}
}

func TestParseRedisTargetRejectsLegacyAddress(t *testing.T) {
	legacyAddresses := []string{
		"localhost:6379@0,2,5",
		"redis://:secret@localhost:6379@0,2,5",
		"redis:localhost:6379/0,2,5",
	}
	for _, address := range legacyAddresses {
		if _, err := parseRedisTarget(address); err == nil {
			t.Fatalf("expected legacy address %q to be rejected", address)
		}
	}
}

func TestRedisURLWithoutDatabaseUsesDatabaseFromImport(t *testing.T) {
	target, err := parseRedisTarget("redis://:secret@localhost:6379")
	if err != nil {
		t.Fatal(err)
	}
	if target.databaseSpecified {
		t.Fatal("database should not be marked as specified")
	}

	options := target.optionsForDatabase(7)
	if options.DB != 7 {
		t.Fatalf("unexpected database: got %d, want 7", options.DB)
	}
	if options.Password != "secret" {
		t.Fatalf("unexpected password: %q", options.Password)
	}
}
