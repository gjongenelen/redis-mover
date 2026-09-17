package archive

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestSingleDatabaseRoundTripPreservesBinaryDump(t *testing.T) {
	wantDump := []byte{0x00, 0xff, 0x0a, 0x80, 'j', 's', 'o', 'n'}
	want := Database{
		Version:   FormatVersion,
		DumpStart: time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC),
		DumpEnd:   time.Date(2026, time.August, 19, 12, 0, 1, 0, time.UTC),
		DB:        2,
		Records: map[string]Record{
			"json:key": {Dump: wantDump, TTLMillis: 12345},
		},
	}

	var encoded bytes.Buffer
	if err := Encode(&encoded, File{Databases: map[int]Database{2: want}}); err != nil {
		t.Fatal(err)
	}
	got, err := Decode(&encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got.Multiple {
		t.Fatal("single database was decoded as a multi-database archive")
	}
	gotRecord := got.Databases[2].Records["json:key"]
	if !bytes.Equal(gotRecord.Dump, wantDump) || gotRecord.TTLMillis != 12345 {
		t.Fatalf("record changed during round trip: %#v", gotRecord)
	}
}

func TestMultiDatabaseRoundTripPreservesShape(t *testing.T) {
	file := File{
		Multiple: true,
		Databases: map[int]Database{
			1: validDatabase(1),
			3: validDatabase(3),
		},
	}
	var encoded bytes.Buffer
	if err := Encode(&encoded, file); err != nil {
		t.Fatal(err)
	}
	got, err := Decode(&encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Multiple || len(got.Databases) != 2 {
		t.Fatalf("unexpected decoded archive: %#v", got)
	}
}

func TestValidateRejectsInvalidData(t *testing.T) {
	tests := []struct {
		name     string
		database Database
		message  string
	}{
		{name: "old version", database: Database{}, message: "unsupported export format version"},
		{name: "empty dump", database: Database{Version: FormatVersion, Records: map[string]Record{"key": {}}}, message: "empty dump"},
		{name: "negative ttl", database: Database{Version: FormatVersion, Records: map[string]Record{"key": {Dump: []byte("dump"), TTLMillis: -1}}}, message: "invalid ttl"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := Validate(test.database)
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("expected error containing %q, got %v", test.message, err)
			}
		})
	}
}

func TestDecodeRejectsInvalidJSON(t *testing.T) {
	if _, err := Decode(strings.NewReader("not json")); err == nil {
		t.Fatal("expected invalid JSON to be rejected")
	}
}

func validDatabase(database int) Database {
	return Database{
		Version: FormatVersion,
		DB:      database,
		Records: map[string]Record{"key": {Dump: []byte("dump")}},
	}
}
