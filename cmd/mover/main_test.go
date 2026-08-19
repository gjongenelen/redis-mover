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
