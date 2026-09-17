package redisurl

import (
	"reflect"
	"strings"
	"testing"
)

func TestParseSupportsPasswordAndDatabaseList(t *testing.T) {
	target, err := Parse("redis://default:p%40ssword@redis.example.com:6380/0,2,5")
	if err != nil {
		t.Fatal(err)
	}

	options := target.Options(2)
	if options.Addr != "redis.example.com:6380" {
		t.Fatalf("unexpected Redis address: %q", options.Addr)
	}
	if options.Username != "default" || options.Password != "p@ssword" {
		t.Fatalf("unexpected credentials: username=%q password=%q", options.Username, options.Password)
	}
	if options.DB != 2 {
		t.Fatalf("unexpected database: %d", options.DB)
	}
	if !reflect.DeepEqual(target.Databases(), []int{0, 2, 5}) {
		t.Fatalf("unexpected databases: %v", target.Databases())
	}
	if !target.HasExplicitDatabases() {
		t.Fatal("databases should be marked as explicit")
	}
	if strings.Contains(target.RedactedURL(), "p%40ssword") || strings.Contains(target.RedactedURL(), "p@ssword") {
		t.Fatalf("redacted URL exposes the password: %q", target.RedactedURL())
	}
}

func TestParseWithoutDatabaseUsesDefaultButIsNotExplicit(t *testing.T) {
	target, err := Parse("redis://:secret@localhost:6379")
	if err != nil {
		t.Fatal(err)
	}
	if target.HasExplicitDatabases() {
		t.Fatal("database should not be marked as explicit")
	}
	if !reflect.DeepEqual(target.Databases(), []int{0}) {
		t.Fatalf("unexpected databases: %v", target.Databases())
	}
	if target.Options(7).DB != 7 {
		t.Fatal("Options did not apply the requested database")
	}
}

func TestParseSupportsQueryDatabase(t *testing.T) {
	target, err := Parse("redis://localhost:6379?db=4")
	if err != nil {
		t.Fatal(err)
	}
	if !target.HasExplicitDatabases() || !reflect.DeepEqual(target.Databases(), []int{4}) {
		t.Fatalf("unexpected database selection: %v", target.Databases())
	}
}

func TestParseRejectsInvalidAndLegacyURLs(t *testing.T) {
	tests := []string{
		"localhost:6379@0,2,5",
		"redis://:secret@localhost:6379@0,2,5",
		"redis:localhost:6379/0,2,5",
		"redis://localhost:6379/0,nope,2",
		"redis://localhost:6379/0,1?db=2",
		"http://localhost:6379/0",
	}
	for _, value := range tests {
		t.Run(value, func(t *testing.T) {
			if _, err := Parse(value); err == nil {
				t.Fatalf("expected %q to be rejected", value)
			}
		})
	}
}
