package redisstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gjongenelen/redis-mover/internal/archive"
	redis "github.com/go-redis/redis/v8"
)

func TestReadReturnsDumpAndTTL(t *testing.T) {
	commands := &fakeCommands{
		dump: redis.NewStringResult("binary-dump", nil),
		ttl:  redis.NewDurationResult(2500*time.Millisecond, nil),
	}
	client := Client{commands: commands}

	record, found, err := client.Read(context.Background(), "key")
	if err != nil {
		t.Fatal(err)
	}
	if !found || string(record.Dump) != "binary-dump" || record.TTLMillis != 2500 {
		t.Fatalf("unexpected record: found=%v record=%#v", found, record)
	}
}

func TestReadHandlesPersistentAndDisappearedKeys(t *testing.T) {
	tests := []struct {
		name  string
		dump  *redis.StringCmd
		ttl   *redis.DurationCmd
		found bool
	}{
		{name: "missing before dump", dump: redis.NewStringResult("", redis.Nil), found: false},
		{name: "missing before ttl", dump: redis.NewStringResult("dump", nil), ttl: redis.NewDurationResult(-2, nil), found: false},
		{name: "persistent", dump: redis.NewStringResult("dump", nil), ttl: redis.NewDurationResult(-1, nil), found: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := Client{commands: &fakeCommands{dump: test.dump, ttl: test.ttl}}
			record, found, err := client.Read(context.Background(), "key")
			if err != nil {
				t.Fatal(err)
			}
			if found != test.found {
				t.Fatalf("unexpected found value: %v", found)
			}
			if found && record.TTLMillis != 0 {
				t.Fatalf("unexpected persistent TTL: %d", record.TTLMillis)
			}
		})
	}
}

func TestRestoreConvertsMilliseconds(t *testing.T) {
	commands := &fakeCommands{restore: redis.NewStatusResult("OK", nil)}
	client := Client{commands: commands}
	record := archive.Record{Dump: []byte("dump"), TTLMillis: 1234}

	if err := client.Restore(context.Background(), "key", record); err != nil {
		t.Fatal(err)
	}
	if commands.restoredKey != "key" || commands.restoredTTL != 1234*time.Millisecond || commands.restoredValue != "dump" {
		t.Fatalf("unexpected restore call: key=%q ttl=%v value=%q", commands.restoredKey, commands.restoredTTL, commands.restoredValue)
	}
}

type fakeCommands struct {
	keys          *redis.StringSliceCmd
	dump          *redis.StringCmd
	ttl           *redis.DurationCmd
	exists        *redis.IntCmd
	restore       *redis.StatusCmd
	restoredKey   string
	restoredTTL   time.Duration
	restoredValue string
	closeErr      error
}

func (commands *fakeCommands) Keys(context.Context, string) *redis.StringSliceCmd {
	if commands.keys == nil {
		return redis.NewStringSliceResult(nil, nil)
	}
	return commands.keys
}

func (commands *fakeCommands) Dump(context.Context, string) *redis.StringCmd {
	if commands.dump == nil {
		return redis.NewStringResult("", errors.New("unexpected dump"))
	}
	return commands.dump
}

func (commands *fakeCommands) PTTL(context.Context, string) *redis.DurationCmd {
	if commands.ttl == nil {
		return redis.NewDurationResult(0, errors.New("unexpected pttl"))
	}
	return commands.ttl
}

func (commands *fakeCommands) Exists(context.Context, ...string) *redis.IntCmd {
	if commands.exists == nil {
		return redis.NewIntResult(0, nil)
	}
	return commands.exists
}

func (commands *fakeCommands) Restore(_ context.Context, key string, ttl time.Duration, value string) *redis.StatusCmd {
	commands.restoredKey = key
	commands.restoredTTL = ttl
	commands.restoredValue = value
	if commands.restore == nil {
		return redis.NewStatusResult("OK", nil)
	}
	return commands.restore
}

func (commands *fakeCommands) Close() error {
	return commands.closeErr
}
