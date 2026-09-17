package redisstore

import (
	"context"
	"time"

	"github.com/gjongenelen/redis-mover/internal/archive"
	"github.com/gjongenelen/redis-mover/internal/redisurl"
	"github.com/gjongenelen/redis-mover/internal/transfer"
	redis "github.com/go-redis/redis/v8"
)

type Factory struct {
	target redisurl.Target
}

func NewFactory(target redisurl.Target) Factory {
	return Factory{target: target}
}

func (factory Factory) Open(database int) (transfer.Store, error) {
	return &Client{commands: redis.NewClient(factory.target.Options(database))}, nil
}

type Client struct {
	commands commands
}

func (client *Client) Keys(ctx context.Context, pattern string) ([]string, error) {
	return client.commands.Keys(ctx, pattern).Result()
}

func (client *Client) Read(ctx context.Context, key string) (archive.Record, bool, error) {
	dump, err := client.commands.Dump(ctx, key).Result()
	if err == redis.Nil {
		return archive.Record{}, false, nil
	}
	if err != nil {
		return archive.Record{}, false, err
	}

	ttl, err := client.commands.PTTL(ctx, key).Result()
	if err != nil {
		return archive.Record{}, false, err
	}
	if ttl == -2 {
		return archive.Record{}, false, nil
	}

	ttlMillis := int64(0)
	if ttl != -1 {
		ttlMillis = ttl.Milliseconds()
	}
	return archive.Record{Dump: []byte(dump), TTLMillis: ttlMillis}, true, nil
}

func (client *Client) Exists(ctx context.Context, key string) (bool, error) {
	count, err := client.commands.Exists(ctx, key).Result()
	return count > 0, err
}

func (client *Client) Restore(ctx context.Context, key string, record archive.Record) error {
	ttl := time.Duration(record.TTLMillis) * time.Millisecond
	return client.commands.Restore(ctx, key, ttl, string(record.Dump)).Err()
}

func (client *Client) Close() error {
	return client.commands.Close()
}

type commands interface {
	Keys(ctx context.Context, pattern string) *redis.StringSliceCmd
	Dump(ctx context.Context, key string) *redis.StringCmd
	PTTL(ctx context.Context, key string) *redis.DurationCmd
	Exists(ctx context.Context, keys ...string) *redis.IntCmd
	Restore(ctx context.Context, key string, ttl time.Duration, value string) *redis.StatusCmd
	Close() error
}
