package transfer

import (
	"context"

	"github.com/gjongenelen/redis-mover/internal/archive"
)

type Store interface {
	Keys(ctx context.Context, pattern string) ([]string, error)
	Read(ctx context.Context, key string) (archive.Record, bool, error)
	Exists(ctx context.Context, key string) (bool, error)
	Restore(ctx context.Context, key string, record archive.Record) error
	Close() error
}

type StoreFactory interface {
	Open(database int) (Store, error)
}
