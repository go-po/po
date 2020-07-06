package streams

import (
	"context"
)

type Handler interface {
	Handle(ctx context.Context, msg Message) error
}

type HandlerFunc func(ctx context.Context, msg Message) error

func (fn HandlerFunc) Handle(ctx context.Context, msg Message) error {
	return fn(ctx, msg)
}

// Marker interface used to indicate that a view or projection
// supports snapshotting.
// The provided name is used to store the instance as a json blob.
type NamedSnapshot interface {
	SnapshotName() string
}
