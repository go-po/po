package stream

import (
	"context"
)

type Handler interface {
	Handle(ctx context.Context, msg Message) error
}
