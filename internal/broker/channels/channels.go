package channels

import (
	"context"
	"fmt"

	"github.com/go-po/po/internal/broker"
)

func New() *Channels {
	return &Channels{}
}

type Channels struct {
}

func (ch *Channels) Register(ctx context.Context, group string, input broker.RecordHandler) (broker.RecordHandler, error) {
	return nil, fmt.Errorf("not implemented")
}
