package broker

import (
	"context"
	"github.com/go-po/po/internal/record"
)

type Distributor interface {
	Distribute(ctx context.Context, record record.Record) error
}
