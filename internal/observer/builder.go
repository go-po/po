package observer

import (
	"github.com/go-po/po/internal/logger"
	"github.com/go-po/po/internal/observer/nullary"
	"github.com/go-po/po/internal/observer/unary"
)

type Logger interface {
	Debugf(template string, args ...interface{})
	Errorf(template string, args ...interface{})
	Infof(template string, args ...interface{})
	Errf(err error, template string, args ...interface{})
}

func New(logger Logger) *Builder {
	return &Builder{
		logger: logger,
	}
}

func NewStub() *Builder {
	return New(&logger.NoopLogger{})
}

type Builder struct {
	logger Logger
}

func (builder *Builder) Unary() *unary.Builder {
	return unary.NewBuilder(builder.logger)
}

func (builder *Builder) Nullary() *nullary.Builder {
	return nullary.NewBuilder(builder.logger)
}
