package observer

import (
	"github.com/go-po/po/internal/logger"
	"github.com/go-po/po/internal/observer/binary"
	"github.com/go-po/po/internal/observer/nullary"
	"github.com/go-po/po/internal/observer/unary"
	"github.com/prometheus/client_golang/prometheus"
)

type Logger interface {
	Debugf(template string, args ...interface{})
	Errorf(template string, args ...interface{})
	Infof(template string, args ...interface{})
	Errf(err error, template string, args ...interface{})
}

func New(logger Logger, registrer prometheus.Registerer) *Builder {
	return &Builder{
		logger:  logger,
		metrics: registrer,
	}
}

func NewStub() *Builder {
	return New(&logger.NoopLogger{}, NewPromStub())
}

type Builder struct {
	logger  Logger
	metrics prometheus.Registerer
}

func (builder *Builder) Debugf(template string, args ...interface{}) {
	builder.logger.Debugf(template, args...)
}

func (builder *Builder) Errorf(template string, args ...interface{}) {
	builder.logger.Errorf(template, args...)
}

func (builder *Builder) Infof(template string, args ...interface{}) {
	builder.logger.Infof(template, args...)
}

func (builder *Builder) Errf(err error, template string, args ...interface{}) {
	builder.logger.Errf(err, template, args...)
}

func (builder *Builder) Unary() *unary.Builder {
	return unary.NewBuilder(builder.logger, builder.metrics)
}

func (builder *Builder) Nullary() *nullary.Builder {
	return nullary.NewBuilder(builder.logger, builder.metrics)
}

func (builder *Builder) Binary() *binary.Builder {
	return binary.NewBuilder(builder.logger, builder.metrics)
}
