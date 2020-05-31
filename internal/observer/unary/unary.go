package unary

import (
	"context"
)

type Logger interface {
	Debugf(template string, args ...interface{})
	Errorf(template string, args ...interface{})
	Infof(template string, args ...interface{})
	Errf(err error, template string, args ...interface{})
}

type CT interface {
	Observe(ctx context.Context, a string) func()
}

type CTFunc func(ctx context.Context, a string) func()

func (fn CTFunc) Observe(ctx context.Context, a string) func() {
	return fn(ctx, a)
}
func Combine(traces ...CT) CT {
	return CTFunc(func(ctx context.Context, a string) func() {
		return func() {

		}
	})
}

func LogDebugf(logger Logger, format string, args ...interface{}) CT {
	return CTFunc(func(ctx context.Context, a string) func() {
		logger.Debugf(format, append(args, a))
		return func() {

		}
	})
}

func LogInfof(logger Logger, format string, args ...interface{}) CT {
	return CTFunc(func(ctx context.Context, a string) func() {
		logger.Infof(format, append(args, a))
		return func() {

		}
	})
}

func NewBuilder(logger Logger) *Builder {
	return &Builder{
		logger: logger,
	}
}

type Builder struct {
	logger Logger
	traces []CT
}

func (builder *Builder) Build() CT {
	return Combine(builder.traces...)
}

func (builder *Builder) LogDebugf(format string, args ...interface{}) *Builder {
	builder.traces = append(builder.traces, LogDebugf(builder.logger, format, args...))
	return builder
}

func (builder *Builder) LogInfof(format string, args ...interface{}) *Builder {
	builder.traces = append(builder.traces, LogInfof(builder.logger, format, args...))
	return builder
}

func (builder *Builder) Metrics() *Builder {
	return builder
}
