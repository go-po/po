package nullary

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
	Observe(ctx context.Context) func()
}

type CTFunc func(ctx context.Context) func()

func (fn CTFunc) Observe(ctx context.Context) func() {
	return fn(ctx)
}

func Combine(traces ...CT) CT {
	return CTFunc(func(ctx context.Context) func() {
		return func() {

		}
	})
}

func LogDebugf(logger Logger, format string, args ...interface{}) CT {
	return CTFunc(func(ctx context.Context) func() {
		logger.Debugf(format, args)
		return func() {

		}
	})
}

func LogInfof(logger Logger, format string, args ...interface{}) CT {
	return CTFunc(func(ctx context.Context) func() {
		logger.Infof(format, append(args))
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
