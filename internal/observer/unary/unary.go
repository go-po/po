package unary

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
)

type Logger interface {
	Debugf(template string, args ...interface{})
	Errorf(template string, args ...interface{})
	Infof(template string, args ...interface{})
	Errf(err error, template string, args ...interface{})
}

type ClientTrace interface {
	Observe(ctx context.Context, a string) func()
}

type ClientTraceFunc func(ctx context.Context, a string) func()

func (fn ClientTraceFunc) Observe(ctx context.Context, a string) func() {
	return fn(ctx, a)
}
func Combine(traces ...ClientTrace) ClientTrace {
	return ClientTraceFunc(func(ctx context.Context, a string) func() {
		return func() {

		}
	})
}

func LogDebugf(logger Logger, format string, args ...interface{}) ClientTrace {
	return ClientTraceFunc(func(ctx context.Context, a string) func() {
		logger.Debugf(format, append(args, a))
		return func() {

		}
	})
}

func LogInfof(logger Logger, format string, args ...interface{}) ClientTrace {
	return ClientTraceFunc(func(ctx context.Context, a string) func() {
		logger.Infof(format, append(args, a))
		return func() {

		}
	})
}

func NewBuilder(logger Logger, metrics prometheus.Registerer) *Builder {
	return &Builder{
		logger:  logger,
		metrics: metrics,
	}
}

type Builder struct {
	logger  Logger
	metrics prometheus.Registerer
	traces  []ClientTrace
}

func (builder *Builder) Build() ClientTrace {
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

func (builder *Builder) MetricCounter(counter *prometheus.CounterVec) *Builder {
	builder.metrics.MustRegister(counter)
	builder.traces = append(builder.traces, ClientTraceFunc(func(ctx context.Context, a string) func() {
		counter.WithLabelValues(a).Inc()
		return func() {}
	}))
	return builder
}
