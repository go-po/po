package nullary

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Logger interface {
	Debugf(template string, args ...interface{})
	Errorf(template string, args ...interface{})
	Infof(template string, args ...interface{})
	Errf(err error, template string, args ...interface{})
}

type ClientTrace interface {
	Observe(ctx context.Context) func()
}

type ClientTraceFunc func(ctx context.Context) func()

func (fn ClientTraceFunc) Observe(ctx context.Context) func() {
	return fn(ctx)
}

func Combine(traces ...ClientTrace) ClientTrace {
	return ClientTraceFunc(func(ctx context.Context) func() {
		return func() {

		}
	})
}

func LogDebugf(logger Logger, format string, args ...interface{}) ClientTrace {
	return ClientTraceFunc(func(ctx context.Context) func() {
		logger.Debugf(format, args)
		return func() {

		}
	})
}

func LogInfof(logger Logger, format string, args ...interface{}) ClientTrace {
	return ClientTraceFunc(func(ctx context.Context) func() {
		logger.Infof(format, append(args))
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

func (builder *Builder) MetricCounter(counter prometheus.Counter) *Builder {
	builder.metrics.MustRegister(counter)
	builder.traces = append(builder.traces, ClientTraceFunc(func(ctx context.Context) func() {
		counter.Inc()
		return func() {}
	}))
	return builder
}

func (builder *Builder) Histogram(histogram prometheus.Histogram) *Builder {
	builder.metrics.MustRegister(histogram)
	builder.traces = append(builder.traces, ClientTraceFunc(func(ctx context.Context) func() {
		start := time.Now()
		return func() {
			histogram.Observe(float64(time.Since(start).Milliseconds()))
		}
	}))
	return builder
}
