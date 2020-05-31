package binary

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
	Observe(ctx context.Context, a, b string) func()
}

type ClientTraceFunc func(ctx context.Context, a, b string) func()

func (fn ClientTraceFunc) Observe(ctx context.Context, a, b string) func() {
	return fn(ctx, a, b)
}

func Combine(traces ...ClientTrace) ClientTrace {
	return ClientTraceFunc(func(ctx context.Context, a, b string) func() {
		return func() {

		}
	})
}

func Noop() ClientTrace {
	return ClientTraceFunc(func(ctx context.Context, a, b string) func() {
		return func() {

		}
	})
}

func LogDebugf(logger Logger, format string, args ...interface{}) ClientTrace {
	return ClientTraceFunc(func(ctx context.Context, a, b string) func() {
		logger.Debugf(format, args)
		return func() {

		}
	})
}

func LogInfof(logger Logger, format string, args ...interface{}) ClientTrace {
	return ClientTraceFunc(func(ctx context.Context, a, b string) func() {
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

func (builder *Builder) MetricCounter(counter *prometheus.CounterVec) *Builder {
	builder.metrics.MustRegister(counter)
	builder.traces = append(builder.traces, ClientTraceFunc(func(ctx context.Context, a, b string) func() {
		counter.WithLabelValues(a, b).Inc()
		return func() {}
	}))
	return builder
}

func (builder *Builder) MetricCounterVec(counter *prometheus.CounterVec) *Builder {
	builder.metrics.MustRegister(counter)
	builder.traces = append(builder.traces, ClientTraceFunc(func(ctx context.Context, a, b string) func() {
		counter.WithLabelValues(a, b).Inc()
		return func() {}
	}))
	return builder
}

func (builder *Builder) HistogramVec(histogram *prometheus.HistogramVec) *Builder {
	builder.metrics.MustRegister(histogram)
	builder.traces = append(builder.traces, ClientTraceFunc(func(ctx context.Context, a, b string) func() {
		start := time.Now()
		return func() {
			histogram.WithLabelValues(a, b).Observe(float64(time.Since(start).Milliseconds()))
		}
	}))
	return builder
}
