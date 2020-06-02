package broker

import (
	"context"

	"github.com/go-po/po/internal/observer"
	"github.com/go-po/po/internal/observer/binary"
	"github.com/go-po/po/internal/observer/unary"
	"github.com/go-po/po/streams"
	"github.com/prometheus/client_golang/prometheus"
)

func NewDefaultObserver(builder *observer.Builder) *obs {
	return &obs{
		register: builder.Binary().
			LogInfof("po/broker registered stream %s to %s").
			Build(),
		streamNotify: builder.Unary().
			MetricCounterVec(prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: "po_broker_notify_stream_counter",
				Help: "number of messages send for a stream",
			}, []string{"group"})).
			Build(),
		streamNotifyReceived: builder.Unary().
			MetricCounterVec(prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: "po_broker_notify_stream_received_counter",
				Help: "number of messages received for a stream",
			}, []string{"group"})).
			Build(),
		streamErrorDistribute: builder.Binary().
			LogInfof("po/broker stream distribute to %s: %s").
			Build(),
		assignNotify: builder.Unary().
			MetricCounterVec(prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: "po_broker_notify_assign_counter",
				Help: "number of messages send for a group number assignment",
			}, []string{"group"})).
			Build(),
		assignNotifyReceived: builder.Unary().
			MetricCounterVec(prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: "po_broker_notify_assign_received_counter",
				Help: "number of messages received for a group number assignment",
			}, []string{"group"})).
			Build(),
		assignErrorParseId: builder.Unary().
			LogInfof("po/broker assign parse id: %s").
			Build(),
		assignErrorGroupNumber: builder.Binary().
			LogInfof("po/broker assign group number to %s: %s").
			Build(),
	}
}

func NewStubObserver() *obs {
	return &obs{
		register:               binary.Noop(),
		streamNotify:           unary.Noop(),
		streamNotifyReceived:   unary.Noop(),
		streamErrorDistribute:  binary.Noop(),
		assignNotify:           unary.Noop(),
		assignNotifyReceived:   unary.Noop(),
		assignErrorParseId:     unary.Noop(),
		assignErrorGroupNumber: binary.Noop(),
	}
}

type Observer interface {
	Register(ctx context.Context, id streams.Id, subscriberId string) func()

	AssignNotify(ctx context.Context, id streams.Id) func()
	AssignNotifyReceived(ctx context.Context, id streams.Id) func()
	AssignErrorParseId(ctx context.Context, err error) func()
	AssignErrorGroupNumber(ctx context.Context, id streams.Id, err error) func()

	StreamNotify(ctx context.Context, id streams.Id) func()
	StreamNotifyReceived(ctx context.Context, id streams.Id) func()
	StreamErrorDistribute(ctx context.Context, id streams.Id, err error) func()
}

var _ Observer = &obs{}

type obs struct {
	register binary.ClientTrace

	streamNotify          unary.ClientTrace
	streamNotifyReceived  unary.ClientTrace
	streamErrorDistribute binary.ClientTrace

	assignNotify           unary.ClientTrace
	assignNotifyReceived   unary.ClientTrace
	assignErrorParseId     unary.ClientTrace
	assignErrorGroupNumber binary.ClientTrace
}

func (o *obs) StreamErrorDistribute(ctx context.Context, id streams.Id, err error) func() {
	return o.streamErrorDistribute.Observe(ctx, id.Group, err.Error())
}

func (o *obs) StreamNotify(ctx context.Context, id streams.Id) func() {
	return o.streamNotify.Observe(ctx, id.Group)
}

func (o *obs) StreamNotifyReceived(ctx context.Context, id streams.Id) func() {
	return o.streamNotifyReceived.Observe(ctx, id.Group)
}

func (o *obs) Register(ctx context.Context, id streams.Id, subscriberId string) func() {
	return o.register.Observe(ctx, id.Group, subscriberId)
}

func (o *obs) AssignNotify(ctx context.Context, id streams.Id) func() {
	return o.assignNotify.Observe(ctx, id.Group)
}

func (o *obs) AssignNotifyReceived(ctx context.Context, id streams.Id) func() {
	return o.assignNotifyReceived.Observe(ctx, id.Group)
}

func (o *obs) AssignErrorParseId(ctx context.Context, err error) func() {
	return o.assignErrorParseId.Observe(ctx, err.Error())
}

func (o *obs) AssignErrorGroupNumber(ctx context.Context, stream streams.Id, err error) func() {
	return o.assignErrorGroupNumber.Observe(ctx, stream.Group, err.Error())
}
