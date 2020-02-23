package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-po/po"
	"github.com/go-po/po/stream"
	"math"
)

type Car struct {
	Speed float64
}

func init() {
	po.RegisterMessages(
		func(b []byte) (interface{}, error) {
			msg := Car{}
			err := json.Unmarshal(b, &msg)
			return msg, err
		},
	)
}

type CarCounter struct {
	count int64
}

func (counter *CarCounter) Handle(ctx context.Context, msg stream.Message) error {
	fmt.Printf("car\n")
	switch msg.Data.(type) {
	case Car:
		counter.count = counter.count + 1
	}
	return nil
}

type SpeedMonitor struct {
	max   float64
	min   float64
	avg   float64
	total float64
	count float64
}

func (monitor *SpeedMonitor) Handle(ctx context.Context, msg stream.Message) error {
	switch event := msg.Data.(type) {
	case Car:
		monitor.count = monitor.count + 1
		monitor.min = math.Min(monitor.min, event.Speed)
		monitor.max = math.Max(monitor.max, event.Speed)
		monitor.total = monitor.total + event.Speed
		monitor.avg = monitor.total / monitor.count
	}
	return nil
}
