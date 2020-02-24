package domain

import (
	"context"
	"encoding/json"
	"github.com/go-po/po"
	"github.com/go-po/po/stream"
	"log"
	"math"
	"sync"
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
	mu    sync.Mutex
	count int
}

func (counter *CarCounter) Handle(ctx context.Context, msg stream.Message) error {
	counter.mu.Lock()
	defer counter.mu.Unlock()
	switch msg.Data.(type) {
	case Car:
		counter.count = counter.count + 1
	}
	return nil
}

func (counter *CarCounter) Count() int {
	return counter.count
}

func NewSpeedMonitor() *SpeedMonitor {
	return &SpeedMonitor{
		mu:  sync.Mutex{},
		max: 0,
		min: 100,
	}
}

type SpeedMonitor struct {
	mu sync.Mutex

	max   float64
	min   float64
	avg   float64
	total float64
	count float64
}

func (m *SpeedMonitor) Handle(ctx context.Context, msg stream.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch event := msg.Data.(type) {
	case Car:
		m.count = m.count + 1
		m.min = math.Min(m.min, event.Speed)
		m.max = math.Max(m.max, event.Speed)
		m.total = m.total + event.Speed
		m.avg = m.total / m.count
	}
	return nil
}

func (m *SpeedMonitor) PrintStats() {
	log.Printf("Speeds %.2f < %.2f < %.2f", m.min, m.avg, m.max)
	log.Printf("Total %.0f / %.0f = %.2f", m.total, m.count, m.avg)
}
