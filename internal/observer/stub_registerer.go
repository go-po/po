package observer

import "github.com/prometheus/client_golang/prometheus"

func NewPromStub() promStub {
	return promStub{}
}

type promStub struct {
}

func (promStub) Register(collector prometheus.Collector) error {
	return nil
}

func (promStub) MustRegister(collector ...prometheus.Collector) {
	return
}

func (promStub) Unregister(collector prometheus.Collector) bool {
	return true
}
