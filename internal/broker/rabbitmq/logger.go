package rabbitmq

type Logger interface {
	Errorf(template string, args ...interface{})
}

type noopLogger struct{}

func (noopLogger) Errorf(template string, args ...interface{}) {
	// noop
}
