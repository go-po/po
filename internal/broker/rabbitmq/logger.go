package rabbitmq

type Logger interface {
	Errorf(template string, args ...interface{})
	Infof(template string, args ...interface{})
}

type noopLogger struct{}

func (noopLogger) Infof(template string, args ...interface{}) {

}
func (noopLogger) Errorf(template string, args ...interface{}) {
	// noop
}
