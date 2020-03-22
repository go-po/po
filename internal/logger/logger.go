package logger

import (
	"fmt"
)

type NoopLogger struct{}

func (*NoopLogger) Debugf(template string, args ...interface{})          {}
func (*NoopLogger) Errorf(template string, args ...interface{})          {}
func (*NoopLogger) Infof(template string, args ...interface{})           {}
func (*NoopLogger) Errf(err error, template string, args ...interface{}) {}

type TestLogger interface {
	Helper()
	Log(args ...interface{})
	Logf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
}

func WrapLogger(s TestLogger) testLogWrapper {
	return testLogWrapper{log: s, fields: ""}
}

type testLogWrapper struct {
	fields string
	log    TestLogger
}

func (l testLogWrapper) Debugf(template string, args ...interface{}) {
	l.log.Helper()
	l.log.Logf(template, args...)
}

func (l testLogWrapper) Errorf(template string, args ...interface{}) {
	l.log.Helper()
	l.log.Logf(template, args...)
}

func (l testLogWrapper) Infof(template string, args ...interface{}) {
	l.log.Helper()
	l.log.Logf(template, args...)
}

func (l testLogWrapper) Errf(err error, template string, args ...interface{}) {
	l.log.Helper()
	l.log.Errorf("error: %s: %s", fmt.Sprintf(template, args...))
}
