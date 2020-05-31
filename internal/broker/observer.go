package broker

import (
	"github.com/go-po/po/internal/observer"
	"github.com/go-po/po/internal/observer/unary"
)

type ProtocolObserver struct {
	Register unary.ClientTrace
}

func DefaultProtocolObserver(builder *observer.Builder) ProtocolObserver {
	return ProtocolObserver{}
}

func StubProtocolObserver() ProtocolObserver {
	return ProtocolObserver{}
}
