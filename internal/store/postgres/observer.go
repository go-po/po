package postgres

import (
	"github.com/go-po/po/internal/observer/nullary"
	"github.com/go-po/po/internal/observer/unary"
)

type pgObserver struct {
	ReadSnapshot   unary.ClientTrace
	UpdateSnapshot nullary.ClientTrace
}
