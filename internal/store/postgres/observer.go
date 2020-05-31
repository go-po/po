package postgres

import (
	"github.com/go-po/po/internal/observer/binary"
	"github.com/go-po/po/internal/observer/nullary"
)

type pgObserver struct {
	ReadSnapshot   binary.ClientTrace
	UpdateSnapshot nullary.ClientTrace
}
