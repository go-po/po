package postgres

import (
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/internal/store/postgres/generated/db"
	"github.com/go-po/po/streams"
)

func subscriberPositionLock(conn db.DBTX, id streams.Id, ids ...string) ([]store.SubscriptionPosition, error) {
	return nil, nil
}

func subsriberPositionUpdate(conn db.DBTX, id streams.Id, position store.SubscriptionPosition) error {
	return nil
}
