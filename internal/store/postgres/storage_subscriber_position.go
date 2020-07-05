package postgres

import (
	"context"

	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/internal/store/postgres/generated/db"
	"github.com/go-po/po/streams"
)

func subscriberPositionLock(ctx context.Context, conn db.DBTX, id streams.Id, ids ...string) ([]store.SubscriptionPosition, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	dao := db.New(conn)
	positions, err := dao.LockSubscriberPosition(ctx, db.LockSubscriberPositionParams{
		Stream:       id.String(),
		SubscriberID: ids,
	})
	if err != nil {
		return nil, err
	}
	var result []store.SubscriptionPosition
	for _, pos := range positions {
		result = append(result, store.SubscriptionPosition{
			SubscriptionId: pos.SubscriberID,
			Position:       pos.No,
		})
	}
	return result, nil
}

func updateSubscriberPosition(ctx context.Context, conn db.DBTX, id streams.Id, position store.SubscriptionPosition) error {
	return db.New(conn).SetSubscriberPosition(ctx, db.SetSubscriberPositionParams{
		No:           position.Position,
		Stream:       id.String(),
		SubscriberID: position.SubscriptionId,
	})
}
