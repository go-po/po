package postgres

import (
	"context"
	"testing"

	"github.com/go-po/po/internal/store"
	"github.com/stretchr/testify/assert"
)

func TestStorage_Subscriber(t *testing.T) {
	// setup
	conn := databaseConnection(t)
	ctx := context.Background()

	t.Run("update subscriber position", func(t *testing.T) {
		// setup
		tx, err := conn.Begin()
		assert.NoError(t, err)
		id := streamId("subscriberB")
		// execute
		err = updateSubscriberPosition(ctx, tx, id, store.SubscriptionPosition{
			SubscriptionId: "B",
			Position:       -1,
		})
		// verify
		assert.NoError(t, tx.Commit())
		assert.NoError(t, err)

	})

}
