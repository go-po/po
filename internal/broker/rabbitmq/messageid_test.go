package rabbitmq

import (
	"github.com/go-po/po/internal/record"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_ParseMessageId(t *testing.T) {
	messageId := toMessageId(record.Record{
		Number:      5,
		Stream:      "my stream name",
		GroupNumber: 15,
	})

	assert.Equal(t, "5#15#my stream name", messageId)

	stream, number, groupNumber, err := parseMessageId(messageId)

	if assert.NoError(t, err) {
		assert.Equal(t, "my stream name", stream)
		assert.Equal(t, int64(5), number)
		assert.Equal(t, int64(15), groupNumber)
	}
}
