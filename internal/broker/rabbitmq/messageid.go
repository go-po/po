package rabbitmq

import (
	"fmt"
	"github.com/go-po/po/internal/record"
	"strconv"
	"strings"
)

func stripFirstDigit(s string) (int64, string, error) {
	i := strings.Index(s, "#")
	if i < 0 {
		return 0, "", fmt.Errorf("number not found: %s", s)
	}
	n, err := strconv.ParseInt(s[0:i], 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("number [%s] not found: %s", s[0:i], s)
	}
	return n, s[i+1:], nil
}

func parseMessageId(messageId string) (stream string, number, groupNumber int64, err error) {
	number, messageId, err = stripFirstDigit(messageId)
	if err != nil {
		return
	}
	groupNumber, messageId, err = stripFirstDigit(messageId)
	if err != nil {
		return
	}
	stream = messageId
	return
}

func toMessageId(record record.Record) string {
	return fmt.Sprintf("%d#%d#%s", record.Number, record.GroupNumber, record.Stream)
}
