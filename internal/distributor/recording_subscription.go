package distributor

import (
	"context"
	"github.com/go-po/po/stream"
)

type recordingSubscription struct {
	groupStream bool
	handler     stream.Handler
	store       messageStore
}

func (s *recordingSubscription) Handle(ctx context.Context, msg stream.Message) error {
	tx, err := s.store.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	lastPosition, err := s.store.GetLastPosition(tx)
	if err != nil {
		return err
	}
	nextPosition := lastPosition + 1
	messagePosition := s.messagePosition(msg)
	switch {
	case nextPosition > messagePosition:
		// old one, ignore
		return nil
	case nextPosition < messagePosition:
		nextPosition, err = s.handlePrev(ctx, lastPosition)
		if err != nil {
			return nil
		}
	default: // equal
		err = s.handler.Handle(ctx, msg)
		if err != nil {
			return err
		}
	}

	err = s.store.SetPosition(tx, nextPosition)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (s *recordingSubscription) handlePrev(ctx context.Context, last int64) (int64, error) {
	messages, err := s.store.ReadMessages(ctx, last)
	if err != nil {
		return last, err
	}
	for _, msg := range messages {
		err = s.handler.Handle(ctx, msg)
		if err != nil {
			return last, err
		}
		last = s.messagePosition(msg)
	}
	return last, nil
}

func (s *recordingSubscription) messagePosition(msg stream.Message) int64 {
	if s.groupStream {
		return msg.GroupNumber
	}
	return msg.Number
}
