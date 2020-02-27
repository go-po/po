package distributor

import (
	"context"
	"github.com/go-po/po/stream"
)

type recordingSubscription struct {
	id      string
	stream  stream.Id
	handler stream.Handler
	store   Store
}

func (s *recordingSubscription) Handle(ctx context.Context, msg stream.Message) error {
	tx, err := s.store.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	lastPosition, err := s.store.GetLastPosition(tx, s.id, s.stream)
	if err != nil {
		return err
	}
	nextPosition := lastPosition + 1
	messagePosition := s.messagePosition(msg)

	if nextPosition > messagePosition {
		// old one, ignore
		return nil
	}
	if nextPosition < messagePosition {
		// TODO too far ahead, request resend
		return nil
	}

	err = s.handler.Handle(ctx, msg)
	if err != nil {
		return err
	}

	err = s.store.SetPosition(tx, s.id, s.stream, nextPosition)
	if err != nil {
		return err
	}
	return nil
}

func (s *recordingSubscription) messagePosition(msg stream.Message) int64 {
	if s.stream.HasEntity() {
		return msg.Number
	}
	return msg.GroupNumber
}
