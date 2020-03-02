package distributor

import (
	"context"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/stream"
)

type recordingSubscription struct {
	id       string
	stream   stream.Id
	handler  stream.Handler
	store    Store
	registry registry
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

	err = s.store.SetPosition(tx, s.id, s.stream, nextPosition)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (s *recordingSubscription) handlePrev(ctx context.Context, last int64) (int64, error) {
	records, err := s.store.ReadRecords(ctx, s.stream, last)
	if err != nil {
		return last, err
	}
	for _, r := range records {
		msg, err := s.registry.ToMessage(r)
		if err != nil {
			return last, err
		}

		err = s.handler.Handle(ctx, msg)
		if err != nil {
			return last, err
		}
		last = s.recordPosition(r)
	}
	return last, nil
}

func (s *recordingSubscription) messagePosition(msg stream.Message) int64 {
	if s.stream.HasEntity() {
		return msg.Number
	}
	return msg.GroupNumber
}
func (s *recordingSubscription) recordPosition(record record.Record) int64 {
	if s.stream.HasEntity() {
		return record.Number
	}
	return record.GroupNumber
}
