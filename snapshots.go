package po

import (
	"context"
	"encoding/json"

	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/streams"
)

type snapshotStore interface {
	ReadSnapshot(ctx context.Context, id streams.Id, snapshotId string) (record.Snapshot, error)
	UpdateSnapshot(ctx context.Context, id streams.Id, snapshotId string, snapshot record.Snapshot) error
}

func newSnapshots(store snapshotStore) *snapper {
	return &snapper{
		store: store,
	}
}

// Takes snapshots
// is request scoped
type snapper struct {
	store snapshotStore
}

type snapshotCommit func(position int64) error

var noopCommits snapshotCommit = func(_ int64) error { return nil }

func (s *snapper) Snapshot(ctx context.Context, id streams.Id, projection Handler) (snapshotCommit, int64, error) {
	snap, supportsSnapshot := projection.(streams.NamedSnapshot)
	if supportsSnapshot {
		snapshot, err := s.store.ReadSnapshot(ctx, id, snap.SnapshotName())
		if err != nil {
			return noopCommits, -1, err
		}

		err = json.Unmarshal(snapshot.Data, projection)
		if err != nil {
			return noopCommits, -1, err
		}
		return func(position int64) error {
			b, err := json.Marshal(projection)
			if err != nil {
				return err
			}

			err = s.store.UpdateSnapshot(ctx, id, snap.SnapshotName(), record.Snapshot{
				Data:        b,
				Position:    position,
				ContentType: "application/json",
			})
			if err != nil {
				return err
			}
			return nil
		}, snapshot.Position, nil
	}
	return noopCommits, -1, nil
}
