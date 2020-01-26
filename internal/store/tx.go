package store

type Tx interface {
	Commit() error
	Rollback() error
}
