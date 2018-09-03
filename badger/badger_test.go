package badgerBench

import (
	"math/rand"
	"testing"
	"time"

	"github.com/dgraph-io/badger"
	. "github.com/rawfalafel/db-benchmarks"
)

func setupBadger(t *testing.T, syncWrites bool) *badger.DB {
	datadir := SetupDir("badger", t)

	opts := badger.DefaultOptions
	opts.Dir = datadir
	opts.ValueDir = datadir
	opts.SyncWrites = syncWrites

	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	return db
}

func randomWrite(t *testing.T, db *badger.DB, size int) {
	s := rand.NewSource(1)
	r := rand.New(s)

	for i := 0; i < size; i++ {
		key := make([]byte, 32)
		r.Read(key)

		value := make([]byte, 300)
		r.Read(value)

		err := db.Update(func(txn *badger.Txn) error {
			if err := txn.Set(key, value); err != nil {
				return err
			}

			return nil
		})

		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}
	}
}

func batchWrite(t *testing.T, db *badger.DB, size int) {
	s := rand.NewSource(1)
	r := rand.New(s)

	txn := db.NewTransaction(true)
	for i:= 0; i < size; i++ {
		key := make([]byte, 32)
		r.Read(key)

		value := make([]byte, 300)
		r.Read(value)

		if err := txn.Set(key, value); err == badger.ErrTxnTooBig {
			t.Log("transaction size limit reached: committing...")
			if err := txn.Commit(nil); err != nil {
				t.Fatalf("failed to commit: %v", err)
			}

			txn = db.NewTransaction(true)
			if err := txn.Set(key, value); err != nil {
				t.Fatalf("failed to set: %v", err)
			}
		}
	}

	if err := txn.Commit(nil); err != nil {
		t.Fatalf("failed final commit: %v", err)
	}
}

func randomRead(t *testing.T, db *badger.DB, size int) {
	s := rand.NewSource(1)
	r := rand.New(s)

	for i := 0; i < size; i++ {
		key := make([]byte, 32)
		r.Read(key)

		value := make([]byte, 300)
		r.Read(value)

		err := db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(key)
			if err != nil {
				t.Fatalf("failed to get item: %v", err)
			}

			v, err := item.Value()
			if err != nil {
				t.Fatalf("failed to retrieve value: %v", err)
			}
			if len(v) != len(value) {
				t.Fatalf("read value does not match: %v", v)
			}

			return nil
		})

		if err != nil {
			t.Fatalf("update failed: %v", err)
		}
	}
}

func TestBadgerWrite(t *testing.T) {
	db := setupBadger(t, true)
	defer db.Close()
	defer TrackTime(time.Now(), "badger write")

	randomWrite(t, db, 1 << 11)
}

func TestBadgerBatchWrite(t *testing.T) {
	db := setupBadger(t, true)
	defer db.Close()

	defer TrackTime(time.Now(), "badger batch write")
	batchWrite(t, db, 1 << 17)
}

func TestBadgerRead(t *testing.T) {
	db := setupBadger(t, false)
	defer db.Close()
	batchWrite(t, db, 1 << 19)

	defer TrackTime(time.Now(), "badger read")
	randomRead(t, db, 1 << 19)
}
