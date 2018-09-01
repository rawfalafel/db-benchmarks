package boltBenchmark

import (
	"fmt"
	"math/rand"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	. "github.com/rawfalafel/db-benchmarks"
)

func setupBolt(t *testing.T, noSync bool) *bolt.DB {
	datadir := SetupDir("bolt", t)
	datafile := path.Join(datadir, "bolt.db")

	db, err := bolt.Open(datafile, 0600, nil)
	db.NoSync = noSync

	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	return db
}

func _randomWrite(db *bolt.DB, r *rand.Rand) error {
	k, v := GenerateKV(r)

	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("MyBucket"))
		if err != nil {
			return fmt.Errorf("create bucket: %v", err)
		}

		return b.Put(k, v)
	})
	if err != nil {
		return fmt.Errorf("update failed: %v", err)
	}

	return nil
}

func _batchWrite(db *bolt.DB, r *rand.Rand) error {
	k, v := GenerateKV(r)

	err := db.Batch(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("MyBucket"))
		if err != nil {
			return fmt.Errorf("create bucket: %v", err)
		}

		return b.Put(k, v)
	})
	if err != nil {
		return fmt.Errorf("batch update failed: %v", err)
	}

	return nil
}

func _randomRead(db *bolt.DB, r *rand.Rand) error {
	key := make([]byte, 32)
	r.Read(key)

	value := make([]byte, 300)
	r.Read(value)

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("MyBucket"))
		v := b.Get(key)
		if len(v) != len(value) {
			return fmt.Errorf("read value does not match: %v", v)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("update failed: %v", err)
	}

	return nil
}

func randomWrite(t *testing.T, db *bolt.DB, size int) {
	s := rand.NewSource(1)
	r := rand.New(s)
	for i := 0; i < size; i++ {
		if err := _randomWrite(db, r); err != nil {
			t.Fatal(err)
		}
	}
}

func randomRead(t *testing.T, db *bolt.DB, size int) {
	s := rand.NewSource(1)
	r := rand.New(s)
	for i := 0; i < size; i++ {
		if err := _randomRead(db, r); err != nil {
			t.Fatal(err)
		}
	}
}

func concurrentWrite(t *testing.T, db *bolt.DB, size, partitions int) {
	wg := sync.WaitGroup{}
	sizePerPartition := size / partitions
	var batchErr error
	for i := 0; i < partitions; i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()

			s := rand.NewSource(seed)
			r := rand.New(s)

			err := db.Batch(func(tx *bolt.Tx) error {
				b, err := tx.CreateBucketIfNotExists([]byte("mybucket"))
				if err != nil {
					return fmt.Errorf("failed to create bucket: %v", err)
				}

				for i := 0; i < sizePerPartition; i++ {
					k, v := GenerateKV(r)
					err := b.Put(k, v)
					if err != nil {
						return fmt.Errorf("failed to put bucket: %v", err)
					}
				}
				return nil
			})

			batchErr = err
		}(int64(i))
	}
	wg.Wait()
	if batchErr != nil {
		t.Fatalf("batch write failed: %v", batchErr)
	}
}

func TestBoltWrite(t *testing.T) {
	db := setupBolt(t, false)
	defer db.Close()
	defer TrackTime(time.Now(), "bolt write")

	randomWrite(t, db, 2<<10)
}

func TestBoltRead(t *testing.T) {
	db := setupBolt(t, true)
	defer db.Close()

	randomWrite(t, db, 2<<20)
	defer TrackTime(time.Now(), "bolt read")

	randomRead(t, db, 2<<10)
}

func TestBoltConcurrentWrite(t *testing.T) {
	db := setupBolt(t, false)
	defer db.Close()

	defer TrackTime(time.Now(), "bolt concurrent write")
	concurrentWrite(t, db, 2<<18, 2<<13)
}
