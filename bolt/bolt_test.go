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

		randomWrite(t, db, 1<<5)
	}

	func TestBoltRead(t *testing.T) {
		db := setupBolt(t, true)
		defer db.Close()

		randomWrite(t, db, 1<<20)
		defer TrackTime(time.Now(), "bolt read")

		randomRead(t, db, 1<<20)
	}

	func TestBoltConcurrentWrite(t *testing.T) {
		db := setupBolt(t, false)
		defer db.Close()

		defer TrackTime(time.Now(), "bolt concurrent write")
		concurrentWrite(t, db, 1<<17, 1<<12)
	}

	func TestBucket(t *testing.T) {
		db := setupBolt(t, false)
		defer db.Close()

		err := db.Update(func(tx *bolt.Tx) error {
			b1, err := tx.CreateBucket([]byte("a"))
			if err != nil {
				return err
			}

			b2, err := tx.CreateBucket([]byte("b"))
			if err != nil {
				return err
			}

			b1.Put([]byte("k"), []byte("v-a1"))
			b2.Put([]byte("k"), []byte("v-b1"))

			return nil
		})
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}

		err = db.View(func(tx *bolt.Tx) error {
			b1 := tx.Bucket([]byte("a"))
			b2 := tx.Bucket([]byte("b"))

			v1 := b1.Get([]byte("k"))
			v2 := b2.Get([]byte("k"))

			var isSame = true
			for i := 0; i < len(v1); i++ {
				if v1[i] != v2[i] {
					isSame = false
				}
			}
			t.Logf("v1: %v", v1)
			t.Logf("v2: %v", v2)
			if isSame {
				t.Fatalf("values match: %v", v1)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("View failed: %v", err)
		}
	}

	func TestFaultyBucket(t *testing.T) {
		db := setupBolt(t, false)
		defer db.Close()

		db.View(func (txn *bolt.Tx) error {
			b, err := txn.CreateBucket([]byte("a"))
			if err != nil {
				t.Fatalf("failed to create bucket: %v", err)
			}

			val := b.Get([]byte("key"))
			if val != nil {
				t.Fatalf("should have been empty: %x", val)
			}

			return nil
		})
	}

	func TestTransactions(t *testing.T) {
		db := setupBolt(t, false)
		defer db.Close()

		db.Update(func(txn *bolt.Tx) error {
			if _, err := txn.CreateBucket([]byte("myBucket")); err != nil {
				t.Fatalf("failed to create bucket: %v", err)
			}
			return nil
		})

		errBuf := make(chan error)

		go func() {
			timeout := time.After(time.Second * 10)
			count := byte(0)
			for {
				select {
				case <-timeout:
					return
				default:
					err := db.Update(func(txn *bolt.Tx) error {
						b := txn.Bucket([]byte("myBucket"))

						v := []byte{count}
						err1 := b.Put([]byte("k1"), v)
						err2 := b.Put([]byte("k2"), v)
						if err1 != nil || err2 != nil {
							return fmt.Errorf("Put failed: %v %v", err1, err2)
						}
						return nil
					})

					if err != nil {
						errBuf <- err
					}

					count++

				}
			}
			}()

			go func() {
				for {
					db.View(func(txn *bolt.Tx) error {
						b := txn.Bucket([]byte("myBucket"))

						v1 := b.Get([]byte("k1"))
						v2 := b.Get([]byte("k2"))
						if areSlicesEqual(v1, v2) {
							errBuf <- fmt.Errorf("Mismatch found: %x %x", v1, v2)
						}
						return nil
					})
				}
				}()
			}

			func areSlicesEqual(s1, s2 []byte) bool {
				if s1 == nil || s2 == nil {
					return false
				}

				if len(s1) != len(s2) {
					return false
				}

				for i := 0; i < len(s1); i++ {
					if s1[i] != s2[i] {
						return false
					}
				}

				return true
			}
