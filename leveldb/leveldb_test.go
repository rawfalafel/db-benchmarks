package leveldbBenchmark

import (
	"path"
	"math/rand"
	"testing"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	. "github.com/rawfalafel/db-benchmarks"
)

func setupDB(t *testing.T, noSync bool) *leveldb.DB {
	datadir := SetupDir("leveldb", t)
	datafile := path.Join(datadir, "db")

	opt := &opt.Options{
		NoSync: noSync,
	}

	db, err := leveldb.OpenFile(datafile, opt)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	return db
}

func randomWrite(t *testing.T, db *leveldb.DB, size int, sync bool) {
	s := rand.NewSource(1)
	r := rand.New(s)

	for i := 0; i < size; i++ {
		k, v := GenerateKV(r)
		opt := &opt.WriteOptions{ Sync: sync }
		if err := db.Put(k, v, opt); err != nil {
			t.Fatalf("failed to put: %v", err)
		}
	}
}

func randomRead(t *testing.T, db *leveldb.DB, size int) {
	s := rand.NewSource(1)
	r := rand.New(s)

	for i := 0; i < size; i++ {
		k, v := GenerateKV(r)
		val, err := db.Get(k, nil)
		if err != nil {
			t.Fatalf("failed to get: %v", err)
		}

		if len(val) != len(v) {
			t.Fatalf("failed to get correct value: %v", val)
		}
	}
}

func batchWrite(t *testing.T, db *leveldb.DB, size int, sync bool) {
	s := rand.NewSource(1)
	r := rand.New(s)

	partitionSize := 1 << 12
	numPartitions := size / partitionSize

	for i := 0; i < numPartitions; i++ {
		batch := new(leveldb.Batch)
		for j := 0; j < partitionSize; j++ {
			k, v := GenerateKV(r)
			batch.Put(k, v)
		}
		opt := &opt.WriteOptions{ Sync: sync }
		err := db.Write(batch, opt)
		if err != nil {
			t.Fatalf("failed to batch write: %v", err)
		}
	}
}

func TestLevelDBWrite(t *testing.T) {
	db := setupDB(t, false)
	defer db.Close()

	defer TrackTime(time.Now(), "leveldb concurrent write")
	randomWrite(t, db, 1 << 9, true)
}

func TestLevelDBRead(t *testing.T) {
	db := setupDB(t, true)
	defer db.Close()

	randomWrite(t, db, 1 << 20, false)

	defer TrackTime(time.Now(), "leveldb read")
	randomRead(t, db, 1 << 20)
}

func TestLevelDBBatchWrite(t *testing.T) {
	db := setupDB(t, false)
	defer db.Close()

	defer TrackTime(time.Now(), "leveldb batch write")
	batchWrite(t, db, 1 << 17, true)
}
