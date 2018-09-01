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

func randomWrite(t *testing.T, db *leveldb.DB, size int) {
	s := rand.NewSource(1)
	r := rand.New(s)

	for i := 0; i < size; i++ {
		k, v := GenerateKV(r)
		if err := db.Put(k, v, nil); err != nil {
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

func TestLevelDBWrite(t *testing.T) {
	db := setupDB(t, true)
	defer db.Close()

	defer TrackTime(time.Now(), "leveldb concurrent write")
	randomWrite(t, db, 2 << 15)
}

func TestLevelDBRead(t *testing.T) {
	db := setupDB(t, true)
	defer db.Close()

	randomWrite(t, db, 2 << 20)

	defer TrackTime(time.Now(), "leveldb concurrent write")
	randomRead(t, db, 2 << 20)
}
