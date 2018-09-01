package benchmark

import (
	"os"
	"math/rand"
	"path"
	"testing"
	"time"
	"log"
)

func SetupDir(dirname string, t *testing.T) string {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to open dir: %v", err)
	}
	datadir := path.Join(dir, "..", "data", dirname)

	if err := os.RemoveAll(datadir); err != nil {
		t.Fatalf("failed to clean dir: %v", err)
	}

	if err := os.MkdirAll(datadir, 0700); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	return datadir
}

func TrackTime(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}

func GenerateKV(r *rand.Rand) ([]byte, []byte) {
	key := make([]byte, 32)
	r.Read(key)

	value := make([]byte, 300)
	r.Read(value)

	return key, value
}
