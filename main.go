package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	mrand "math/rand"
	"os"
	"path/filepath"
	"strconv"
	"time"

	tdb "github.com/tendermint/tendermint/libs/db"
)

const (
	totalSteps    = 100
	eachStepScale = 100

	valueLen = 24

	testDir = "."
)

func cleanupDBDir(name, dir string) {
	if err := os.RemoveAll(filepath.Join(dir, name) + ".db"); err != nil {
		panic(err)
	}
}

type dbTestFunc = func(int, int, tdb.DB) string

func testDB(name, dir string, dtype tdb.DBBackendType, steps, stepScale int, suite map[string]dbTestFunc) {
	db := tdb.NewDB(name, dtype, dir)
	defer cleanupDBDir(name, dir)

	for step := 0; step < totalSteps; step++ {
		scale := step * eachStepScale
		for testName, test := range suite {
			fmt.Printf("%s, %s, %d, %s\n", name, testName, scale, test(stepScale, scale, db))
		}
		fmt.Printf("! step %d done\n", step+1)
	}

	db.Close()
}

func setSync(stepScale, scale int, db tdb.DB) string {
	start := time.Now()
	for i := 0; i < stepScale; i++ {
		key := []byte(strconv.Itoa(scale + i))
		val := make([]byte, valueLen)
		if _, err := rand.Read(val); err != nil {
			panic(err)
		}

		db.SetSync(key, []byte(hex.EncodeToString(val)))
	}
	return fmt.Sprintf("setSync, %d, %dms", stepScale, time.Since(start).Milliseconds())
}

func setInBatch(stepScale, scale int, db tdb.DB) string {
	start := time.Now()

	batch := db.NewBatch()
	defer batch.Close()

	for i := 0; i < stepScale; i++ {
		key := []byte(strconv.Itoa(scale + i))
		val := make([]byte, valueLen)
		if _, err := rand.Read(val); err != nil {
			panic(err)
		}
		batch.Set(key, []byte(hex.EncodeToString(val)))
	}
	// for goleveldb backend, Write & WriteSync are actually the same thing
	// but for others, it's different
	batch.WriteSync()

	return fmt.Sprintf("setInBatch, %d, %dms", stepScale, time.Since(start).Milliseconds())
}

func getRand(stepScale, scale int, db tdb.DB) string {
	start := time.Now()
	getCount := stepScale / 100
	for i := 0; i < getCount; i++ {
		key := []byte(strconv.Itoa(mrand.Intn(scale + stepScale)))
		db.Get(key)
	}
	return fmt.Sprintf("getRand, %d, %dms", getCount, time.Since(start).Milliseconds())
}

func stat(stepScale, scale int, db tdb.DB) string {
	statStr := "["
	start := time.Now()
	for key, val := range db.Stats() {
		statStr += ("," + key + ":" + val)
	}
	statStr += "]"
	return fmt.Sprintf("stat, %s, %dms", statStr, time.Since(start).Milliseconds())
}

// find a way to warp this & make it work
// TODO: fix this
func reopen(name, dir string, dtype tdb.DBBackendType, scale int, db tdb.DB) string {
	start := time.Now()
	db.Close()

	db = tdb.NewDB(name, dtype, dir)
	return fmt.Sprintf("reopen, %dms", time.Since(start).Milliseconds())
}

func main() {
	mrand.Seed(time.Now().UnixNano())

	// direct
	directSuite := map[string]dbTestFunc{
		"setSync": setSync,
		"getRand": getRand,
	}
	testDB("direct_fsdb", testDir, tdb.FSDBBackend, 10, eachStepScale, directSuite)
	testDB("direct_goleveldb", testDir, tdb.GoLevelDBBackend, totalSteps, eachStepScale, directSuite)

	// batch
	batchSuite := map[string]dbTestFunc{
		"setInBatch": setInBatch,
		"getRand":    getRand,
	}
	testDB("batch_goleveldb", testDir, tdb.GoLevelDBBackend, totalSteps, eachStepScale, batchSuite)
}
