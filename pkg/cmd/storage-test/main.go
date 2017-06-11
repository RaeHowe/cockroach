package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/elastic/gosigar"
	"github.com/pkg/errors"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {
	chunked := flag.Bool("chunked", false, "do chunked sort")
	sortConcurrency := flag.Int("sort_concurrency", 5, "how many sorts in parallel")
	tmpPath := flag.String("tmp_path", "/tmp/bench", "")

	useRocksDB := flag.Bool("rocksdb", false, "use rocksdb")
	rocksdbPath := flag.String("rocksdb_path", "/tmp/bench", "")
	readFile := flag.String("read_file", "", "read a file")
	numRowsPtr := flag.Int("rows", 1000000, "number of rows")
	rowSizePtr := flag.Int("row_size", 200, "approximate size of each row")
	flag.Parse()

	if *chunked {
		streamSort(*numRowsPtr, *rowSizePtr, *tmpPath, *sortConcurrency)
	} else if *useRocksDB {
		check(writeToRocksDB(*numRowsPtr, *rowSizePtr, *rocksdbPath))
	} else if *readFile == "" {
		genFile(*numRowsPtr, *rowSizePtr)
	} else {
		readSortedFile(*readFile)
	}
}

func genFile(numRows int, rowSize int) {
	// rowData := make([]byte, rowSize)
	// // !!! rand.Read(rowData)
	// for i := range rowData {
	//   rowData[i] = 'a'
	// }
	log.Infof(context.TODO(), "starting writing to file.")
	rowData := randBytes(rowSize)
	rowData[len(rowData)-1] = '\n'

	w := bufio.NewWriter(os.Stdout)
	for i := 0; i < numRows; i++ {
		key := rand.Int63()
		w.WriteString(strconv.Itoa(int(key)))
		w.WriteRune(' ')
		_, err := w.Write(rowData)
		check(err)
	}
	w.Flush()
	log.Infof(context.TODO(), "finished writing to file.")
	os.Stderr.WriteString("done\n")
	// sort -k1n --numeric-sort --parallel=<n> --output=<file>
	// -nk1.6,1.8 -s
}

func readSortedFile(path string) {
	fmt.Printf("reading file %s\n", path)
	f, err := os.Open(path)
	check(err)

	r := bufio.NewReader(f)
	rows := 0
	for {
		_, isPrefix, err := r.ReadLine()
		if err == io.EOF {
			break
		}
		if !isPrefix {
			rows++
		}
	}
	fmt.Printf("reading file got rows: %d\n", rows)
}

func randBytes(n int) []byte {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Int63()%int64(len(letters))]
	}
	return b
}

func writeToRocksDB(numRows int, rowSize int, rocksdbPath string) error {
	fmt.Println("running rocksdb sort")
	// Use as much disk space as possible.
	fsu := gosigar.FileSystemUsage{}
	if err := fsu.Get(rocksdbPath); err != nil {
		return errors.Wrapf(err, "could not get filesystem usage")
	}
	cache := engine.NewRocksDBCache(0 /* size */)
	r, err := engine.NewRocksDB(
		roachpb.Attributes{},
		rocksdbPath,
		cache,
		int64(fsu.Total),
		10000, /* open file limit */
	)
	if err != nil {
		return err
	}
	defer func() {
		stats, err := r.GetStats()
		check(err)
		log.Infof(context.TODO(), "rocks final stats: %+v", stats)
		r.Close()
		os.RemoveAll(rocksdbPath)
		os.Mkdir(rocksdbPath, 0700)
	}()

	rowData := randBytes(rowSize)
	rowData[len(rowData)-1] = '\n'

	var b engine.Batch
	for i := 0; i < numRows; i++ {
		if i%10000 == 0 {
			if b != nil {
				b.Commit(false /* sync */)
			}
			b = r.NewWriteOnlyBatch()
		}
		k := rand.Int63()

		var key [8]byte
		binary.LittleEndian.PutUint64(key[:], uint64(k))

		// // Assume ascending. Note that the comparator isn't set.
		// key, err := val.Encode(&ss.rows.datumAlloc, sqlbase.DatumEncoding_ASCENDING_KEY, key)
		// if err != nil {
		//   return err
		// }
		if err := b.Put(engine.MVCCKey{Key: roachpb.Key(key[:])}, rowData); err != nil {
			return err
		}
	}
	b.Commit(false /* sync */)

	log.Infof(context.TODO(), "finished writing to rocksdb. Now reading.")

	i := r.NewIterator(false /* prefix */)
	defer i.Close()

	i.Seek(engine.NilKey)
	rows := 0
	for {
		ok, err := i.Valid()
		if err != nil {
			return err
		} else if !ok {
			break
		}
		// !!! key := i.Key().Key
		rows++
		i.Next()
	}

	log.Infof(context.TODO(), "rocksdb got rows: %d", rows)
	fmt.Printf("rocksdb got rows: %d\n", rows)
	return nil
}

func streamSort(numRows int, rowSize int, tmpPath string, sortConcurrency int) {
	log.Infof(context.TODO(), "starting writing chunks.")
	sem = make(chan struct{}, sortConcurrency)

	rowData := randBytes(rowSize)
	rowData[len(rowData)-1] = '\n'

	sortLimitMB := 370 / sortConcurrency
	curChunkFile, curChunkPath := makeChunkFile(tmpPath, chunkNum)
	w := bufio.NewWriter(curChunkFile)
	for i := 0; i < numRows; i++ {
		key := rand.Int63()
		w.WriteString(strconv.Itoa(int(key)))
		w.WriteRune(' ')
		_, err := w.Write(rowData)
		check(err)

		if i != 0 && i%2000000 == 0 {
			w.Flush()
			log.Infof(context.TODO(), "starting sort of chunk: %d", chunkNum)
			startChunkSort(chunkNum, curChunkPath, tmpPath, sortLimitMB)
			chunkNum++
			curChunkFile, curChunkPath = makeChunkFile(tmpPath, chunkNum)
			w = bufio.NewWriter(curChunkFile)
		}
	}
	w.Flush()
	log.Infof(context.TODO(), "starting sort of final chunk: %d", chunkNum)
	startChunkSort(chunkNum, curChunkPath, tmpPath, sortLimitMB)
	log.Infof(context.TODO(), "finished writing chunks. Waiting for all sorts to finish.")
	chunkWait.Wait()
}

var chunkNum = 0
var chunkWait sync.WaitGroup
var sem chan struct{}
var stopper = stop.NewStopper()

func makeChunkFileName(tmpPath string, chunkNum int) string {
	return fmt.Sprintf("%s/chunk-%d", tmpPath, chunkNum)
}

func makeChunkFile(tmpPath string, chunkNum int) (*os.File, string) {
	path := makeChunkFileName(tmpPath, chunkNum)
	f, err := os.Create(path)
	check(err)
	return f, path
}

func startChunkSort(chunkNum int, chunkPath string, tmpPath string, memLimitMB int) {
	chunkWait.Add(1)

	if err := stopper.RunLimitedAsyncTask(context.TODO(), sem, true, /* wait */
		func(context.Context) {
			cmd := exec.Command("sort",
				"-S", fmt.Sprintf("%dM", memLimitMB),
				"-T", tmpPath,
				"--parallel=1",
				"-k1.1,1.15",
				"-s",
				fmt.Sprintf("--output=%s/sorted-chunk-%d", tmpPath, chunkNum),
				chunkPath)
			check(cmd.Start())
			check(cmd.Wait())
			chunkWait.Done()
			log.Infof(context.TODO(), "finished sorting of chunk: %d", chunkNum)
		}); err != nil {
		check(err)
	}
}
