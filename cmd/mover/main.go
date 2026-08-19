package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	goRedis "github.com/go-redis/redis/v8"
)

func main() {

	isExport := flag.Bool("export", false, "export redis to file")
	isImport := flag.Bool("import", false, "import redis to file")
	dataFile := flag.String("file", "", "path to data file")
	pattern := flag.String("pattern", "", "pattern to export")
	redis := flag.String("redis", "", "url to redis")

	flag.Parse()

	if !*isExport && !*isImport {
		fmt.Println("Need either export or import flag")
		os.Exit(1)
	}

	if dataFile == nil || *dataFile == "" {
		fmt.Println("Need data-file location")
		os.Exit(1)
	}

	if redis == nil || *redis == "" {
		fmt.Println("Need redis url")
		os.Exit(1)
	}

	if *isExport {
		exportFn(*redis, *dataFile, *pattern)
	}
	if *isImport {
		importFn(*redis, *dataFile)
	}
}

type Data struct {
	Version   int                   `json:"version"`
	DumpStart time.Time             `json:"dump_start"`
	DumpEnd   time.Time             `json:"dump_end"`
	Db        int                   `json:"db"`
	Data      map[string]DumpRecord `json:"data"`
}

type DumpRecord struct {
	Dump      []byte `json:"dump"`
	TTLMillis int64  `json:"ttl_ms"`
}

const exportFormatVersion = 1

func promptConfirm() bool {
	fmt.Printf("Continue? [y/N] ")
	input := bufio.NewScanner(os.Stdin)
	input.Scan()
	fmt.Printf("\n")
	return strings.ToLower(input.Text()) == "y"
}

func exportFn(redis string, file string, pattern string) {
	redisParts := strings.Split(redis, "@")
	fmt.Printf("Exporting data from redis (%s) to data-file (%s)\n", redis, file)
	if !promptConfirm() {
		fmt.Printf("\nAborting...")
		os.Exit(0)
	}

	if _, err := os.Stat(file); err == nil {
		fmt.Printf("\n%s already exists. Aborting...", file)
		os.Exit(1)
	}

	dbs := []int{}
	if len(redisParts) > 1 {
		for _, dbnum := range strings.Split(redisParts[1], ",") {
			dbint, err := strconv.Atoi(dbnum)
			if err == nil {
				dbs = append(dbs, dbint)
			}
		}
	} else {
		dbs = append(dbs, 0)
	}

	dbData := map[int]Data{}
	for _, dbNum := range dbs {
		rdb := goRedis.NewClient(&goRedis.Options{
			Addr:     redisParts[0],
			Password: "",
			DB:       dbNum,
		})

		keys, err := rdb.Keys(context.Background(), pattern+"*").Result()
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		data := Data{
			Version:   exportFormatVersion,
			DumpStart: time.Now(),
			Db:        dbNum,
			Data:      map[string]DumpRecord{},
		}
		for _, key := range keys {
			dump, err := rdb.Dump(context.Background(), key).Result()
			if err == goRedis.Nil {
				// The key expired or was deleted after KEYS returned it.
				continue
			}
			if err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			}

			ttl, err := rdb.PTTL(context.Background(), key).Result()
			if err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			}
			if ttl == -2 {
				// The key expired or was deleted after DUMP returned it.
				continue
			}

			ttlMillis := int64(0)
			if ttl != -1 {
				ttlMillis = ttl.Milliseconds()
			}

			if _, ok := data.Data[key]; ok {
				fmt.Printf("Conflicting key: %s\nAborting...", key)
				os.Exit(1)
			}
			fmt.Printf("Exporting key: %s (dump len: %d, ttl: %dms)\n", key, len(dump), ttlMillis)
			data.Data[key] = DumpRecord{
				Dump:      []byte(dump),
				TTLMillis: ttlMillis,
			}
		}
		data.DumpEnd = time.Now()
		rdb.Close()

		dbData[dbNum] = data
	}

	if len(dbs) == 1 {
		jsonDump, err := json.MarshalIndent(dbData[dbs[0]], "", " ")
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		err = ioutil.WriteFile(file, jsonDump, 0644)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
		fmt.Printf("\nExport done, %d keys exported", len(dbData[dbs[0]].Data))

	} else {

		jsonDump, err := json.MarshalIndent(dbData, "", " ")
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		err = ioutil.WriteFile(file, jsonDump, 0644)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
		fmt.Printf("\nExport done, %d dbs exported", len(dbData))

	}

}

func importFn(redis string, file string) {
	redisParts := strings.Split(redis, "@")
	fmt.Printf("Importing data from data-file (%s) to redis (%s)\n", file, redis)

	if !promptConfirm() {
		fmt.Printf("\nAborting...")
		os.Exit(0)
	}

	byteValue, err := os.ReadFile(file)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	// Eerst proberen als multi-db export
	multiData := map[int]Data{}
	if err := json.Unmarshal(byteValue, &multiData); err == nil && len(multiData) > 0 {
		for dbNum, data := range multiData {
			if err := validateData(data); err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			}
			importDataToDb(redisParts[0], dbNum, data)
		}
		fmt.Printf("\nImport done, %d dbs imported", len(multiData))
		return
	}

	// Fallback: single-db export
	data := Data{}
	if err := json.Unmarshal(byteValue, &data); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	if err := validateData(data); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	db := data.Db
	if len(redisParts) > 1 {
		db, _ = strconv.Atoi(redisParts[1])
	}

	importDataToDb(redisParts[0], db, data)
	fmt.Printf("\nImport done, %d keys imported", len(data.Data))
}

func validateData(data Data) error {
	if data.Version != exportFormatVersion {
		return fmt.Errorf("unsupported export format version %d (expected %d)", data.Version, exportFormatVersion)
	}
	for key, record := range data.Data {
		if len(record.Dump) == 0 {
			return fmt.Errorf("key %s has an empty dump", key)
		}
		if record.TTLMillis < 0 {
			return fmt.Errorf("key %s has an invalid ttl: %dms", key, record.TTLMillis)
		}
	}
	return nil
}

func importDataToDb(redisAddr string, db int, data Data) {
	rdb := goRedis.NewClient(&goRedis.Options{
		Addr:     redisAddr,
		Password: "",
		DB:       db,
	})
	defer rdb.Close()

	for key, record := range data.Data {
		exists, err := rdb.Exists(context.Background(), key).Result()
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
		if exists > 0 {
			fmt.Printf("Key %s already exists in db %d.\nAborting...", key, db)
			os.Exit(1)
		}

		fmt.Printf("Importing key: %s to db %d (dump len: %d, ttl: %dms)\n", key, db, len(record.Dump), record.TTLMillis)
		ttl := time.Duration(record.TTLMillis) * time.Millisecond
		if err := rdb.Restore(context.Background(), key, ttl, string(record.Dump)).Err(); err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
	}
}
