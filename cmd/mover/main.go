package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/url"
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
	redis := flag.String("redis", "", "Redis URL (for example redis://:password@localhost:6379/0,1,2)")

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

type redisTarget struct {
	options           goRedis.Options
	databases         []int
	databaseSpecified bool
	displayURL        string
}

func parseRedisTarget(value string) (redisTarget, error) {
	parsedURL, err := url.Parse(value)
	if err != nil {
		return redisTarget{}, err
	}
	if parsedURL.Scheme == "" {
		return redisTarget{}, fmt.Errorf("Redis URL must include a scheme, for example redis://localhost:6379/0")
	}
	switch parsedURL.Scheme {
	case "redis", "rediss", "unix":
	default:
		return redisTarget{}, fmt.Errorf("unsupported Redis URL scheme %q", parsedURL.Scheme)
	}
	urlParts := strings.SplitN(value, "://", 2)
	if len(urlParts) != 2 {
		return redisTarget{}, fmt.Errorf("invalid Redis URL; expected %s://", parsedURL.Scheme)
	}
	if parsedURL.Scheme != "unix" {
		authority := urlParts[1]
		if separator := strings.IndexAny(authority, "/?#"); separator >= 0 {
			authority = authority[:separator]
		}
		if strings.Count(authority, "@") > 1 || strings.Contains(parsedURL.Host, ",") {
			return redisTarget{}, fmt.Errorf("invalid Redis URL; specify databases in the path, for example /0,1,2")
		}
	}

	optionsURL := *parsedURL
	databaseSpecified := parsedURL.Query().Has("db")
	var databases []int

	if parsedURL.Scheme != "unix" {
		databasePath := strings.Trim(parsedURL.Path, "/")
		if databasePath != "" {
			databaseSpecified = true
			databases, err = parseDatabaseList(databasePath)
			if err != nil {
				return redisTarget{}, err
			}
			if len(databases) > 1 && parsedURL.Query().Has("db") {
				return redisTarget{}, fmt.Errorf("Redis databases cannot be specified in both the URL path and query")
			}

			// go-redis only accepts one database in the URL path. Parse the
			// connection using the first database; each client gets its actual
			// database through optionsForDatabase.
			optionsURL.Path = "/" + strconv.Itoa(databases[0])
			optionsURL.RawPath = ""
		}
	}

	options, err := goRedis.ParseURL(optionsURL.String())
	if err != nil {
		return redisTarget{}, err
	}
	if len(databases) == 0 || parsedURL.Query().Has("db") {
		databases = []int{options.DB}
	}

	return redisTarget{
		options:           *options,
		databases:         databases,
		databaseSpecified: databaseSpecified,
		displayURL:        parsedURL.Redacted(),
	}, nil
}

func parseDatabaseList(value string) ([]int, error) {
	databases := make([]int, 0, strings.Count(value, ",")+1)
	for _, database := range strings.Split(value, ",") {
		databaseNumber, err := strconv.Atoi(database)
		if err != nil {
			return nil, fmt.Errorf("invalid Redis database %q: %w", database, err)
		}
		databases = append(databases, databaseNumber)
	}
	return databases, nil
}

func (target redisTarget) optionsForDatabase(database int) *goRedis.Options {
	options := target.options
	options.DB = database
	return &options
}

func promptConfirm() bool {
	fmt.Printf("Continue? [y/N] ")
	input := bufio.NewScanner(os.Stdin)
	input.Scan()
	fmt.Printf("\n")
	return strings.ToLower(input.Text()) == "y"
}

func exportFn(redisURL string, file string, pattern string) {
	target, err := parseRedisTarget(redisURL)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	fmt.Printf("Exporting data from redis (%s) to data-file (%s)\n", target.displayURL, file)
	if !promptConfirm() {
		fmt.Printf("\nAborting...")
		os.Exit(0)
	}

	if _, err := os.Stat(file); err == nil {
		fmt.Printf("\n%s already exists. Aborting...", file)
		os.Exit(1)
	}

	dbs := target.databases

	dbData := map[int]Data{}
	for _, dbNum := range dbs {
		rdb := goRedis.NewClient(target.optionsForDatabase(dbNum))

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

func importFn(redisURL string, file string) {
	target, err := parseRedisTarget(redisURL)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	fmt.Printf("Importing data from data-file (%s) to redis (%s)\n", file, target.displayURL)

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
			importDataToDb(target, dbNum, data)
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
	if target.databaseSpecified {
		db = target.databases[0]
	}

	importDataToDb(target, db, data)
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

func importDataToDb(target redisTarget, db int, data Data) {
	rdb := goRedis.NewClient(target.optionsForDatabase(db))
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
