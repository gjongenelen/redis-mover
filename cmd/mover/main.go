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
	DumpStart time.Time         `json:"dump_start"`
	DumpEnd   time.Time         `json:"dump_end"`
	Db        int               `json:"db"`
	Data      map[string]string `json:"data"`
}

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
			DumpStart: time.Now(),
			Db:        dbNum,
			Data:      map[string]string{},
		}
		for _, key := range keys {
			value, err := rdb.Get(context.Background(), key).Result()
			if err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			}
			if _, ok := data.Data[key]; ok {
				fmt.Printf("Conflicting key: %s\nAborting...", key)
				os.Exit(1)
			}
			fmt.Printf("Exporting key: %s (len: %d)\n", key, len(value))
			data.Data[key] = value
		}
		data.DumpEnd = time.Now()

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

	jsonFile, err := os.Open(file)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	data := Data{}
	err = json.Unmarshal(byteValue, &data)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	db := 0
	if len(redisParts) > 1 {
		db, _ = strconv.Atoi(redisParts[1])
	}
	rdb := goRedis.NewClient(&goRedis.Options{
		Addr:     redisParts[0],
		Password: "",
		DB:       db,
	})

	for key, value := range data.Data {
		curValue, err := rdb.Get(context.Background(), key).Result()
		if err == nil && curValue != "" {
			fmt.Printf("Key %s already exists.\nAborting...", key)
			os.Exit(1)
		}

		fmt.Printf("Importing key: %s (len: %d)\n", key, len(value))
		_, err = rdb.Set(context.Background(), key, value, 0).Result()
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

	}

	fmt.Printf("\nImport done, %d keys imported", len(data.Data))
}
