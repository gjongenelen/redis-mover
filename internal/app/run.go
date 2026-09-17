package app

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/gjongenelen/redis-mover/internal/archive"
	"github.com/gjongenelen/redis-mover/internal/redisstore"
	"github.com/gjongenelen/redis-mover/internal/redisurl"
	"github.com/gjongenelen/redis-mover/internal/transfer"
)

type dependencies struct {
	newStoreFactory func(redisurl.Target) transfer.StoreFactory
	now             func() time.Time
}

func Run(ctx context.Context, args []string, input io.Reader, output io.Writer) error {
	return run(ctx, args, input, output, dependencies{
		newStoreFactory: func(target redisurl.Target) transfer.StoreFactory {
			return redisstore.NewFactory(target)
		},
		now: time.Now,
	})
}

func run(ctx context.Context, args []string, input io.Reader, output io.Writer, deps dependencies) error {
	flags := flag.NewFlagSet("mover", flag.ContinueOnError)
	flags.SetOutput(output)
	isExport := flags.Bool("export", false, "export Redis to file")
	isImport := flags.Bool("import", false, "import file into Redis")
	dataFile := flags.String("file", "", "path to data file")
	pattern := flags.String("pattern", "", "key prefix to export")
	redisValue := flags.String("redis", "", "Redis URL (for example redis://:password@localhost:6379/0,1,2)")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	if !*isExport && !*isImport {
		return fmt.Errorf("need either export or import flag")
	}
	if *dataFile == "" {
		return fmt.Errorf("need data-file location")
	}
	if *redisValue == "" {
		return fmt.Errorf("need Redis URL")
	}

	target, err := redisurl.Parse(*redisValue)
	if err != nil {
		return err
	}
	reader := bufio.NewReader(input)

	if *isExport {
		continued, err := runExport(ctx, reader, output, deps, target, *dataFile, *pattern)
		if err != nil || !continued {
			return err
		}
	}
	if *isImport {
		_, err := runImport(ctx, reader, output, deps, target, *dataFile)
		return err
	}
	return nil
}

func runExport(ctx context.Context, input *bufio.Reader, output io.Writer, deps dependencies, target redisurl.Target, path, pattern string) (bool, error) {
	fmt.Fprintf(output, "Exporting data from redis (%s) to data-file (%s)\n", target.RedactedURL(), path)
	if !promptConfirm(input, output) {
		fmt.Fprint(output, "\nAborting...")
		return false, nil
	}

	if _, err := os.Stat(path); err == nil {
		return false, fmt.Errorf("%s already exists", path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}

	exporter := transfer.Exporter{
		Stores:   deps.newStoreFactory(target),
		Progress: output,
		Now:      deps.now,
	}
	file, err := exporter.Export(ctx, target.Databases(), pattern)
	if err != nil {
		return false, err
	}

	var encoded bytes.Buffer
	if err := archive.Encode(&encoded, file); err != nil {
		return false, err
	}
	if err := writeNewFile(path, encoded.Bytes()); err != nil {
		return false, err
	}

	if file.Multiple {
		fmt.Fprintf(output, "\nExport done, %d dbs exported", len(file.Databases))
	} else {
		for _, database := range file.Databases {
			fmt.Fprintf(output, "\nExport done, %d keys exported", len(database.Records))
		}
	}
	return true, nil
}

func runImport(ctx context.Context, input *bufio.Reader, output io.Writer, deps dependencies, target redisurl.Target, path string) (bool, error) {
	fmt.Fprintf(output, "Importing data from data-file (%s) to redis (%s)\n", path, target.RedactedURL())
	if !promptConfirm(input, output) {
		fmt.Fprint(output, "\nAborting...")
		return false, nil
	}

	inputFile, err := os.Open(path)
	if err != nil {
		return false, err
	}
	file, decodeErr := archive.Decode(inputFile)
	closeErr := inputFile.Close()
	if decodeErr != nil {
		return false, decodeErr
	}
	if closeErr != nil {
		return false, closeErr
	}

	var databaseOverride *int
	if !file.Multiple && target.HasExplicitDatabases() {
		database := target.Databases()[0]
		databaseOverride = &database
	}

	importer := transfer.Importer{
		Stores:   deps.newStoreFactory(target),
		Progress: output,
	}
	result, err := importer.Import(ctx, file, databaseOverride)
	if err != nil {
		return false, err
	}
	if file.Multiple {
		fmt.Fprintf(output, "\nImport done, %d dbs imported", result.Databases)
	} else {
		fmt.Fprintf(output, "\nImport done, %d keys imported", result.Keys)
	}
	return true, nil
}

func promptConfirm(input *bufio.Reader, output io.Writer) bool {
	fmt.Fprint(output, "Continue? [y/N] ")
	line, _ := input.ReadString('\n')
	fmt.Fprintln(output)
	return strings.EqualFold(strings.TrimSpace(line), "y")
}

func writeNewFile(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(data)
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}
