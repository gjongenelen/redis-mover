package redisurl

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	redis "github.com/go-redis/redis/v8"
)

type Target struct {
	options           redis.Options
	databases         []int
	databaseSpecified bool
	redactedURL       string
}

func Parse(value string) (Target, error) {
	parsedURL, err := url.Parse(value)
	if err != nil {
		return Target{}, err
	}
	if parsedURL.Scheme == "" {
		return Target{}, fmt.Errorf("Redis URL must include a scheme, for example redis://localhost:6379/0")
	}
	switch parsedURL.Scheme {
	case "redis", "rediss", "unix":
	default:
		return Target{}, fmt.Errorf("unsupported Redis URL scheme %q", parsedURL.Scheme)
	}

	urlParts := strings.SplitN(value, "://", 2)
	if len(urlParts) != 2 {
		return Target{}, fmt.Errorf("invalid Redis URL; expected %s://", parsedURL.Scheme)
	}
	if parsedURL.Scheme != "unix" {
		authority := urlParts[1]
		if separator := strings.IndexAny(authority, "/?#"); separator >= 0 {
			authority = authority[:separator]
		}
		if strings.Count(authority, "@") > 1 || strings.Contains(parsedURL.Host, ",") {
			return Target{}, fmt.Errorf("invalid Redis URL; specify databases in the path, for example /0,1,2")
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
				return Target{}, err
			}
			if len(databases) > 1 && parsedURL.Query().Has("db") {
				return Target{}, fmt.Errorf("Redis databases cannot be specified in both the URL path and query")
			}

			optionsURL.Path = "/" + strconv.Itoa(databases[0])
			optionsURL.RawPath = ""
		}
	}

	options, err := redis.ParseURL(optionsURL.String())
	if err != nil {
		return Target{}, err
	}
	if len(databases) == 0 || parsedURL.Query().Has("db") {
		databases = []int{options.DB}
	}

	return Target{
		options:           *options,
		databases:         databases,
		databaseSpecified: databaseSpecified,
		redactedURL:       parsedURL.Redacted(),
	}, nil
}

func (target Target) Databases() []int {
	return append([]int(nil), target.databases...)
}

func (target Target) HasExplicitDatabases() bool {
	return target.databaseSpecified
}

func (target Target) RedactedURL() string {
	return target.redactedURL
}

func (target Target) Options(database int) *redis.Options {
	options := target.options
	options.DB = database
	return &options
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
