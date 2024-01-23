package pg

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/go-pg/pg/v10"
	"golang.org/x/xerrors"
)

// Database is a connection to a Postgres database.
type Database struct {
	*pg.DB
	ops *pg.Options
}

// NewDatabase creates a new database connection.
func NewDatabase(conf Config) (*Database, error) {
	// useful when debugging long running queries
	conf.ConnectionOps.ApplicationName = conf.PGApplicationName

	db, err := connectPG(&conf.ConnectPGOptions, conf.ConnectionOps)
	if err != nil {
		return nil, err
	}

	// set query logger
	if conf.SetInternalPGLogger {
		pg.SetLogger(&logger{log.New(os.Stdout, conf.PGLoggerPrefix, log.LstdFlags)})
	}

	provider := &Database{
		DB:  db,
		ops: conf.ConnectionOps,
	}

	return provider, nil
}

// Ping pings the database to ensure that we can connect to it.
func (d *Database) Ping(ctx context.Context) (err error) {
	return d.DB.RunInTransaction(ctx, func(tx *pg.Tx) error {
		i := 0
		_, err = tx.QueryOne(pg.Scan(&i), "SELECT 1")
		return err
	})
}

// Close closes the database.
func (d *Database) Close() error {
	return d.DB.Close()
}

// create table and index in database
func (d *Database) InitDatabase(purgeOnStart bool) error {
	createIndex := `CREATE INDEX IF NOT EXISTS public_key_at_brin_idx ON publickeyindexer USING brin(publicKey)`
	createTable := `CREATE TABLE IF NOT EXISTS publickeyindexer(
		publicKey varchar, 
		account varchar, 
		keyId int, 
		weight int,
		PRIMARY KEY(publicKey, account, keyId))`
	createStatsTable := `CREATE TABLE IF NOT EXISTS publickeyindexer_stats(
		pendingBlockheight int,
		updatedBlockheight int,
		uniquePublicKeys int
		)`
	insertStatsTable := `INSERT INTO publickeyindexer_stats select 0,0,0 from publickeyindexer_stats having count(*) < 1;`
	deleteIndex := `DROP INDEX IF EXISTS public_key_at_brin_idx`
	deleteTable := `DROP TABLE IF EXISTS publickeyindexer`
	deleteTableStats := `DROP TABLE IF EXISTS publickeyindexer_stats`

	ctx, cancelfunc := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelfunc()

	if purgeOnStart {
		if _, err := d.DB.ExecContext(ctx, deleteIndex); err != nil {
			return err
		}
		if _, err := d.DB.ExecContext(ctx, deleteTable); err != nil {
			return err
		}
		if _, err := d.DB.ExecContext(ctx, deleteTableStats); err != nil {
			return err
		}
	}
	if _, err := d.DB.ExecContext(ctx, createTable); err != nil {
		return err
	}

	if _, err := d.DB.ExecContext(ctx, createIndex); err != nil {
		return err
	}

	if _, err := d.DB.ExecContext(ctx, createStatsTable); err != nil {
		return err
	}

	if _, err := d.DB.ExecContext(ctx, insertStatsTable); err != nil {
		return err
	}

	return nil
}

// TruncateAll truncates all tables other that schema_migrations.
func (d *Database) TruncateAll() error {
	// query the DB for a list of all our tables
	var tableInfo []struct {
		Table string
	}

	query := `
		SELECT table_name "table"
		FROM information_schema.tables WHERE table_schema='public'
		AND table_type='BASE TABLE' AND table_name!= 'schema_migrations';
	`

	if _, err := d.DB.Query(&tableInfo, query); err != nil {
		return err
	}

	// run a transaction that truncates all our tables
	return d.DB.RunInTransaction(context.Background(), func(tx *pg.Tx) error {
		for _, info := range tableInfo {
			if _, err := tx.Exec("TRUNCATE " + info.Table + " CASCADE;"); err != nil {
				return err
			}
		}
		return nil
	})
}

func (d *Database) RunInTransaction(ctx context.Context, next func(ctx context.Context) error) error {
	tx, err := d.DB.Begin()
	if err != nil {
		return err
	}

	return tx.RunInTransaction(ctx, func(tx *pg.Tx) error {
		return convertError(next(ctx))
	})
}

func ToOps(port int, ssl bool, username, password, db, host string) *pg.Options {
	return &pg.Options{
		Addr:     fmt.Sprintf("%s:%d", host, port),
		User:     username,
		Password: password,
		Database: db,
	}
}

type Options = pg.Options

// ToURL constructs a Postgres querystring with sensible defaults.
func ToURL(port int, ssl bool, username, password, db, host string) string {
	str := "postgres://"

	if len(username) == 0 {
		username = "postgres"
	}
	str += url.PathEscape(username)

	if len(password) > 0 {
		str = str + ":" + url.PathEscape(password)
	}

	if port == 0 {
		port = 5432
	}

	if db == "" {
		db = "postgres"
	}

	if host == "" {
		host = "localhost"
	}

	mode := ""
	if !ssl {
		mode = " sslmode=disable"
	}

	dbURI := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d",
		host, username, password, db, port) + mode

	return dbURI
}

// parseURL is a wrapper around `pg.ParseURL`
// that undoes the logic in https://github.com/go-pg/pg/pull/458; which is
// to ensure that InsecureSkipVerify is false.
func parseURL(sURL string) (*pg.Options, error) {
	options, err := pg.ParseURL(sURL)
	if err != nil {
		return nil, xerrors.Errorf("pg: %w", err)
	}

	if options.TLSConfig != nil {
		// override https://github.com/go-pg/pg/pull/458
		options.TLSConfig.InsecureSkipVerify = false
		// TLSConfig now requires either InsecureSkipVerify = true or ServerName not empty
		options.TLSConfig.ServerName = strings.Split(options.Addr, ":")[0]
	}

	return options, nil
}

type logger struct {
	log *log.Logger
}

func (l *logger) Printf(ctx context.Context, format string, v ...interface{}) {
	l.log.Printf(format, v...)
}
