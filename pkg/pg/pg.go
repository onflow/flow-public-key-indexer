package pg

import (
	"context"
	"database/sql"
	"log"
	"net/url"
	"strconv"
	"time"

	"github.com/uptrace/bun"
)

// Database is a connection to a Postgres database.
type Database struct {
	*bun.DB
	connectionString string
}

// NewDatabase creates a new database connection.
func NewDatabase(conf Config) (*Database, error) {

	db, err := connectPG(conf)
	if err != nil {
		return nil, err
	}

	provider := &Database{
		DB:               db,
		connectionString: conf.ConnectionString,
	}

	return provider, nil
}

// Ping pings the database to ensure that we can connect to it.
func (d *Database) Ping(ctx context.Context) (err error) {
	err = d.Ping(ctx)

	return err
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

	if _, err := d.DB.Query(query); err != nil {
		return err
	}

	// run a transaction that truncates all our tables
	return d.DB.RunInTx(context.Background(), &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
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

	return tx.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		return convertError(next(ctx))
	})
}

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
		mode = "?sslmode=disable"
	}

	return str + "@" +
		host + ":" +
		strconv.Itoa(port) + "/" +
		url.PathEscape(db) + mode
}

type logger struct {
	log *log.Logger
}

func (l *logger) Printf(ctx context.Context, format string, v ...interface{}) {
	l.log.Printf(format, v...)
}
