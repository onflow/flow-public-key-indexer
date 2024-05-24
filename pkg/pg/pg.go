package pg

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"strings"
	"time"

	"gorm.io/gorm"
)

// Database is a connection to a Postgres database.
type Database struct {
	*gorm.DB
	connectionConfig DatabaseConfig
}

// NewDatabase creates a new database connection.
func NewDatabase(conf DatabaseConfig) (*Database, error) {

	db, err := connectPG(conf)
	if err != nil {
		return nil, err
	}

	provider := &Database{
		DB:               db,
		connectionConfig: conf,
	}

	return provider, nil
}

// Ping pings the database to ensure that we can connect to it.
func (d *Database) Ping(ctx context.Context) (err error) {
	err = d.Ping(ctx)

	return err
}

// create table and index in database
func (d *Database) InitDatabase(purgeOnStart bool) error {
	createIndex := `CREATE INDEX IF NOT EXISTS public_key_btree_idx ON publickeyindexer USING btree(publicKey)`
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
	deleteIndex := `DROP INDEX IF EXISTS public_key_btree_idx`
	deleteTable := `DROP TABLE IF EXISTS publickeyindexer`
	deleteTableStats := `DROP TABLE IF EXISTS publickeyindexer_stats`

	_, cancelfunc := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelfunc()

	if purgeOnStart {
		d.DB.Exec(deleteIndex)
		d.DB.Exec(deleteTable)
		d.DB.Exec(deleteTableStats)
	}
	d.DB.Exec(createTable)

	d.DB.Exec(createIndex)

	d.DB.Exec(createStatsTable)

	d.DB.Exec(insertStatsTable)

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

	if err := d.DB.Raw(query).Error; err != nil {
		return err
	}

	// run a transaction that truncates all our tables
	return d.DB.Transaction(func(tx *gorm.DB) error {
		for _, info := range tableInfo {
			if err := tx.Exec("TRUNCATE " + info.Table + " CASCADE;").Error; err != nil {
				return err
			}
		}
		return nil
	})
}
func (d *Database) RunInTransaction(ctx context.Context, next func(ctx context.Context) error) error {
	gormTx := d.DB.Begin()
	if gormTx.Error != nil {
		return gormTx.Error
	}

	defer func() {
		// Recover from panic and handle transaction rollback or commit
		if r := recover(); r != nil {
			gormTx.Rollback()
			panic(r) // Re-panic after rollback
		} else if err := gormTx.Commit().Error; err != nil {
			gormTx.Rollback()
		}
	}()

	// Execute the provided function within the transaction
	return convertError(next(context.WithValue(ctx, "gorm:db", gormTx)))
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

	if strings.HasPrefix(host, "/") {
		return fmt.Sprintf("unix://%s:%s@%s%s", username, password, db, host)
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
