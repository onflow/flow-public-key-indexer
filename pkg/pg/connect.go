package pg

import (
	"database/sql"
	"errors"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
)

// connectPG will attempt to connect to a Postgres database.
func connectPG(conf Config) (*bun.DB, error) {

	var err = errors.New("temp")
	var numtries uint16
	var bunDB *bun.DB
	var pgconn *pgdriver.Connector

	// use provided TLS config is present
	if conf.TLSConfig != nil {
		pgconn = pgdriver.NewConnector(pgdriver.WithDSN(conf.ConnectionString), pgdriver.WithTLSConfig(conf.ConnectPGOptions.TLSConfig), pgdriver.WithApplicationName(conf.PGApplicationName))
	} else {
		pgconn = pgdriver.NewConnector(pgdriver.WithDSN(conf.ConnectionString), pgdriver.WithApplicationName(conf.PGApplicationName))
	}
	sqldb := sql.OpenDB(pgconn)

	for err != nil && numtries < conf.RetryNumTimes {

		bunDB = bun.NewDB(sqldb, pgdialect.New())

		err = bunDB.Ping()

		if err != nil {
			if conf.ConnErrorLogger != nil {
				conf.ConnErrorLogger(int(numtries), conf.RetrySleepTime, ops.Addr, ops.Database, ops.User, ops.TLSConfig != nil, err)
			}
			// not sure if we need to close if we had an error
			bunDB.Close()
			// sleep
			time.Sleep(conf.RetrySleepTime)
		}
		numtries++
	}

	return bunDB, err
}
