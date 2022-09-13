package pg

import (
	"errors"
	"time"

	"github.com/go-pg/pg/v10"
)

// connectPG will attempt to connect to a Postgres database.
func connectPG(conf *ConnectPGOptions, ops *pg.Options) (*pg.DB, error) {

	// use provided TLS config is present
	if conf.TLSConfig != nil {
		ops.TLSConfig = conf.TLSConfig
	}

	var err = errors.New("temp")
	var numtries uint16
	var pgdb *pg.DB

	for err != nil && numtries < conf.RetryNumTimes {

		pgdb = pg.Connect(ops)

		i := 0
		_, err = pgdb.QueryOne(pg.Scan(&i), "SELECT 1")

		if err != nil {
			if conf.ConnErrorLogger != nil {
				conf.ConnErrorLogger(int(numtries), conf.RetrySleepTime, ops.Addr, ops.Database, ops.User, ops.TLSConfig != nil, err)
			}
			// not sure if we need to close if we had an error
			pgdb.Close()
			// sleep
			time.Sleep(conf.RetrySleepTime)
		}
		numtries++
	}

	return pgdb, err
}
