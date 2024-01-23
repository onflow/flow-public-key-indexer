package pg

import (
	"crypto/tls"
	"time"

	"github.com/go-pg/pg/v10"
)

// Config is the service configuration
type Config struct {
	ConnectPGOptions
	SetInternalPGLogger bool
	PGApplicationName   string
	PGLoggerPrefix      string
	PGPoolSize          int
}

// ConnectPGOptions attempts to connect to a pg instance;
// retries `RetryNumTimes`
type ConnectPGOptions struct {
	ConnectionOps  *pg.Options
	RetrySleepTime time.Duration
	RetryNumTimes  uint16
	// TLSConfig overrides any TLS config parsed from the connection string if not nil
	TLSConfig       *tls.Config
	ConnErrorLogger LogConErrorFunc
}

type LogConErrorFunc func(
	numTries int,
	duration time.Duration,
	host string,
	db string,
	user string,
	ssl bool,
	err error,
)
