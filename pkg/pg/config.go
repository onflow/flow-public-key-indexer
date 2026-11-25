package pg

import (
	"crypto/tls"
	"time"
)

type DatabaseConfig struct {
	User     string
	Password string
	Name     string
	Host     string
	Port     int
	// SSL toggles `sslmode=require` vs `sslmode=disable` in the DSN. Default true in the
	// app's Params struct so hosted Postgres (RDS, Cloud SQL via TCP) works; set to false
	// for local dev against a plaintext container.
	SSL bool
}

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
	ConnectionString string
	RetrySleepTime   time.Duration
	RetryNumTimes    uint16
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
