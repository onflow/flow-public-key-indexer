package pg

import (
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// CustomLogger wraps zerolog and implements the Printf method required by GORM's logger interface.
type CustomLogger struct {
	Zerologger zerolog.Logger
}

func (cl CustomLogger) Printf(format string, v ...interface{}) {
	cl.Zerologger.Info().Msgf(format, v...)
}

// connectPG will attempt to connect to a Postgres database.
func connectPG(conf DatabaseConfig) (*gorm.DB, error) {
	cfg := postgres.Config{
		DSN: fmt.Sprintf(
			"host=%s user=%s password=%s dbname=%s port=%d sslmode=disable",
			conf.Host,
			conf.User,
			conf.Password,
			conf.Name,
			conf.Port,
		),
	}

	dial := postgres.New(cfg)

	// Create a CustomLogger instance
	customLogger := CustomLogger{
		Zerologger: log.Logger,
	}

	// Set custom logger for GORM
	newLogger := logger.New(
		customLogger, // Use custom zerolog for logging
		logger.Config{
			SlowThreshold:             time.Second,   // Customize the slow query threshold
			LogLevel:                  logger.Silent, // Adjust the log level to suppress slow SQL logs
			IgnoreRecordNotFoundError: true,
			Colorful:                  false,
		},
	)

	gormDB, err := gorm.Open(dial, &gorm.Config{
		Logger: newLogger,
	})

	return gormDB, err
}
