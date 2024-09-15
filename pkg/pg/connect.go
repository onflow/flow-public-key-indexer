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

func getDSN(conf DatabaseConfig) string {

	dsn := fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%d sslmode=disable",
		conf.Host,
		conf.User,
		conf.Password,
		conf.Name,
		conf.Port,
	)
	return dsn
}

// connectPG will attempt to connect to a Postgres database.
func connectPG(conf DatabaseConfig) (*gorm.DB, error) {
	cfg := postgres.Config{
		DSN: getDSN(conf),
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
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Set up connection pooling
	sqlDB, err := gormDB.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get sql.DB from Gorm DB: %w", err)
	}

	// Configure connection pool settings
	sqlDB.SetMaxIdleConns(20)                  // Adjust based on expected idle time and load
	sqlDB.SetMaxOpenConns(100)                 // Adjust based on database server capacity and peak load
	sqlDB.SetConnMaxLifetime(time.Hour)        // Recycle connections every 1 hour
	sqlDB.SetConnMaxIdleTime(15 * time.Minute) // Close idle connections after 15 minutes

	return gormDB, nil
}
