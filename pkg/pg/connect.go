package pg

import (
	"fmt"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

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

	gormDB, err := gorm.Open(dial, &gorm.Config{})

	return gormDB, err
}
