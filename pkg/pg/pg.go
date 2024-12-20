package pg

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
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
	log.Info().Msg("Initializing database, creating tables and indexes if they do not exist")
	createIndex := `CREATE INDEX IF NOT EXISTS public_key_btree_idx ON publickeyindexer USING btree(publicKey)`
	createAccountIndex := `CREATE INDEX IF NOT EXISTS idx_publickeyindexer_account ON publickeyindexer (account);`
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
	createAddressProcessingTable := `CREATE TABLE IF NOT EXISTS addressprocessing (
		account VARCHAR PRIMARY KEY,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`
	insertStatsTable := `INSERT INTO publickeyindexer_stats select 0,0,0 from publickeyindexer_stats having count(*) < 1;`
	deleteIndex := `DROP INDEX IF EXISTS public_key_btree_idx`
	deleteAccountIndex := `DROP INDEX IF EXISTS idx_publickeyindexer_account`
	deleteTable := `DROP TABLE IF EXISTS publickeyindexer`
	deleteTableStats := `DROP TABLE IF EXISTS publickeyindexer_stats`
	deleteAddressProcessingTable := `DROP TABLE IF EXISTS addressprocessing`

	_, cancelfunc := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelfunc()

	if purgeOnStart {
		d.DB.Exec(deleteIndex)
		d.DB.Exec(deleteAccountIndex)
		d.DB.Exec(deleteTable)
		d.DB.Exec(deleteTableStats)
		d.DB.Exec(deleteAddressProcessingTable)
	}
	d.DB.Exec(createTable)

	d.DB.Exec(createAddressProcessingTable)

	d.DB.Exec(createIndex)

	d.DB.Exec(createAccountIndex)

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

// MigrateDatabase adds new columns to the publickeyindexer table if they do not exist
func (d *Database) MigrateDatabase() error {
	log.Info().Msg("Migrating database: adding sigAlgo, hashAlgo, and isRevoked columns")

	// Check if sigAlgo column exists
	var sigAlgoExists bool
	checkSigAlgoQuery := `SELECT EXISTS (
		SELECT 1 
		FROM information_schema.columns 
		WHERE table_name = 'publickeyindexer' 
		AND column_name = 'sigalgo'
	);`
	if err := d.DB.Raw(checkSigAlgoQuery).Scan(&sigAlgoExists).Error; err != nil {
		return fmt.Errorf("failed to check sigalgo column existence: %w", err)
	}

	log.Info().Msgf("sigalgo Column Exists: %v", sigAlgoExists)
	// Check if hashAlgo column exists
	var hashAlgoExists bool
	checkHashAlgoQuery := `SELECT EXISTS (
		SELECT 1 
		FROM information_schema.columns 
		WHERE table_name = 'publickeyindexer' 
		AND column_name = 'hashalgo'
	);`
	if err := d.DB.Raw(checkHashAlgoQuery).Scan(&hashAlgoExists).Error; err != nil {
		return fmt.Errorf("failed to check hashAlgo column existence: %w", err)
	}

	log.Info().Msgf("hashalgo Column Exists: %v", hashAlgoExists)
	// Check if isRevoked column exists
	var isRevokedExists bool
	checkIsRevokedQuery := `SELECT EXISTS (
		SELECT 1 
		FROM information_schema.columns 
		WHERE table_name = 'publickeyindexer' 
		AND column_name = 'isrevoked'
	);`
	if err := d.DB.Raw(checkIsRevokedQuery).Scan(&isRevokedExists).Error; err != nil {
		return fmt.Errorf("failed to check isRevoked column existence: %w", err)
	}

	log.Info().Msgf("isRevoked Column Exists: %v", isRevokedExists)

	// Add sigAlgo column if it doesn't exist
	if !sigAlgoExists {
		addSigAlgoColumn := `ALTER TABLE publickeyindexer ADD COLUMN IF NOT EXISTS sigalgo int;`
		if err := d.DB.Exec(addSigAlgoColumn).Error; err != nil {
			return fmt.Errorf("failed to add sigalgo column: %w", err)
		}
		log.Info().Msg("sigAlgo column added successfully")
	}

	// Add hashAlgo column if it doesn't exist
	if !hashAlgoExists {
		addHashAlgoColumn := `ALTER TABLE publickeyindexer ADD COLUMN IF NOT EXISTS hashalgo int;`
		if err := d.DB.Exec(addHashAlgoColumn).Error; err != nil {
			return fmt.Errorf("failed to add hashalgo column: %w", err)
		}
		log.Info().Msg("hashAlgo column added successfully")
	}

	// Add isRevoked column if it doesn't exist
	if !isRevokedExists {
		addIsRevokedColumn := `ALTER TABLE publickeyindexer ADD COLUMN IF NOT EXISTS isrevoked BOOLEAN DEFAULT FALSE;`
		if err := d.DB.Exec(addIsRevokedColumn).Error; err != nil {
			return fmt.Errorf("failed to add isRevoked column: %w", err)
		}
		log.Info().Msg("isRevoked column added successfully")

		// Update existing rows to set isrevoked to false
		updateExistingRows := `UPDATE publickeyindexer SET isrevoked = FALSE WHERE isrevoked IS NULL;`
		if err := d.DB.Exec(updateExistingRows).Error; err != nil {
			return fmt.Errorf("failed to update existing rows with isRevoked value: %w", err)
		}
		log.Info().Msg("Existing rows updated with isRevoked set to false")
	}

	log.Info().Msg("Database migration completed successfully")
	return nil
}
