package pg

import (
	"bytes"
	"context"
	"example/flow-key-indexer/model"
	"example/flow-key-indexer/utils"
	"fmt"
	"io"

	_ "github.com/golang-migrate/migrate/database/postgres"
	_ "github.com/golang-migrate/migrate/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Store struct {
	conf   DatabaseConfig
	logger zerolog.Logger
	db     *Database
	dsn    string
	done   chan struct{} // Semaphore channel to limit concurrencyj
}

func NewStore(conf DatabaseConfig, logger zerolog.Logger) *Store {
	return &Store{
		conf:   conf,
		logger: logger,
		dsn:    getDSN(conf),
		done:   make(chan struct{}, 1), // Initialize semaphore with capacity 1
	}
}

func (s *Store) Start(purgeOnStart bool) error {
	var err error
	s.db, err = NewDatabase(s.conf)
	if err != nil {
		return err
	}

	err = s.db.InitDatabase(purgeOnStart)
	if err != nil {
		return err
	}

	// Call MigrateDatabase after initializing the database
	err = s.db.MigrateDatabase()
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to migrate database")
		return err
	}

	s.logger.Info().Msg("Database migration completed successfully")
	return nil
}

func (s Store) Stats() model.PublicKeyStatus {
	status, _ := s.GetPublicKeyStats()

	return model.PublicKeyStatus{
		Count:         status.Count,
		LoadedToBlock: status.LoadedToBlock,
	}
}

func (s Store) BatchInsertPublicKeyAccounts(ctx context.Context, publicKeys []model.PublicKeyAccountIndexer) (int64, error) {
	if len(publicKeys) == 0 {
		return 0, nil
	}

	batchSize := len(publicKeys)

	// Ensure conflict resolution happens when account, keyid, and publickey all match
	result := s.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "account"},
			{Name: "keyid"},
			{Name: "publickey"},
		}, // Detect conflict based on these three columns
		DoUpdates: clause.AssignmentColumns([]string{"sigalgo", "hashalgo"}), // Update only SigAlgo and HashAlgo on conflict
	}).CreateInBatches(publicKeys, batchSize)

	if result.Error != nil {
		return 0, result.Error
	}

	return result.RowsAffected, nil
}

func (s Store) InsertPublicKeyAccounts(ctx context.Context, publicKeys []model.PublicKeyAccountIndexer) error {
	if len(publicKeys) == 0 {
		return nil
	}

	insertedCount, err := s.BatchInsertPublicKeyAccounts(ctx, publicKeys)

	if insertedCount > 0 {
		log.Info().Msgf("DB Inserted %v of %v public key accounts", insertedCount, len(publicKeys))
	}

	if err != nil {
		log.Error().Err(err).Msg("Transaction failed")
		return err
	}

	return nil
}

func (s Store) GetUniqueAddresses() (<-chan string, error) {
	out := make(chan string)
	query := "SELECT DISTINCT account FROM publickeyindexer;"

	go func() {
		defer close(out)

		rows, err := s.db.Raw(query).Rows()
		if err != nil {
			log.Debug().Err(err).Msg("Error checking if accounts exist")
			return
		}
		defer rows.Close()

		var address string
		for rows.Next() {
			if err := rows.Scan(&address); err != nil {
				log.Debug().Err(err).Msg("Error scanning address")
				return
			}
			out <- address
		}

		if err := rows.Err(); err != nil {
			log.Debug().Err(err).Msg("Error during rows iteration")
		}
	}()

	return out, nil
}

func (s Store) GetPublicKeyStats() (model.PublicKeyStatus, error) {
	query := "SELECT uniquePublicKeys FROM publickeyindexer_stats;"

	var uniquePublicKeys int

	err := s.db.Raw(query).Scan(&uniquePublicKeys).Error
	if err != nil {
		s.logger.Error().Err(err).Msgf("get status %v", uniquePublicKeys)
	}

	// get pending block height, multi field select is having issues
	pending, _ := s.GetLoadedBlockHeight()
	status := model.PublicKeyStatus{
		Count:         uniquePublicKeys,
		LoadedToBlock: int(pending),
	}
	return status, nil
}

func (s Store) UpdateDistinctCount() {
	// Acquire a slot in the semaphore
	s.done <- struct{}{}
	defer func() {
		// Release the slot when finished
		<-s.done
	}()

	cnt, err := s.GetCount()
	if err != nil {
		s.logger.Error().Err(err).Msg("Error getting count")
		return
	}
	s.logger.Debug().Msgf("Updating unique public keys count to %v", cnt)
	sqlStatement := `UPDATE publickeyindexer_stats SET uniquePublicKeys = ?`
	err = s.db.Exec(sqlStatement, cnt).Error
	if err != nil {
		s.logger.Error().Err(err).Msgf("could not update unique public keys %v", cnt)
	}
}

func (s Store) GetCount() (int, error) {
	query := "SELECT COUNT(distinct publickey) as cnt FROM publickeyindexer;"
	var cnt int

	err := s.db.Raw(query).Scan(&cnt).Error
	if err != nil {
		s.logger.Error().Err(err).Msgf("get distinct publickey count %v", cnt)
		return 0, err
	}

	s.logger.Debug().Msgf("Distinct public key count is %v", cnt)
	return cnt, nil
}

func (s Store) UpdateLoadedBlockHeight(blockNumber uint64) {
	log.Debug().Msgf("Updating loaded block height to %v", blockNumber)
	sqlStatement := `UPDATE publickeyindexer_stats SET pendingBlockheight = ?`

	err := s.db.Exec(sqlStatement, blockNumber).Error
	if err != nil {
		s.logger.Error().Err(err).Msgf("could not update loading block height %v", blockNumber)
	}
}

func (s Store) GetLoadedBlockHeight() (uint64, error) {
	query := "SELECT pendingBlockheight FROM publickeyindexer_stats;"
	var blockNumber uint64

	err := s.db.Raw(query).Scan(&blockNumber).Error
	if err != nil {
		s.logger.Error().Err(err).Msgf("get loading block height %v", blockNumber)
	}

	return blockNumber, nil
}

func (s Store) RemoveAccountForReloading(account string) {
	sqlStatement := `DELETE FROM publickeyindexer WHERE account = $2;`
	err := s.db.Raw(sqlStatement, account).Error
	if err != nil {
		s.logger.Warn().Msgf("Could not remove record %v", account)
	}
}

func (s Store) GetAccountsByPublicKey(publicKey string) (model.PublicKeyIndexer, error) {
	var publickeys []model.PublicKeyAccountIndexer
	err := s.db.Where("publickey = ?", publicKey).Find(&publickeys).Error

	if err != nil {
		return model.PublicKeyIndexer{}, err
	}

	accts := []model.AccountKey{}
	// consolidate account data
	for _, pk := range publickeys {
		a := utils.FixAccountLength(pk.Account)
		acct := model.AccountKey{
			Account:  a,
			KeyId:    pk.KeyId,
			Weight:   pk.Weight,
			SigAlgo:  pk.SigAlgo,
			HashAlgo: pk.HashAlgo,
			Signing:  GetSignatureAlgoString(pk.SigAlgo),
			Hashing:  GetHashingAlgoString(pk.HashAlgo),
		}
		accts = append(accts, acct)

	}
	publicKeyAccounts := model.PublicKeyIndexer{
		PublicKey: publicKey,
		Accounts:  accts,
	}
	return publicKeyAccounts, err
}

func GetHashingAlgoString(hashAlgoInt int) string {
	switch hashAlgoInt {
	case 1:
		return "SHA2_256"
	case 2:
		return "SHA2_384"
	case 3:
		return "SHA3_256"
	case 4:
		return "SHA3_384"
	case 5:
		return "KMAC128_BLS_BLS12_381"
	case 6:
		return "KECCAK_256"
	default:
		log.Warn().Msgf("Unknown hashing algorithm: %v", hashAlgoInt)
		return "Unknown"
	}
}
func GetSignatureAlgoString(sigAlgoInt int) string {
	switch sigAlgoInt {
	case 1:
		return "ECDSA_P256"
	case 2:
		return "ECDSA_secp256k1"
	case 3:
		return "BLS_BLS12_381"
	default:
		log.Warn().Msgf("Unknown signature algorithm: %v", sigAlgoInt)
		return "Unknown"
	}
}

func (s *Store) GetUniqueAddressesWithoutAlgos(limit int, ignoreList []string) ([]string, error) {
	var addresses []string
	err := s.db.Transaction(func(tx *gorm.DB) error {
		query := tx.Table("publickeyindexer").
			Select("DISTINCT account").
			Where("(sigalgo IS NULL OR hashalgo IS NULL) AND publickey != ?", "blank")

		// Add condition for ignoring accounts in the ignore list if it's not empty
		if len(ignoreList) > 0 {
			query = query.Where("account NOT IN (?)", ignoreList)
		}

		return query.Order("account ASC"). // Change to DESC for descending order
							Limit(limit).
							Pluck("account", &addresses).Error
	})

	if err != nil {
		s.logger.Error().Err(err).Msg("Error fetching unique addresses without algos")
	}

	return addresses, err
}

// Generate COPY-compatible string for batch inserting/updating data into PostgreSQL
func (s Store) GenerateCopyStringForPublicKeyAccounts(ctx context.Context, publicKeys []model.PublicKeyAccountIndexer) (string, error) {
	if len(publicKeys) == 0 {
		return "", nil
	}

	var buffer bytes.Buffer
	const batchSize = 10000 // You can adjust this based on performance
	fieldDelimiter := "\t"  // Tab delimiter for the COPY command (adjust based on requirements)
	recordDelimiter := "\n"

	for i := 0; i < len(publicKeys); i += batchSize {
		end := i + batchSize
		if end > len(publicKeys) {
			end = len(publicKeys)
		}

		// Iterate through the batch and create the COPY-compatible string
		for _, key := range publicKeys[i:end] {
			// Generate the row, assuming PublicKeyAccountIndexer has these fields
			row := fmt.Sprintf("%s%s%d%s%s%s%d%s%d%s%d%s",
				key.Account, fieldDelimiter,
				key.KeyId, fieldDelimiter,
				key.PublicKey, fieldDelimiter,
				key.Weight, fieldDelimiter,
				key.SigAlgo, fieldDelimiter,
				key.HashAlgo, recordDelimiter)
			// Append the row to the buffer
			buffer.WriteString(row)
		}
	}

	return buffer.String(), nil
}

func (s Store) LoadPublicKeyIndexerFromReader(ctx context.Context, file io.Reader) (int64, error) {

	// Create a pgx pool or connection
	pgxConn, err := pgxpool.New(ctx, s.dsn)
	if err != nil {
		log.Error().Err(err).Msg("Error creating pgx connection")
		return 0, err
	}
	defer pgxConn.Close()

	// Begin a transaction
	tx, err := pgxConn.Begin(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Error beginning transaction")
		return 0, err
	}
	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		} else {
			err = tx.Commit(ctx)
			if err != nil {
				log.Error().Err(err).Msg("Error committing transaction")
			}
		}
	}()

	// Create the temporary table
	createTempTableQuery := `
        CREATE TEMP TABLE temp_publickeyindexer (
            account TEXT NOT NULL,
            keyid INT NOT NULL,
            publickey TEXT NOT NULL,
            sigalgo INT,
            hashalgo INT,
            weight INT
        ) ON COMMIT DROP;
    `
	_, err = tx.Exec(ctx, createTempTableQuery)
	if err != nil {
		log.Error().Err(err).Msg("Error creating temp table")
		return 0, err
	}

	// Perform the COPY FROM STDIN operation
	copyFromQuery := `COPY temp_publickeyindexer (account, keyid, publickey, weight, sigalgo, hashalgo) FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', HEADER false);`
	pgConn := tx.Conn().PgConn()
	rowsCopied, err := pgConn.CopyFrom(ctx, file, copyFromQuery)
	if err != nil {
		log.Error().Err(err).Msg("Error during COPY FROM STDIN")
		return 0, err
	}
	log.Info().Msgf("Batch Bulk Loaded %d rows", rowsCopied.RowsAffected())

	// Insert data from the temp table into the main table
	insertQuery := `
        INSERT INTO publickeyindexer (account, keyid, publickey, weight, sigalgo, hashalgo)
        SELECT account, keyid, publickey, weight, sigalgo, hashalgo FROM temp_publickeyindexer
        ON CONFLICT (account, keyid, publickey)
        DO UPDATE SET sigalgo = EXCLUDED.sigalgo, hashalgo = EXCLUDED.hashalgo;
    `
	cmdTag, err := tx.Exec(ctx, insertQuery)
	if err != nil {
		log.Error().Err(err).Msg("Error executing INSERT INTO publickeyindexer")
		return 0, err
	}

	rowsAffected := cmdTag.RowsAffected()
	log.Debug().Msgf("Rows affected: %d", rowsAffected)

	return rowsAffected, nil
}
