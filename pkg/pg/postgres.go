package pg

import (
	"context"
	"example/flow-key-indexer/model"
	"example/flow-key-indexer/utils"

	_ "github.com/golang-migrate/migrate/database/postgres"
	_ "github.com/golang-migrate/migrate/source/file"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gorm.io/gorm/clause"
)

type Store struct {
	conf   DatabaseConfig
	logger zerolog.Logger
	db     *Database
	done   chan struct{} // Semaphore channel to limit concurrency
}

func NewStore(conf DatabaseConfig, logger zerolog.Logger) *Store {
	return &Store{
		conf:   conf,
		logger: logger,
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
	batchSize := len(publicKeys)
	result := s.db.WithContext(ctx).Clauses(clause.OnConflict{
		DoNothing: true,
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
		return "Unknown"
	}
}

func (s *Store) UpdatePublicKeyAccounts(records []model.PublicKeyAccountIndexer) error {
	// Implement the update logic here
	// For example:
	for _, record := range records {
		err := s.db.Exec("UPDATE publickeyindexer SET sigalgo = ?, hashalgo = ? WHERE account = ? AND keyid = ? AND publickey = ?",
			record.SigAlgo, record.HashAlgo, record.Account, record.KeyId, record.PublicKey).Error
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) GetUniqueAddressesWithoutAlgos(limit int) ([]string, error) {
	var addresses []string
	err := s.db.Table("publickeyindexer").
		Select("DISTINCT account").
		Where("sigalgo IS NULL OR hashalgo IS NULL").
		Limit(limit).
		Pluck("account", &addresses).Error
	return addresses, err
}
