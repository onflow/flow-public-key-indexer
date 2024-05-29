package pg

import (
	"context"
	"example/flow-key-indexer/model"

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
	return err
}

func (s Store) Stats() model.PublicKeyStatus {
	status, _ := s.GetPublicKeyStats()

	return model.PublicKeyStatus{
		Count:         status.Count,
		LoadedToBlock: status.LoadedToBlock,
	}
}

func (s Store) InsertPublicKeyAccounts(pkis []model.PublicKeyAccountIndexer) error {
	ctx := context.Background()
	log.Debug().Msgf("Inserting %v public key accounts", len(pkis))
	err := s.db.RunInTransaction(ctx, func(txCtx context.Context) error {
		for _, publicKeyAccount := range pkis {
			err := s.db.Clauses(clause.OnConflict{DoNothing: true}).Create(&publicKeyAccount).Error
			if err != nil {
				log.Debug().Err(err).Msg("Error inserting public key account record")
				// Return the error if it's not an integrity violation
				return err
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	// update the distinct count in a non-blocking way
	go s.UpdateDistinctCount()

	return nil
}

func (s Store) AddressesNotInDatabase(addresses []string) ([]string, error) {
	var existingAddresses []string
	err := s.db.Model(&model.PublicKeyAccountIndexer{}).Where("account IN ?", addresses).Pluck("account", &existingAddresses).Error
	if err != nil {
		log.Debug().Err(err).Msg("Error checking if accounts exist")
		return nil, err
	}
	s.logger.Debug().Msgf("existing addresses %v", len(existingAddresses))
	addressMap := make(map[string]bool)
	for _, addr := range existingAddresses {
		addressMap[addr] = true
	}

	var nonExistingAddresses []string
	for _, addr := range addresses {
		if !addressMap[addr] {
			nonExistingAddresses = append(nonExistingAddresses, addr)
		}
	}

	s.logger.Debug().Msgf("returning addresses %v", len(nonExistingAddresses))
	return nonExistingAddresses, nil
}

func (s Store) GetUpdatedBlockHeight() (uint64, error) {
	query := "SELECT updatedBlockheight FROM publickeyindexer_stats;"
	var blockNumber uint64

	err := s.db.Raw(
		query,
	).Scan(&blockNumber).Error

	if err != nil {
		s.logger.Error().Err(err).Msgf("get loading block height %v", blockNumber)
	}

	return blockNumber, nil
}

func (s Store) GetPublicKeyStats() (model.PublicKeyStatus, error) {
	query := "SELECT uniquePublicKeys, updatedBlockheight, pendingBlockheight FROM publickeyindexer_stats;"
	type Result struct {
		UniquePublicKeys   int `gorm:"column:uniquepublickeys"`
		UpdatedBlockheight int `gorm:"column:updatedblockheight"`
		LoadedBlockheight  int `gorm:"column:loadedblockheight"`
	}

	var result Result

	err := s.db.Raw(query).Scan(&result).Error
	if err != nil {
		s.logger.Error().Err(err).Msgf("get status %v", result.UniquePublicKeys)
	}
	status := model.PublicKeyStatus{
		Count:         result.UniquePublicKeys,
		LoadedToBlock: result.LoadedBlockheight,
	}
	return status, nil
}
func (s Store) UpdateDistinctCount() {
	s.logger.Debug().Msg("UpdateDistinctCount called")
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
	s.logger.Debug().Msgf("remove acct %v", account)
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
		acct := model.AccountKey{
			Account: pk.Account,
			KeyId:   pk.KeyId,
			Weight:  pk.Weight,
		}
		accts = append(accts, acct)
	}
	publicKeyAccounts := model.PublicKeyIndexer{
		PublicKey: publicKey,
		Accounts:  accts,
	}
	return publicKeyAccounts, err
}
