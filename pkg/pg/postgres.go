package pg

import (
	"context"
	"example/flow-key-indexer/model"
	"strings"

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
	isBulkLoading := uint64(status.UpdatedToBlock) == uint64(0)

	return model.PublicKeyStatus{
		Count:          status.Count,
		UpdatedToBlock: status.UpdatedToBlock,
		PendingToBlock: status.PendingToBlock,
		IsBulkLoading:  isBulkLoading,
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

func (s Store) UpdateUpdatedBlockHeight(blockNumber uint64) {
	sqlStatement := "UPDATE publickeyindexer_stats SET updatedBlockheight = ?"
	err := s.db.Exec(sqlStatement, blockNumber).Error
	if err != nil {
		s.logger.Error().Err(err).Msgf("could not update updated block height %v", blockNumber)
	}
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
		PendingBlockheight int `gorm:"column:pendingblockheight"`
	}

	var result Result

	err := s.db.Raw(query).Scan(&result).Error
	if err != nil {
		s.logger.Error().Err(err).Msgf("get status %v", result.UniquePublicKeys)
	}
	status := model.PublicKeyStatus{
		Count:          result.UniquePublicKeys,
		UpdatedToBlock: result.UpdatedBlockheight,
		PendingToBlock: result.PendingBlockheight,
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

	cnt, _ := s.GetCount()
	sqlStatement := `UPDATE publickeyindexer_stats SET uniquePublicKeys = ?`
	err := s.db.Exec(sqlStatement, cnt).Error
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
	}

	return cnt, nil
}

func (s Store) UpdatePendingBlockHeight(blockNumber uint64) {
	sqlStatement := `UPDATE publickeyindexer_stats SET pendingBlockheight = ?`

	err := s.db.Exec(sqlStatement, blockNumber).Error
	if err != nil {
		s.logger.Error().Err(err).Msgf("could not update loading block height %v", blockNumber)
	}
}

func (s Store) GetPendingBlockHeight() (uint64, error) {
	query := "SELECT pendingBlockheight FROM publickeyindexer_stats;"
	var blockNumber uint64

	err := s.db.Raw(query).Scan(&blockNumber).Error
	if err != nil {
		s.logger.Error().Err(err).Msgf("get loading block height %v", blockNumber)
	}

	return blockNumber, nil
}

func (s Store) RemoveAccountForReloading(account string) {
	s.logger.Debug().Msgf("remove pk and acct %v", account)
	sqlStatement := `DELETE FROM publickeyindexer WHERE account = $2;`
	err := s.db.Raw(sqlStatement, account).Error
	if err != nil {
		s.logger.Warn().Msgf("Could not remove record %v", account)
	}
}

func (s Store) GetAccountsByPublicKey(publicKey string) (model.PublicKeyIndexer, error) {
	var publickeys []model.PublicKeyAccountIndexer
	// add 0x if does not exist
	if !strings.HasPrefix(publicKey, "0x") {
		publicKey = "0x" + publicKey
	}
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
