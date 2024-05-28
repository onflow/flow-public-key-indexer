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
	done   chan bool
}

func NewStore(conf DatabaseConfig, logger zerolog.Logger) *Store {
	return &Store{
		conf:   conf,
		logger: logger,
		done:   make(chan bool, 1),
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

	// update the distinct count
	s.UpdateDistinctCount()
	return nil
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

func (s Store) RemovePublicKeyInfo(publicKey string, account string) {
	s.logger.Debug().Msgf("remove pk and acct %v %v", publicKey, account)
	sqlStatement := `DELETE FROM publickeyindexer WHERE publickey = $1 and account = $2;`
	err := s.db.Raw(sqlStatement, publicKey, account).Error
	if err != nil {
		s.logger.Warn().Msgf("Could not remove record %v %v", account, publicKey)
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
