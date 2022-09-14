package pg

import (
	"context"
	"errors"
	"example/flow-key-indexer/model"
	"fmt"

	"github.com/go-pg/pg/v10"
	_ "github.com/golang-migrate/migrate/database/postgres"
	_ "github.com/golang-migrate/migrate/source/file"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Store struct {
	conf   Config
	logger zerolog.Logger
	db     *Database
	done   chan bool
}

func NewStore(conf Config, logger zerolog.Logger) *Store {
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

func (s *Store) Stop() {
	_ = s.db.Close()
	s.done <- true
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
	err := s.db.RunInTransaction(ctx, func(ctx context.Context) error {

		for _, publicKeyAccount := range pkis {
			_, err := s.db.Model(&publicKeyAccount).Insert()

			if errors.Is(err, ErrIntegrityViolation) {
				// this can occur when accounts get reloaded, expected
				// leaving this here for documentation
			} else {
				if err != nil {
					log.Debug().Err(err).Msg("inserting public key account record")
				}

			}
		}
		return nil
	})

	if err != nil {
		if errors.Is(err, ErrIntegrityViolation) {
			// ignore if data already stored
			return nil
		}

		return err
	}

	return nil
}

func (s Store) UpdateUpdatedBlockHeight(blockNumber uint64) {
	sqlStatement := fmt.Sprintf(`UPDATE publickeyindexer_stats SET updatedBlockheight = %v`, blockNumber)
	_, err := s.db.Exec(sqlStatement, blockNumber)
	if err != nil {
		s.logger.Error().Err(err).Msgf("could not update updated block height %v", blockNumber)
	}
}

func (s Store) GetUpdatedBlockHeight() (uint64, error) {
	query := "SELECT updatedBlockheight FROM publickeyindexer_stats;"
	var blockNumber uint64

	_, err := s.db.Model((*model.PublicKeyStatus)(nil)).QueryOne(pg.Scan(&blockNumber), query)
	if err != nil {
		s.logger.Error().Err(err).Msgf("get loading block height %v", blockNumber)
	}

	return blockNumber, nil
}

func (s Store) GetPublicKeyStats() (model.PublicKeyStatus, error) {
	query := "SELECT uniquePublicKeys, updatedBlockheight, pendingBlockheight FROM publickeyindexer_stats;"
	var uniquePublicKeys int
	var updatedBlockheight int
	var pendingBlockheight int

	_, err := s.db.Model((*model.PublicKeyStatus)(nil)).QueryOne(pg.Scan(&uniquePublicKeys, &updatedBlockheight, &pendingBlockheight), query)
	if err != nil {
		s.logger.Error().Err(err).Msgf("get status %v", uniquePublicKeys)
	}
	status := model.PublicKeyStatus{
		Count:          uniquePublicKeys,
		UpdatedToBlock: updatedBlockheight,
		PendingToBlock: pendingBlockheight,
	}
	return status, nil
}

func (s Store) UpdateDistinctCount() {
	cnt, _ := s.GetCount()
	sqlStatement := fmt.Sprintf(`UPDATE publickeyindexer_stats SET uniquePublicKeys = %v`, cnt)
	_, err := s.db.Exec(sqlStatement)
	if err != nil {
		s.logger.Error().Err(err).Msgf("could not update unique public keys %v", cnt)
	}
}

func (s Store) GetCount() (int, error) {
	query := "SELECT COUNT(distinct publickey) as cnt FROM publickeyindexer;"
	var cnt int

	_, err := s.db.Model((*model.PublicKeyStatus)(nil)).QueryOne(pg.Scan(&cnt), query)
	if err != nil {
		s.logger.Error().Err(err).Msgf("get distinct publickey count %v", cnt)
	}

	return cnt, nil
}

func (s Store) UpdateLoadingBlockHeight(blockNumber uint64) {
	sqlStatement := fmt.Sprintf(`UPDATE publickeyindexer_stats SET pendingBlockheight = %v`, blockNumber)

	_, err := s.db.Exec(sqlStatement, blockNumber)
	if err != nil {
		s.logger.Error().Err(err).Msgf("could not update loading block height %v", blockNumber)
	}

}

func (s Store) GetLoadingBlockHeight() (uint64, error) {
	query := "SELECT pendingBlockheight FROM publickeyindexer_stats;"
	var blockNumber uint64

	_, err := s.db.Model((*model.PublicKeyStatus)(nil)).QueryOne(pg.Scan(&blockNumber), query)
	if err != nil {
		s.logger.Error().Err(err).Msgf("get loading block height %v", blockNumber)
	}

	return blockNumber, nil
}

func (s Store) RemovePublicKeyInfo(publicKey string, account string) {
	s.logger.Debug().Msgf("remove pk and acct %v %v", publicKey, account)
	sqlStatement := `DELETE FROM publickeyindexer WHERE publickey = $1 and account = $2;`
	_, err := s.db.Exec(sqlStatement, publicKey, account)
	if err != nil {
		s.logger.Warn().Msgf("Could not remove record %v %v", account, publicKey)
	}
}

func (s Store) GetAccountsByPublicKey(publicKey string) (model.PublicKeyIndexer, error) {
	var publickeys []model.PublicKeyAccountIndexer
	err := s.db.Model(&publickeys).Where("publickey = ?", publicKey).Select()

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
