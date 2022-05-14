package main

import (
	"encoding/binary"
	"encoding/json"

	"github.com/dgraph-io/badger"
	"github.com/rs/zerolog/log"
)

type Database struct {
	db *badger.DB
}

type Account struct {
	Account     string `json:"account"`
	BlockHeight uint64 `json:"blockHeight"`
	KeyId       int    `json:"keyId"`
	Weight      int    `json:"weight"`
	SigningAlgo int    `json:"signingAlgo"`
	HashingAlgo int    `json:"hashingAlgo"`
	IsRevoked   bool   `json:"isRevoked"`
}

type PublicKeyIndexer struct {
	PublicKey string    `json:"publicKey"`
	Accounts  []Account `json:"accounts"`
}

type PublicKeyStatus struct {
	Count          int  `json:"publicKeyCount"`
	CurrentBlock   int  `json:"currentBlockHeight"`
	UpdatedToBlock int  `json:"updatedToBlockHeight"`
	PendingToBlock int  `json:"pendingLoadBlockHeight"`
	IsBulkLoading  bool `json:"isBulkLoading"`
}

const UpdatedToBlock = "updatedToBlock"
const LoadingFromBlock = "loadingFromBlock"

func NewDatabase(dbPath string) *Database {
	d := Database{}
	db, err := badger.Open(badger.DefaultOptions(dbPath))
	if err != nil {
		log.Error().Msg("Badger db could not be opened")
	}
	d.db = db

	//defer db.Close()
	return &d
}

func (d *Database) UpdatePublicKeys(pkis []PublicKeyIndexer) {
	for _, pki := range pkis {
		err := d.UpsertPublicKey(pki.PublicKey, pki.Accounts)
		if err != nil {
			log.Warn().Err(err).Msg("save error, retrying public key")
			errRetry := d.UpsertPublicKey(pki.PublicKey, pki.Accounts)
			if errRetry != nil {
				log.Error().Err(err).Msgf("could not save %v", pki.PublicKey)
			}
		}
	}
}

func (d *Database) UpsertPublicKey(publicKey string, accounts []Account) error {
	err := d.db.Update(func(txn *badger.Txn) error {
		if _, err := txn.Get([]byte(publicKey)); err == badger.ErrKeyNotFound {
			data, _ := json.Marshal(accounts)
			txn.Set([]byte(publicKey), []byte(data))
			return nil
		} else {
			// merge in new accounts
			keyInfo, errGet := txn.Get([]byte(publicKey))
			if errGet != nil {
				log.Error().Err(errGet).Msg("Could not get public key data")
				return errGet
			}
			return keyInfo.Value(func(val []byte) error {
				var existingAccounts []Account
				var newAccounts []Account
				json.Unmarshal(val, &existingAccounts)
				newAccounts = existingAccounts

				// make sure to have unique array
				for _, a := range accounts {
					found := false
					for index, x := range existingAccounts {
						if x.Account == a.Account && x.KeyId == a.KeyId {
							found = true
							if a.BlockHeight > x.BlockHeight {
								newAccounts[index] = a
							}
							break
						}
					}
					if !found {
						newAccounts = append(newAccounts, a)
					}
				}
				data, _ := json.Marshal(newAccounts)
				if errSet := txn.Set([]byte(publicKey), []byte(data)); errSet == badger.ErrConflict {
					txn = d.db.NewTransaction(true)
					_ = txn.Set([]byte(publicKey), []byte(data))
				}
				return nil
			})
		}
	})
	return err
}

func (d *Database) GetPublicKey(publicKey string, hashAlgo int, signAlgo int, exZero bool, exRevoked bool) (PublicKeyIndexer, error) {
	var keyInfo PublicKeyIndexer
	err := d.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(publicKey))
		if err != nil {
			log.Error().Err(err).Msgf("could not get public key %s", publicKey)
			return err
		}
		errValue := item.Value(func(val []byte) error {
			var accts []Account
			var accounts []Account
			json.Unmarshal(val, &accounts)

			for _, a := range accounts {
				if exZero && a.Weight == 0 {
					continue
				}
				if exRevoked && a.IsRevoked {
					continue
				}
				if hashAlgo != -1 && a.HashingAlgo == hashAlgo {
					accts = append(accts, a)
					continue
				}
				if signAlgo != -1 && a.SigningAlgo == signAlgo {
					accts = append(accts, a)
					continue
				}
				if hashAlgo == -1 && signAlgo == -1 {
					accts = append(accts, a)
				}
			}

			keyInfo = PublicKeyIndexer{PublicKey: publicKey, Accounts: accts}
			return nil
		})
		return errValue
	})
	return keyInfo, err
}

func (d *Database) updateBlockHeight(value uint64) error {
	return updateBlockInfo(d.db, value, UpdatedToBlock)
}

func (d *Database) updateLoadingBlockHeight(value uint64) error {
	return updateBlockInfo(d.db, value, LoadingFromBlock)
}

func updateBlockInfo(db *badger.DB, height uint64, name string) error {
	return db.Update(func(txn *badger.Txn) error {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(height))
		return txn.Set([]byte(name), []byte(b))
	})
}

func (d *Database) GetUpdatedBlockHeight() (uint64, error) {
	return GetBlockInfo(d.db, UpdatedToBlock)
}

func (d *Database) GetLoadingBlockHeight() (uint64, error) {
	return GetBlockInfo(d.db, LoadingFromBlock)
}

func GetBlockInfo(db *badger.DB, name string) (uint64, error) {
	var blockHeight uint64 = 0
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(name))
		if err != nil {
			return err
		}
		errValue := item.Value(func(val []byte) error {
			blockHeight = uint64(binary.LittleEndian.Uint64(val))
			return nil
		})
		return errValue
	})
	return blockHeight, err
}

func (d *Database) ClearAllData() error {
	return d.db.DropAll()
}

func (d *Database) ReadValues() error {
	return d.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				log.Info().Msgf("key=%s, value=%s\n", k, v)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (d *Database) Stats() PublicKeyStatus {
	var stats PublicKeyStatus
	itemCount := 0
	d.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			itemCount = itemCount + 1
		}
		return nil
	})
	value, err := d.GetUpdatedBlockHeight()
	blk, errLoading := d.GetLoadingBlockHeight()
	stats.UpdatedToBlock = int(value)
	stats.PendingToBlock = int(blk)
	if err != nil {
		stats.UpdatedToBlock = -1
		stats.IsBulkLoading = true
	}
	if errLoading != nil {
		stats.PendingToBlock = -1
	}
	stats.Count = itemCount
	return stats
}
