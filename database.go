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
	Account     string `json: "account"`
	KeyId       int    `json: "keyId"`
	Weight      int    `json: "weight"`
	SigningAlgo int    `json: "signingAlgo"`
	HashingAlgo int    `json: "hashingAlgo"`
	IsRevoked   bool   `json: "isRevoked"`
}

type PublicKeyIndexer struct {
	PublicKey string    `json: "publicKey"`
	Accounts  []Account `json: "accounts"`
}

type PublicKeyStatus struct {
	Count          int `json: "publicKeyCount"`
	CurrentBlock   int `json: "currentBlockHeight"`
	UpdatedToBlock int `json: "updatedToBlockHeight`
}

func (d *Database) Init(dbPath string) *Database {
	db, err := badger.Open(badger.DefaultOptions(dbPath))
	if err != nil {
		log.Error().Msg("Badger db could not be opened")
	}
	d.db = db

	//defer db.Close()
	return d
}

func (d *Database) BulkAddPublicKey(pkis []PublicKeyIndexer) error {
	err := d.db.Update(func(txn *badger.Txn) error {
		for _, pki := range pkis {
			data, _ := json.Marshal(pki.Accounts)
			errSet := txn.Set([]byte(pki.PublicKey), []byte(data))
			if errSet != nil {
				log.Error().Err(errSet).Msgf("Bulk data load not saved: %s", pki.PublicKey)
				return errSet
			}
		}
		return nil
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

func (d *Database) updateBlockHeight(height uint64) error {
	return d.db.Update(func(txn *badger.Txn) error {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(height))
		return txn.Set([]byte("height"), []byte(b))
	})
}

func (d *Database) GetUpdatedBlockHeight() (uint64, error) {
	var blockHeight uint64 = 0
	err := d.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("height"))
		if err != nil {
			log.Info().Msgf("block height not stored, will store after load")
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
	stats.UpdatedToBlock = int(value)
	if err != nil {
		stats.UpdatedToBlock = -1
	}
	stats.Count = itemCount
	return stats
}
