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

type PublicKeyIndexer struct {
	PublicKey string   `json:"publicKey"`
	Accounts  []string `json:"accounts"`
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
const TotalPublicKeyCount = "totalPublicKeyCount"

func NewDatabase(dbPath string, silence bool, purgeOnStart bool) *Database {
	d := Database{}
	c := badger.DefaultOptions(dbPath)
	c.SyncWrites = true
	c.Dir, c.ValueDir = dbPath, dbPath

	if silence {
		c.Logger = nil // ignore logs from badgerdb
	}
	db, err := badger.Open(c)

	if err != nil {
		log.Error().Err(err).Msg("Badger db could not be opened")
	}

	defer db.Close()

	d.db = db
	if purgeOnStart {
		d.ClearAllData()
	}
	return &d
}

func (d *Database) CleanUp() error {
	log.Debug().Msg("Cleaning up logs to save space")
	return d.db.RunValueLogGC(0.5)
}

func (d *Database) UpdatePublicKeys(pkis []PublicKeyIndexer) {
	maxRetries := 5
	for _, pki := range pkis {
		retry := 0
		for {
			err := UpsertPublicKeyInfo(d.db, pki.PublicKey, pki.Accounts)
			if err == nil {
				break
			}
			if retry > maxRetries {
				log.Error().Err(err).Msgf("exhausted merge save retries, %v", pki.PublicKey)
				break
			}
			log.Warn().Err(err).Msgf("retrying saving public key, %v", pki.PublicKey)
			retry = retry + 1
		}
	}
}

func UpsertPublicKeyInfo(db *badger.DB, publicKey string, accounts []string) error {
	var keyInfo PublicKeyIndexer
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(publicKey))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var oAccounts []string
			json.Unmarshal(val, &oAccounts)
			keyInfo = PublicKeyIndexer{PublicKey: publicKey, Accounts: oAccounts}
			return nil
		})
	})
	var newKeyInfo PublicKeyIndexer
	if err == badger.ErrKeyNotFound {
		newKeyInfo = PublicKeyIndexer{
			PublicKey: publicKey,
			Accounts:  accounts,
		}
	} else {
		newKeyInfo = MergePublicKeyData(keyInfo, accounts)
	}

	return SavePublicKey(db, newKeyInfo)
}

func (d *Database) RemovePublicKeyInfo(publicKey string, account string) error {
	var keyInfo PublicKeyIndexer
	err := d.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(publicKey))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var oAccounts []string
			json.Unmarshal(val, &oAccounts)
			keyInfo = PublicKeyIndexer{PublicKey: publicKey, Accounts: oAccounts}
			return nil
		})
	})
	var newKeyInfo PublicKeyIndexer
	if err == badger.ErrKeyNotFound {
		return nil
	}
	var accounts []string
	for _, acct := range keyInfo.Accounts {
		if acct != account {
			accounts = append(accounts, acct)
		}
	}
	newKeyInfo = PublicKeyIndexer{
		PublicKey: publicKey,
		Accounts:  accounts,
	}

	return SavePublicKey(d.db, newKeyInfo)
}

func MergePublicKeyData(pki PublicKeyIndexer, accounts []string) PublicKeyIndexer {
	newAccounts := pki.Accounts
	for _, a := range accounts {
		if !contains(pki.Accounts, a) {
			newAccounts = append(newAccounts, a)
		}
	}
	pki.Accounts = newAccounts
	return pki
}

func SavePublicKey(db *badger.DB, pki PublicKeyIndexer) error {
	return db.Update(func(txn *badger.Txn) error {
		data, _ := json.Marshal(pki.Accounts)
		return txn.Set([]byte(pki.PublicKey), []byte(data))
	})
}

func (d *Database) GetPublicKey(publicKey string) (PublicKeyIndexer, error) {
	var keyInfo PublicKeyIndexer
	err := d.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(publicKey))
		if err != nil {
			log.Error().Err(err).Msgf("could not get public key %s", publicKey)
			return err
		}
		errValue := item.Value(func(val []byte) error {
			var accounts []string
			json.Unmarshal(val, &accounts)
			keyInfo = PublicKeyIndexer{PublicKey: publicKey, Accounts: accounts}
			return nil
		})
		return errValue
	})
	return keyInfo, err
}

func (d *Database) updateUpdatedBlockHeight(value uint64) error {
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

func (d *Database) GetTotalKeyCount() (uint64, error) {
	return GetBlockInfo(d.db, TotalPublicKeyCount)
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
	log.Info().Msg("Clear all data on start")
	return d.db.DropAll()
}

func (d *Database) ReadKeyValues(limit int) []string {
	var keys []string
	d.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		count := 0
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			count = count + 1
			keys = append(keys, string(k))
			if count > limit {
				break
			}
		}
		return nil
	})
	return keys
}

func (d *Database) UpdateTotalPublicKeyCount() {
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
	updateBlockInfo(d.db, uint64(itemCount), TotalPublicKeyCount)
}

func (d *Database) Stats() PublicKeyStatus {
	var stats PublicKeyStatus
	itemCount, errCnt := d.GetTotalKeyCount()
	value, err := d.GetUpdatedBlockHeight()
	blk, errLoading := d.GetLoadingBlockHeight()
	stats.UpdatedToBlock = int(value)
	stats.PendingToBlock = int(blk)
	if err != nil || value == 0 {
		stats.UpdatedToBlock = -1
		stats.IsBulkLoading = true
	}
	if errLoading != nil {
		stats.PendingToBlock = -1
	}
	stats.Count = int(itemCount)
	if errCnt != nil {
		stats.Count = -1
	}

	return stats
}

func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}
	return false
}
