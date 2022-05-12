package main

import (
	"testing"

	"github.com/rs/zerolog/log"
)

func TestAddAnotherAccount(t *testing.T) {
	// test storing key and retreiving it
	db := NewDatabase("./tests")
	pki := PublicKeyIndexer{}
	pki.PublicKey = "ecb8c19039edeafb7ae262dafc18c060aab029bef9be37b712cdf585894a270ed1365db6c114f8e47d75b6604d12734edd749c7029eb9af1186e1cfc2799bd07"
	pki.Accounts = []Account{{
		Account:     "0x5487b9669ebabd39",
		KeyId:       0,
		Weight:      400,
		SigningAlgo: 1,
		HashingAlgo: 3,
		IsRevoked:   false,
	}}
	pkis := []PublicKeyIndexer{pki}

	pki2 := PublicKeyIndexer{}
	pki2.PublicKey = "ecb8c19039edeafb7ae262dafc18c060aab029bef9be37b712cdf585894a270ed1365db6c114f8e47d75b6604d12734edd749c7029eb9af1186e1cfc2799bd07"
	pki2.Accounts = []Account{{
		Account:     "0x5c02410a188d3d1a",
		KeyId:       1,
		Weight:      500,
		SigningAlgo: 2,
		HashingAlgo: 1,
		IsRevoked:   false,
	}}
	pkis2 := []PublicKeyIndexer{pki2}
	db.UpdatePublicKeys(pkis)
	log.Info().Msg("finished first insert")
	publicKey, err := db.GetPublicKey(pki.PublicKey, -1, -1, false, false)

	if err != nil {
		t.Errorf("error: could not retrieve public key %v", pki.PublicKey)
	}
	if len(publicKey.Accounts) != 1 {
		t.Errorf("error: public key has incorrect accounts %d", len(pki.Accounts))
	}
	if publicKey.PublicKey != pki.PublicKey {
		t.Error("error: public key value not match")
	}
	if publicKey.Accounts[0].Account != pki.Accounts[0].Account {
		t.Error("error: public key account value not match")
	}
	// add another account to public key
	log.Info().Msg("start second insert")
	db.UpdatePublicKeys(pkis2)
	publicKey2, err2 := db.GetPublicKey(pki.PublicKey, -1, -1, false, false)
	if err2 != nil {
		t.Errorf("error: could not retrieve public key %v", pki2.PublicKey)
	}
	if len(publicKey2.Accounts) != 2 {
		t.Errorf("error: public key has incorrect accounts %d", len(publicKey2.Accounts))
	}
	count := 0
	for _, acct := range publicKey2.Accounts {
		if acct.Account == pki.Accounts[0].Account {
			a := pki.Accounts[0]
			checkAccount(t, a, acct)
			count = count + 1
		}
		if acct.Account == pki2.Accounts[0].Account {
			a := pki2.Accounts[0]
			checkAccount(t, a, acct)
			count = count + 1
		}
	}
	if count != 2 {
		t.Errorf("all accounts not in collection")
	}
}

func checkAccount(t *testing.T, oAccount Account, tAccount Account) {
	if tAccount.KeyId != oAccount.KeyId {
		t.Errorf("key id does not match %v", oAccount.Account)
	}
	if tAccount.Weight != oAccount.Weight {
		t.Errorf("weight does not match %v", oAccount.Account)
	}
	if tAccount.HashingAlgo != oAccount.HashingAlgo {
		t.Errorf("HashingAlgo does not match %v", oAccount.Account)
	}
	if tAccount.SigningAlgo != oAccount.SigningAlgo {
		t.Errorf("SigningAlgo does not match %v", oAccount.Account)
	}
}
