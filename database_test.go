package main

import (
	"testing"

	"github.com/rs/zerolog/log"
)

func TestAddAnotherAccount(t *testing.T) {
	pkiAcct := []Account{{
		Account:     "0x5487b9669ebabd39",
		BlockHeight: 1,
		KeyId:       0,
		Weight:      400,
		SigningAlgo: 1,
		HashingAlgo: 3,
		IsRevoked:   false,
	}}
	pki := PublicKeyIndexer{
		PublicKey: "ecb8c19039edeafb7ae262dafc18c060aab029bef9be37b712cdf585894a270ed1365db6c114f8e47d75b6604d12734edd749c7029eb9af1186e1cfc2799bd07",
		Accounts:  pkiAcct,
	}

	pki2Acct := []Account{{
		Account:     "0x5c02410a188d3d1a",
		BlockHeight: 2,
		KeyId:       1,
		Weight:      500,
		SigningAlgo: 2,
		HashingAlgo: 1,
		IsRevoked:   false,
	}}
	pki2 := PublicKeyIndexer{
		PublicKey: "ecb8c19039edeafb7ae262dafc18c060aab029bef9be37b712cdf585894a270ed1365db6c114f8e47d75b6604d12734edd749c7029eb9af1186e1cfc2799bd07",
		Accounts:  pki2Acct,
	}

	pki3Acct := []Account{{
		Account:     "0x5c02410a188d3d1a",
		BlockHeight: 3,
		KeyId:       1,
		Weight:      500,
		SigningAlgo: 2,
		HashingAlgo: 1,
		IsRevoked:   true,
	}}
	pki3 := PublicKeyIndexer{
		PublicKey: "ecb8c19039edeafb7ae262dafc18c060aab029bef9be37b712cdf585894a270ed1365db6c114f8e47d75b6604d12734edd749c7029eb9af1186e1cfc2799bd07",
		Accounts:  pki3Acct,
	}

	pki4Acct := []Account{{
		Account:     "0x5c02410a188d3d1a",
		BlockHeight: 0,
		KeyId:       1,
		Weight:      500,
		SigningAlgo: 2,
		HashingAlgo: 1,
		IsRevoked:   true,
	}}
	pki4 := PublicKeyIndexer{
		PublicKey: "ecb8c19039edeafb7ae262dafc18c060aab029bef9be37b712cdf585894a270ed1365db6c114f8e47d75b6604d12734edd749c7029eb9af1186e1cfc2799bd07",
		Accounts:  pki4Acct,
	}

	// test storing key and retreiving it
	db := NewDatabase("./tests", false)

	pkis := []PublicKeyIndexer{pki}
	pkis2 := []PublicKeyIndexer{pki2}

	db.UpdatePublicKeys(pkis)
	log.Info().Msg("test first insert")
	testPublicKey, err := db.GetPublicKey(pki.PublicKey, -1, -1, false, false)
	checkPublicKey(t, err, pki, testPublicKey)

	// add another account to public key
	log.Info().Msg("test second insert")
	db.UpdatePublicKeys(pkis2)
	testPublicKey2, err2 := db.GetPublicKey(pki.PublicKey, -1, -1, false, false)

	aggPki1 := pki
	aggPki1.Accounts = append(pkiAcct, pki2Acct...)
	checkPublicKey(t, err2, aggPki1, testPublicKey2)

	pkis3 := []PublicKeyIndexer{pki3}
	log.Info().Msg("test third insert")
	db.UpdatePublicKeys(pkis3)
	testPublicKey3, err2 := db.GetPublicKey(pki.PublicKey, -1, -1, false, false)
	aggPki2 := pki
	aggPki2.Accounts = append(pkiAcct, pki3Acct...)
	checkPublicKey(t, err2, aggPki2, testPublicKey3)

	pkis4 := []PublicKeyIndexer{pki4}
	log.Info().Msg("test old data not insert")
	db.UpdatePublicKeys(pkis4)
	testPublicKey4, err2 := db.GetPublicKey(pki.PublicKey, -1, -1, false, false)
	checkPublicKey(t, err2, aggPki2, testPublicKey4)
}

func checkAccounts(t *testing.T, oAccounts, tAccounts []Account) {

	if len(oAccounts) != len(tAccounts) {
		t.Errorf("account len not the same o: %d t: %d", len(oAccounts), len(tAccounts))
	}
	for idx := range oAccounts {
		checkAccount(t, oAccounts[idx], tAccounts[idx])
	}
}

func checkAccount(t *testing.T, oAccount Account, tAccount Account) {
	if tAccount.Account != oAccount.Account {
		t.Errorf("account names did not match %v", oAccount.Account)
	}
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

func checkPublicKey(t *testing.T, err error, oPublicKey, tPublicKey PublicKeyIndexer) {
	if err != nil {
		t.Errorf("error: could not retrieve public key %v", tPublicKey.PublicKey)
	}
	if oPublicKey.PublicKey != tPublicKey.PublicKey {
		t.Error("error: public key values do not match")
	}
	checkAccounts(t, oPublicKey.Accounts, tPublicKey.Accounts)

}
