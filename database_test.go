package main

import (
	"testing"

	"github.com/rs/zerolog/log"
)

func TestAddAnotherAccount(t *testing.T) {
	pkiAcct := []string{"0x5487b9669ebabd39"}
	pki := PublicKeyIndexer{
		PublicKey: "ecb8c19039edeafb7ae262dafc18c060aab029bef9be37b712cdf585894a270ed1365db6c114f8e47d75b6604d12734edd749c7029eb9af1186e1cfc2799bd07",
		Accounts:  pkiAcct,
	}

	pki2Acct := []string{"0x5c02410a188d3d1a"}
	pki2 := PublicKeyIndexer{
		PublicKey: "ecb8c19039edeafb7ae262dafc18c060aab029bef9be37b712cdf585894a270ed1365db6c114f8e47d75b6604d12734edd749c7029eb9af1186e1cfc2799bd07",
		Accounts:  pki2Acct,
	}

	pki3Acct := []string{"0x5c02410a188d3d1a"}
	pki3 := PublicKeyIndexer{
		PublicKey: "ecb8c19039edeafb7ae262dafc18c060aab029bef9be37b712cdf585894a270ed1365db6c114f8e47d75b6604d12734edd749c7029eb9af1186e1cfc2799bd07",
		Accounts:  pki3Acct,
	}

	pki4Acct := []string{"0x5c02410a188d3d1a"}
	pki4 := PublicKeyIndexer{
		PublicKey: "ecb8c19039edeafb7ae262dafc18c060aab029bef9be37b712cdf585894a270ed1365db6c114f8e47d75b6604d12734edd749c7029eb9af1186e1cfc2799bd07",
		Accounts:  pki4Acct,
	}

	// test storing key and retreiving it
	db := NewDatabase("./tests", false, false)

	pkis := []PublicKeyIndexer{pki}
	pkis2 := []PublicKeyIndexer{pki2}

	db.UpdatePublicKeys(pkis)
	log.Info().Msg("test first insert")
	testPublicKey, err := db.GetPublicKey(pki.PublicKey)
	checkPublicKey(t, err, pki, testPublicKey)

	// add another account to public key
	log.Info().Msg("test second insert")
	db.UpdatePublicKeys(pkis2)
	testPublicKey2, err2 := db.GetPublicKey(pki.PublicKey)

	aggPki1 := pki
	aggPki1.Accounts = append(pkiAcct, pki2Acct...)
	checkPublicKey(t, err2, aggPki1, testPublicKey2)

	pkis3 := []PublicKeyIndexer{pki3}
	log.Info().Msg("test third insert")
	db.UpdatePublicKeys(pkis3)
	testPublicKey3, err2 := db.GetPublicKey(pki.PublicKey)
	aggPki2 := pki
	aggPki2.Accounts = append(pkiAcct, pki3Acct...)
	checkPublicKey(t, err2, aggPki2, testPublicKey3)

	pkis4 := []PublicKeyIndexer{pki4}
	log.Info().Msg("test old data not insert")
	db.UpdatePublicKeys(pkis4)
	testPublicKey4, err2 := db.GetPublicKey(pki.PublicKey)
	checkPublicKey(t, err2, aggPki2, testPublicKey4)
}

func TestAddSameAccountDiffKey(t *testing.T) {
	pkiAcct := []string{"0x1e3c78c6d580273b", "0x1e3c78c6d580273b", "0x1e3c78c6d580273b"}
	pki := PublicKeyIndexer{
		PublicKey: "df37f72795256b1a4bd797469455b9ec7cf7e5e5e1201cdd90df8daf647b951f28babba11bba2197a9f74dac66f02d3c0028befe51b80fb5ac4002fa970afc3b",
		Accounts:  pkiAcct,
	}

	// test storing key and retreiving it
	db := NewDatabase("./tests2", false, false)

	pkis := []PublicKeyIndexer{pki}

	db.UpdatePublicKeys(pkis)
	log.Info().Msg("test first insert")
	testPublicKey, err := db.GetPublicKey(pki.PublicKey)
	if err != nil {
		t.Errorf("some error %v", err)
	}

	if len(testPublicKey.Accounts) != len(pkiAcct) {
		t.Errorf("account keys does not match %d should be %d", len(testPublicKey.Accounts), len(pkiAcct))
	}
}

func checkAccounts(t *testing.T, oAccounts, tAccounts []string) {
	if len(oAccounts) != len(tAccounts) {
		t.Errorf("account len not the same o: %d t: %d", len(oAccounts), len(tAccounts))
	}
	for idx := range oAccounts {
		checkAccount(t, oAccounts[idx], tAccounts[idx])
	}
}

func checkAccount(t *testing.T, oAccount string, tAccount string) {
	if tAccount != oAccount {
		t.Errorf("account names did not match %v", oAccount)
	}
}

func checkPublicKey(t *testing.T, err error, oPublicKey, tPublicKey PublicKeyIndexer) {
	if err != nil {
		t.Errorf("error: could not retrieve public key %v", tPublicKey.PublicKey)
	}
	if oPublicKey.PublicKey != tPublicKey.PublicKey {
		t.Error("error: public key values do not match")
	}
	if len(oPublicKey.Accounts) != len(tPublicKey.Accounts) {
		t.Errorf("account len not the same o: %d t: %d", len(oPublicKey.Accounts), len(tPublicKey.Accounts))
	}
	checkAccounts(t, oPublicKey.Accounts, tPublicKey.Accounts)

}
