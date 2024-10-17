package model

type AccountKey struct {
	Account   string `json:"address"`
	KeyId     int    `json:"keyId"`
	Weight    int    `json:"weight"`
	SigAlgo   int    `json:"sigAlgo"`
	HashAlgo  int    `json:"hashAlgo"`
	IsRevoked bool   `json:"isRevoked"`
	Signing   string `json:"signing"`
	Hashing   string `json:"hashing"`
}

type PublicKeyIndexer struct {
	PublicKey string       `json:"publicKey"`
	Accounts  []AccountKey `json:"accounts"`
}

type PublicKeyAccountIndexer struct {
	PublicKey string `json:"publicKey" gorm:"column:publickey"`
	Account   string `json:"account" gorm:"column:account"`
	KeyId     int    `json:"keyId" gorm:"column:keyid"`
	Weight    int    `json:"weight" gorm:"column:weight"`
	SigAlgo   int    `json:"sigAlgo" gorm:"column:sigalgo"`
	HashAlgo  int    `json:"hashAlgo" gorm:"column:hashalgo"`
	IsRevoked bool   `json:"isRevoked" gorm:"column:isrevoked"`
}

func (PublicKeyAccountIndexer) TableName() string {
	return "publickeyindexer"
}

type PublicKeyBlockHeight struct {
	UpdatedBlockheight uint64 `gorm:"column:updatedBlockheight"`
	LoadToBlockHeight  uint64 `gorm:"column:pendingBlockheight"`
}

func (PublicKeyBlockHeight) TableName() string {
	return "publickeyindexer_stats"
}

type PublicKeyStatus struct {
	Count         int `json:"publicKeyCount"`
	CurrentBlock  int `json:"currentBlockHeight"`
	LoadedToBlock int `json:"LoadToBlockHeight"`
}
