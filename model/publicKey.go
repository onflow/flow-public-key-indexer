package model

type AccountKey struct {
	Account string `json:"address"`
	KeyId   int    `json:"keyId"`
	Weight  int    `json:"weight"`
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
}

func (PublicKeyAccountIndexer) TableName() string {
	return "publickeyindexer"
}

type PublicKeyBlockHeight struct {
	UpdatedBlockheight uint64 `gorm:"column:updatedBlockheight"`
	LoadedBlockheight  uint64 `gorm:"column:loadedBlockheight"`
}

func (PublicKeyBlockHeight) TableName() string {
	return "publickeyindexer_stats"
}

type PublicKeyStatus struct {
	Count         int `json:"publicKeyCount"`
	CurrentBlock  int `json:"currentBlockHeight"`
	LoadedToBlock int `json:"LoadToBlockHeight"`
}
