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
	tableName struct{} `pg:"publickeyindexer"`
	PublicKey string   `json:"publicKey" pg:"publickey"`
	Account   string   `json:"account" pg:"account"`
	KeyId     int      `json:"keyId" pg:"keyid,use_zero"`
	Weight    int      `json:"weight" pg:"weight,use_zero"`
}

type PublicKeyBlockHeight struct {
	tableName          struct{} `pg:"publickeyindexer_stats"`
	UpdatedBlockheight uint64   `pg:"updatedBlockheight"`
	PendingBlockheight uint64   `pg:"pendingBlockheight"`
}

type PublicKeyStatus struct {
	Count          int    `json:"publicKeyCount"`
	CurrentBlock   int    `json:"currentBlockHeight"`
	UpdatedToBlock uint64 `json:"updatedToBlockHeight"`
	PendingToBlock uint64 `json:"pendingLoadBlockHeight"`
	IsBulkLoading  bool   `json:"isBulkLoading"`
}
