
pub struct _AccountKey {
    pub var hashAlgorithm: UInt8
    pub var isRevoked: Bool
    pub var weight: UFix64
    pub var publicKey: String
    pub var keyIndex: Int
    pub var signatureAlgorithm: UInt8
    
    init(acctKey: AccountKey) {
        self.hashAlgorithm = acctKey.hashAlgorithm.rawValue
        self.isRevoked = acctKey.isRevoked
        self.weight = acctKey.weight
        self.keyIndex = acctKey.keyIndex
        self.publicKey = String.encodeHex(acctKey.publicKey.publicKey)
        self.signatureAlgorithm = acctKey.publicKey.signatureAlgorithm.rawValue
    }
}

pub fun main(addresses: [Address], keyCap: Int, ignoreZeroWeight: Bool, ignoreRevoked: Bool): {Address: AnyStruct} {
    let allKeys: {Address: AnyStruct} = {}

    for address in addresses {
        let account = getAccount(address)

        let acctKeys: {Int: _AccountKey} = {}

        var keyIndex: Int = 0
        var didNotFindKey: Bool = false

        while(!didNotFindKey) {
            let currKey = account.keys.get(keyIndex: keyIndex)
            if let _currKey = currKey {
                acctKeys[keyIndex] = _AccountKey(acctKey: _currKey)
            } else {
                didNotFindKey = true
            }
            keyIndex = keyIndex + 1
        }
        // filter out keys if needed
        var keys: {Int: AnyStruct} = {}
        if ignoreZeroWeight || ignoreRevoked {
            for key in acctKeys.keys {
                if let value = acctKeys[key] {
                    if ignoreZeroWeight && value.weight == UFix64(0) {
                        continue    
                    }  
                    if ignoreRevoked && value.isRevoked {
                        continue    
                    }
                    keys[key] = value
                }
                keyIndex = keyIndex + 1
                if keyCap > 0 && keys.length >= keyCap {
                    break
                }
            }
        } else {
            keys = acctKeys
        }

        if keys.length > 0 {
            allKeys[address] = keys
        }
    }

    return allKeys
}