
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

        let keys: {Int: AnyStruct} = {}

        var keyIndex: Int = 0
        var didNotFindKey: Bool = false

        while(!didNotFindKey) {
          let currKey = account.keys.get(keyIndex: keyIndex)
            keyIndex = keyIndex + 1
          if let _currKey = currKey {
              var included = true
              if ignoreZeroWeight && _currKey.weight == UFix64(0) {
                included = false
              }
              if ignoreRevoked && _currKey.isRevoked {
                  included = false
              }
              if (included) {
                keys[_currKey.keyIndex] = _AccountKey(acctKey: _currKey)
              }              
          } else {
              didNotFindKey = true
          }
          if keyCap > 0 && keys.length >= keyCap {
              didNotFindKey = true
          }

        }
        allKeys[address] = keys
    }

    return allKeys
}