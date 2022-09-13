
pub fun main(addresses: [Address], keyCap: Int, ignoreZeroWeight: Bool, ignoreRevoked: Bool): {Address: [String]} {
    let allKeys: {Address: [String]} = {}

    for address in addresses {
        let account = getAccount(address)

        let publicKeys: {String: Bool} = {}

        var keyIndex: Int = 0
        var didNotFindKey: Bool = false

        while(!didNotFindKey) {
          let currKey = account.keys.get(keyIndex: keyIndex)
          keyIndex = keyIndex + 1
          if let _currKey = currKey {
            var publicKey = String.encodeHex(_currKey.publicKey.publicKey)
            var included = true
              if ignoreZeroWeight && _currKey.weight == UFix64(0) {
                included = false
              }
              if ignoreRevoked && _currKey.isRevoked {
                  included = false
              }
              if (included) {
                publicKeys[publicKey] = true             
              }
          } else {
              didNotFindKey = true
          }
          if keyCap > 0 && publicKeys.keys.length >= keyCap {
              didNotFindKey = true
          }

        }
        allKeys[address] = publicKeys.keys
    }

    return allKeys
}
