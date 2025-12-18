# Flow Public Key Indexer
A observer service for indexing flow Accounts public keys and REST service that serves up public key data.


## Run Parameters
`KEYIDX_LOGLEVEL` default: "info"
<br>Log Level: Takes string log level value for zerolog, "debug", "info", ...</br>

`KEYIDX_PORT` default: "8080"
<br>Port: The port the REST service is hosted on</br>

`KEYIDX_FLOWURL1` default: "access.mainnet.nodes.onflow.org:9000"
<br>Flow Url: Access node endpoint blockchain data is pulled from, needs to match up with Chain Id. Up to 4 access nodes can be provided, only one is required. The access nodes are cycled through each request to get public key data.</br>

`KEYIDX_FLOWURL2` default: none
<br>Flow Url: Access node endpoint</br>

`KEYIDX_FLOWURL3` default: none
<br>Flow Url: Access node endpoint</br>

`KEYIDX_FLOWURL4` default: none
<br>Flow Url: Access node endpoint</br>

`KEYIDX_CHAINID` default: "flow-mainnet"
<br>Chain Id: target blockchain, valid values are "flow-testnet" and "flow-mainnet". Needs to match up with Flow Url</br>

`KEYIDX_MAXACCTKEYS` default: 1000
<br>Max Acct Keys: maximum number of keys to index per account, accounts that have more public keys that exceed max do not get indexed. Accounts are logged out that exceed the max</br>

`KEYIDX_BATCHSIZE` default: 50000
<br>Batch Size: max number of accounts in a batch sent to cadence script that access node executes. Cadence script can exceed execution if accounts have a lot of keys</br>

`KEYIDX_IGNOREZEROWEIGHT` default: true
<br>Ignore Zero Weight: tells the cadence script to ignore public keys with zero weight. These keys will not be indexed</br>

`KEYIDX_IGNOREREVOKED` default: false
<br>Ignore Revoked: tells the cadence script to ignore public keys that have been revoked. These keys will not be indexed</br>

`KEYIDX_WAITNUMBLOCKS` default: 200
<br>Wait Num Blocks: number of blocks to wait before running an incremental data load</br>

`KEYIDX_BLOCKPOLINTERVALSEC` default: 180
<br>Block Pol Interval Sec: number of seconds to wait before checking current block height to determine to run an incremental data load</br>

`KEYIDX_SYNCDATAPOLINTERVALMIN` default: 1
<br>Sync Data Polling Interval: number of minutes to wait between sync data operations</br>

`KEYIDX_SYNCDATASTARTINDEX` default: 30000000
<br>Sync Data Start Index: starting block height for sync operations</br>

`KEYIDX_MAXBLOCKRANGE` default: 600
<br>Max Block Range: number of blocks that will trigger a bulk load if services falls behind</br>

`KEYIDX_FETCHSLOWDOWNMS` default: 500
<br>Fetch Slowdown: milliseconds to wait between fetch operations</br>

`KEYIDX_PURGEONSTART` default: false
<br>Purge on Start: When changing the data structure or want to clear the database and start from scratch change this variable to true</br>

`KEYIDX_ENABLESYNCDATA` default: true
<br>Enable Sync Data: Run this service as a sync service. It's possible to run this service only as rest service</br>

`KEYIDX_ENABLEINCREMENTAL` default: true
<br>Enable Incremental: Enable incremental updates of the database</br>

## PostgreSQL configurations
`KEYIDX_POSTGRESQLHOST` default: "localhost"
`KEYIDX_POSTGRESQLPORT` default: 5432
`KEYIDX_POSTGRESQLUSERNAME` default: "postgres"
`KEYIDX_POSTGRESQLPASSWORD` not required, no default
`KEYIDX_POSTGRESQLDATABASE` default: "keyindexer"
`KEYIDX_POSTGRESQLSSL` default: true
`KEYIDX_POSTGRESQLLOGQUERIES` default: false
`KEYIDX_POSTGRESQLSETLOGGER` default: false
`KEYIDX_POSTGRESQLRETRYNUMTIMES` default: 30
`KEYIDX_POSTGRESQLRETRYSLEEPTIME` default: "1s"
`KEYIDX_POSTGRESQLPOOLSIZE` default: 1
`KEYIDX_POSTGRESLOGGERPREFIX` default: "keyindexer"
`KEYIDX_POSTGRESPROMETHEUSSUBSYSTEM` default: "keyindexer"

## Re-indexing Accounts

The service supports re-indexing of specific accounts by adding them to the `addressprocessing` table. This feature is useful when you need to:
- Update account information that might have changed
- Re-process accounts that may have had errors during initial indexing
- Force a refresh of specific account data

### How to Re-index Accounts

1. Add addresses to the `addressprocessing` table:
```sql
INSERT INTO addressprocessing (account) 
VALUES ('0x1234...'), ('0x5678...')
ON CONFLICT (account) DO NOTHING;
```

2. The service will automatically:
   - Pick up these addresses during the next bulk processing cycle
   - Re-fetch their public key information
   - Update the database with any changes
   - Remove the addresses from the `addressprocessing` table once processed

### Processing Behavior
- Addresses in the `addressprocessing` table are processed in batches (defined by `KEYIDX_BATCHSIZE`)
- Duplicate addresses are automatically ignored (ON CONFLICT DO NOTHING)
- Processing occurs during the bulk load cycle (controlled by `KEYIDX_SYNCDATAPOLINTERVALMIN`)
- After successful processing, addresses are automatically removed from the `addressprocessing` table

### Related Configuration Parameters
- `KEYIDX_BATCHSIZE` default: 50000
  <br>Controls how many addresses are processed in each batch</br>
- `KEYIDX_SYNCDATAPOLINTERVALMIN` default: 1
  <br>Determines how frequently the service checks for new addresses to process</br>

## How to Run
Since this is a golang service there are many ways to run it. Below are two ways to run this service
### Command line
```go run .```
### Docker
Configuration: Run docker in default 10 gig memory size. Reducing the running memory size reduces performance, the lowest is 6 gig, bulk sync and public key query responses are reasonable compared to running with more memory.<br>
Create a docker container<br>
```docker build -t key-indexer .``` <br>

``` need to configure to use postgresql ```
This service stores public key data and needs persistent storage in postgresql<br>
Run the docker and map the rest service port<br>
Notice that environmental variables can be passed in. See variables above<br>
```docker run -p 8888:8080 --env KEYIDX_POSTGRESQLHOST=localhost``` <br>
To see the logs of the container, get the container id <br>
```docker container ls``` <br>
View the containers logs <br>
```docker logs <container id>``` <br>
## REST service
`Endpoints`
* `GET /key/{public key}`
<p>note: public key is in base64 format
serves up json object</p>

```json
{
    "publicKey": string,  // public key string in base64
    "accounts": [
        {
            "address": string,    // Flow account address
            "keyId": int,        // Key index in the account
            "weight": int,       // Key weight for signing
            "sigAlgo": int,      // Signing algorithm identifier
            "hashAlgo": int,     // Hashing algorithm identifier
            "signing": string,   // Human-readable signing algorithm name
            "hashing": string    // Human-readable hashing algorithm name
        }
    ]
}
```

<p>sigAlgo - signing: 2 - ECDSA_P256, 3 - ECDSA_secp256k1</p>
<p>hashAlgo - hashing: 1 - SHA2_256, 3 - SHA3_256</p>

* `GET /status`
<p>note: this endpoint gives ability to see if the server is active and updating</p>

```json
{
    "Count": int,           // Number of unique public keys indexed
    "LoadedToBlock": int,   // Last processed block height
    "CurrentBlock": int     // Current block height on the Flow network
}
```
