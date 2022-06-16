# Flow Public Key Indexer
A observer service for indexing flow Accounts public keys and REST service that serves up public key data.


## Run Parameters
`KEYIDX_LOGLEVEL` default: "info"
<br>Log Level: Takes string log level value for zerolog, "debug", "info", ...</br>

`KEYIDX_PORT` default: "8888"
<br>Port: The port the REST service is hosted on</br>

`KEYIDX_FLOWURL1` default: "access.mainnet.nodes.onflow.org:9000"
<br>Flow Url: Access node endpoint blockchain data is pulled from, needs to match up with Chain Id. Up to 4 access nodes can be provided, only one is required. The access nodes are cycled through each request to get public key data.</br>

`KEYIDX_FLOWURL2` default: none
<br>Flow Url: Access node endpoint</br>

`KEYIDX_FLOWURL3` default: none
<br>Flow Url: Access node endpoint</br>

`KEYIDX_FLOWURL4` default: none
<br>Flow Url: Access node endpoint</br>

`KEYIDX_DBPATH` default: "./db"
<br>db path: badger db directory where all db files live </br>

`KEYIDX_CHAINID` default: "flow-mainnet"
<br>Chain Id: target blockchain, valid values are "flow-testnet" and "flow-mainnet". Needs to match up with Flor Url</br>

`KEYIDX_MAXACCTKEYS` default: 500
<br>Max Acct Keys: maximum number of keys to index per account, accounts that have more public keys that exceed max do not get indexed. Accounts are logged out that exceed the max </br>

`KEYIDX_BATCHSIZE` default: 500
<br>Batch Size: max number of accounts in a batch sent to cadence script that access node executes. Cadence script can exceed execution if accounts have a lot of keys</br>

`KEYIDX_IGNOREZEROWEIGHT` default: true
<br>Ignore Zero Weight: tells the cadence script to ignore public keys with zero weight. These keys will not be indexed</br>

`KEYIDX_IGNOREREVOKED` default: true
<br>Ignroe Revoked: tells the cadenc escript to ignore public keys that have been revoked. These keys will not be indexed</br>

`KEYIDX_WAITNUMBLOCKS` default: 500
<br>Wait Num Blocks: number of blocks to wait before running an incremental data load</br>

`KEYIDX_BLOCKPOLINTERVALSEC` default: 120
<br>Block Pol Interval Sec: number of seconds to wait before checking current block height to determine to run an incremental data load</br>

`KEYIDX_MAXBLOCKRANGE` default: 600
<br>Max Block Range: number of blocks that will trigger a bulk load if services falls behind. If this happens try increasing `Batch Size` parameter to a safe amount and not to trigger max computation error when cadence script is executed. Also, the server can only query 600 blocks</br>

`KEYIDX_SILENCEBADGERDB` default: true
<br>Badger db has log output that is not informative, the user can choose to see the output if this is changed to false</br>


## How to Run
Since this is a golang service there are many ways to run it. Below are two ways to run this service
### Command line
```go run .```
### Docker
Configuration: Run docker in default 10 gig memory size. Reducing the running memory size reduces performance, the lowest is 6 gig, bulk sync and public key query responses are reasonable compared to running with more memory.<br>
Create a docker container<br>
```docker build -t key-indexer .``` <br>
This service stores public key data and needs persistent storage <br>
Run the docker and map the port and use a directory on disk<br>
Notice that environmental variables can be passed in. See variables above<br>
```docker run -p 8888:8080 --env KEYIDX_DBPATH=/db -v /tmp:/db key-indexer``` <br>
To see the logs of the container, get the container id <br>
```docker container ls``` <br>
View the containers logs <br>
```docker logs <container id>``` <br>
## REST service
`Endpoints`
* \<root\>/key/{public key}
<p>note: public key is in base64 format
serves up json object</p>

```
{
	"publicKey": string  // public key string in base64
	"accounts" : string array
}

```

* \<root\>/status
<p>note: this endpoint gives ability to see if the server is active and updating</p>

```
{
"publicKeyCount": int         // total public keys indexed
"currentBlockHeight": int     // current block of access node
"updatedToBlockHeight": int   // block data is updated against
"pendingLoadBlockHeight": int // block height loading data from 
"isBulkLoading": bool         // indicates if bulk loading data
}
```
