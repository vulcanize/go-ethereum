// VulcanizeDB
// Copyright © 2019 Vulcanize

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.

// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package indexer

import (
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"

	node "github.com/ipfs/go-ipld-format"
	"github.com/jmoiron/sqlx"
	"github.com/multiformats/go-multihash"

	"github.com/ethereum/go-ethereum/statediff/indexer/ipfs/ipld"
	"github.com/ethereum/go-ethereum/statediff/indexer/models"
	"github.com/ethereum/go-ethereum/statediff/indexer/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/shared"
	sdtypes "github.com/ethereum/go-ethereum/statediff/types"
)

// Indexer interface to allow substitution of mocks for testing
type Indexer interface {
	PushBlock(block *types.Block, receipts types.Receipts, totalDifficulty *big.Int) (*BlockTx, error)
	PushStateNode(tx *BlockTx, stateNode sdtypes.StateNode) error
}

// StateDiffIndexer satisfies the Indexer interface for ethereum statediff objects
type StateDiffIndexer struct {
	chainConfig *params.ChainConfig
	dbWriter    *PostgresCIDWriter
}

// NewStateDiffIndexer creates a pointer to a new PayloadConverter which satisfies the PayloadConverter interface
func NewStateDiffIndexer(chainConfig *params.ChainConfig, db *postgres.DB) *StateDiffIndexer {
	return &StateDiffIndexer{
		chainConfig: chainConfig,
		dbWriter:    NewPostgresCIDWriter(db),
	}
}

type BlockTx struct {
	dbtx        *sqlx.Tx
	BlockNumber uint64
	headerID    int64
	err         error
	Close       func() error
}

// Pushes and indexes block data in database, excluding state & storage nodes (header, uncles, transactions & receipts)
// Returns an initiated DB transaction which must be Closed via defer to commit or rollback
func (sdt *StateDiffIndexer) PushBlock(block *types.Block, receipts types.Receipts, totalDifficulty *big.Int) (*BlockTx, error) {
	start, t := time.Now(), time.Now()
	blockHash := block.Hash()
	blockHashStr := blockHash.String()
	height := block.NumberU64()
	traceMsg := fmt.Sprintf("indexer stats for statediff at %d with hash %s:\r\n", height, blockHashStr)
	transactions := block.Transactions()
	// Derive any missing fields
	if err := receipts.DeriveFields(sdt.chainConfig, blockHash, height, transactions); err != nil {
		return nil, err
	}
	// Generate the block iplds
	headerNode, uncleNodes, txNodes, txTrieNodes, rctNodes, rctTrieNodes, err := ipld.FromBlockAndReceipts(block, receipts)
	if err != nil {
		return nil, err
	}
	if len(txNodes) != len(txTrieNodes) && len(rctNodes) != len(rctTrieNodes) && len(txNodes) != len(rctNodes) {
		return nil, fmt.Errorf("expected number of transactions (%d), transaction trie nodes (%d), receipts (%d), and receipt trie nodes (%d)to be equal", len(txNodes), len(txTrieNodes), len(rctNodes), len(rctTrieNodes))
	}
	// Calculate reward
	reward := CalcEthBlockReward(block.Header(), block.Uncles(), block.Transactions(), receipts)
	traceMsg += fmt.Sprintf("payload decoding duration: %s\r\n", time.Now().Sub(t).String())
	t = time.Now()
	// Begin new db tx for everything
	tx, err := sdt.dbWriter.db.Beginx()
	if err != nil {
		return nil, err
	}
	blocktx := BlockTx{
		dbtx: tx,
		// handle transaction commit or rollback for any return case
		Close: func() error {
			var err error
			if p := recover(); p != nil {
				shared.Rollback(tx)
				panic(p)
			} else {
				err = tx.Commit()
				traceMsg += fmt.Sprintf("postgres transaction commit duration: %s\r\n", time.Now().Sub(t).String())
			}
			traceMsg += fmt.Sprintf(" TOTAL PROCESSING DURATION: %s\r\n", time.Now().Sub(start).String())
			log.Info(traceMsg)
			return err
		},
	}
	traceMsg += fmt.Sprintf("time spent waiting for free postgres tx: %s:\r\n", time.Now().Sub(t).String())
	t = time.Now()

	// Publish and index header, collect headerID
	headerID, err := sdt.processHeader(tx, block.Header(), headerNode, reward, totalDifficulty)
	if err != nil {
		return nil, err
	}
	traceMsg += fmt.Sprintf("header processing duration: %s\r\n", time.Now().Sub(t).String())
	t = time.Now()
	// Publish and index uncles
	if err := sdt.processUncles(tx, headerID, height, uncleNodes); err != nil {
		return nil, err
	}
	traceMsg += fmt.Sprintf("uncle processing duration: %s\r\n", time.Now().Sub(t).String())
	t = time.Now()
	// Publish and index receipts and txs
	if err := sdt.processReceiptsAndTxs(tx, processArgs{
		headerID:     headerID,
		blockNumber:  block.Number(),
		receipts:     receipts,
		txs:          transactions,
		rctNodes:     rctNodes,
		rctTrieNodes: rctTrieNodes,
		txNodes:      txNodes,
		txTrieNodes:  txTrieNodes,
	}); err != nil {
		return nil, err
	}
	traceMsg += fmt.Sprintf("tx and receipt processing duration: %s\r\n", time.Now().Sub(t).String())
	// t = time.Now()
	blocktx.BlockNumber = height
	blocktx.headerID = headerID
	return &blocktx, err // return error explicity so that the defer() assigns to it
}

// processHeader publishes and indexes a header IPLD in Postgres
// it returns the headerID
func (sdt *StateDiffIndexer) processHeader(tx *sqlx.Tx, header *types.Header, headerNode node.Node, reward, td *big.Int) (int64, error) {
	// publish header
	if err := shared.PublishIPLD(tx, headerNode); err != nil {
		return 0, err
	}
	// index header
	return sdt.dbWriter.upsertHeaderCID(tx, models.HeaderModel{
		CID:             headerNode.Cid().String(),
		MhKey:           shared.MultihashKeyFromCID(headerNode.Cid()),
		ParentHash:      header.ParentHash.String(),
		BlockNumber:     header.Number.String(),
		BlockHash:       header.Hash().String(),
		TotalDifficulty: td.String(),
		Reward:          reward.String(),
		Bloom:           header.Bloom.Bytes(),
		StateRoot:       header.Root.String(),
		RctRoot:         header.ReceiptHash.String(),
		TxRoot:          header.TxHash.String(),
		UncleRoot:       header.UncleHash.String(),
		Timestamp:       header.Time,
	})
}

func (sdt *StateDiffIndexer) processUncles(tx *sqlx.Tx, headerID int64, blockNumber uint64, uncleNodes []*ipld.EthHeader) error {
	// publish and index uncles
	for _, uncleNode := range uncleNodes {
		if err := shared.PublishIPLD(tx, uncleNode); err != nil {
			return err
		}
		uncleReward := CalcUncleMinerReward(blockNumber, uncleNode.Number.Uint64())
		uncle := models.UncleModel{
			CID:        uncleNode.Cid().String(),
			MhKey:      shared.MultihashKeyFromCID(uncleNode.Cid()),
			ParentHash: uncleNode.ParentHash.String(),
			BlockHash:  uncleNode.Hash().String(),
			Reward:     uncleReward.String(),
		}
		if err := sdt.dbWriter.upsertUncleCID(tx, uncle, headerID); err != nil {
			return err
		}
	}
	return nil
}

// processArgs bundles arugments to processReceiptsAndTxs
type processArgs struct {
	headerID     int64
	blockNumber  *big.Int
	receipts     types.Receipts
	txs          types.Transactions
	rctNodes     []*ipld.EthReceipt
	rctTrieNodes []*ipld.EthRctTrie
	txNodes      []*ipld.EthTx
	txTrieNodes  []*ipld.EthTxTrie
}

// processReceiptsAndTxs publishes and indexes receipt and transaction IPLDs in Postgres
func (sdt *StateDiffIndexer) processReceiptsAndTxs(tx *sqlx.Tx, args processArgs) error {
	// Process receipts and txs
	signer := types.MakeSigner(sdt.chainConfig, args.blockNumber)
	for i, receipt := range args.receipts {
		// tx that corresponds with this receipt
		trx := args.txs[i]
		from, err := types.Sender(signer, trx)
		if err != nil {
			return err
		}

		// Publishing
		// publish trie nodes, these aren't indexed directly
		if err := shared.PublishIPLD(tx, args.txTrieNodes[i]); err != nil {
			return err
		}
		if err := shared.PublishIPLD(tx, args.rctTrieNodes[i]); err != nil {
			return err
		}
		// publish the txs and receipts
		txNode, rctNode := args.txNodes[i], args.rctNodes[i]
		if err := shared.PublishIPLD(tx, txNode); err != nil {
			return err
		}
		if err := shared.PublishIPLD(tx, rctNode); err != nil {
			return err
		}

		// Indexing
		// extract topic and contract data from the receipt for indexing
		topicSets := make([][]string, 4)
		mappedContracts := make(map[string]bool) // use map to avoid duplicate addresses
		for _, log := range receipt.Logs {
			for i, topic := range log.Topics {
				topicSets[i] = append(topicSets[i], topic.Hex())
			}
			mappedContracts[log.Address.String()] = true
		}
		// these are the contracts seen in the logs
		logContracts := make([]string, 0, len(mappedContracts))
		for addr := range mappedContracts {
			logContracts = append(logContracts, addr)
		}
		// this is the contract address if this receipt is for a contract creation tx
		contract := shared.HandleZeroAddr(receipt.ContractAddress)
		var contractHash string
		isDeployment := contract != ""
		if isDeployment {
			contractHash = crypto.Keccak256Hash(common.HexToAddress(contract).Bytes()).String()
			// if tx is a contract deployment, publish the data (code)
			// codec doesn't matter in this case sine we are not interested in the cid and the db key is multihash-derived
			// TODO: THE DATA IS NOT DIRECTLY THE CONTRACT CODE; THERE IS A MISSING PROCESSING STEP HERE
			// the contractHash => contract code is not currently correct
			if _, err := shared.PublishRaw(tx, ipld.MEthStorageTrie, multihash.KECCAK_256, trx.Data()); err != nil {
				return err
			}
		}
		// index tx first so that the receipt can reference it by FK
		txModel := models.TxModel{
			Dst:        shared.HandleZeroAddrPointer(trx.To()),
			Src:        shared.HandleZeroAddr(from),
			TxHash:     trx.Hash().String(),
			Index:      int64(i),
			Data:       trx.Data(),
			Deployment: isDeployment,
			CID:        txNode.Cid().String(),
			MhKey:      shared.MultihashKeyFromCID(txNode.Cid()),
		}
		txID, err := sdt.dbWriter.upsertTransactionCID(tx, txModel, args.headerID)
		if err != nil {
			return err
		}
		// index the receipt
		rctModel := models.ReceiptModel{
			Topic0s:      topicSets[0],
			Topic1s:      topicSets[1],
			Topic2s:      topicSets[2],
			Topic3s:      topicSets[3],
			Contract:     contract,
			ContractHash: contractHash,
			LogContracts: logContracts,
			CID:          rctNode.Cid().String(),
			MhKey:        shared.MultihashKeyFromCID(rctNode.Cid()),
		}
		if err := sdt.dbWriter.upsertReceiptCID(tx, rctModel, txID); err != nil {
			return err
		}
	}
	return nil
}

func (sdt *StateDiffIndexer) PushStateNode(tx *BlockTx, stateNode sdtypes.StateNode) error {
	// publish the state node
	stateCIDStr, err := shared.PublishRaw(tx.dbtx, ipld.MEthStateTrie, multihash.KECCAK_256, stateNode.NodeValue)
	if err != nil {
		return err
	}
	mhKey, _ := shared.MultihashKeyFromCIDString(stateCIDStr)
	stateModel := models.StateNodeModel{
		Path:     stateNode.Path,
		StateKey: common.BytesToHash(stateNode.LeafKey).String(),
		CID:      stateCIDStr,
		MhKey:    mhKey,
		NodeType: ResolveFromNodeType(stateNode.NodeType),
	}
	// index the state node, collect the stateID to reference by FK
	stateID, err := sdt.dbWriter.upsertStateCID(tx.dbtx, stateModel, tx.headerID)
	if err != nil {
		return err
	}
	// if we have a leaf, decode and index the account data
	if stateNode.NodeType == sdtypes.Leaf {
		var i []interface{}
		if err := rlp.DecodeBytes(stateNode.NodeValue, &i); err != nil {
			return fmt.Errorf("error decoding state leaf node rlp: %s", err.Error())
		}
		if len(i) != 2 {
			return fmt.Errorf("eth IPLDPublisher expected state leaf node rlp to decode into two elements")
		}
		var account state.Account
		if err := rlp.DecodeBytes(i[1].([]byte), &account); err != nil {
			return fmt.Errorf("error decoding state account rlp: %s", err.Error())
		}
		accountModel := models.StateAccountModel{
			Balance:     account.Balance.String(),
			Nonce:       account.Nonce,
			CodeHash:    account.CodeHash,
			StorageRoot: account.Root.String(),
		}
		if err := sdt.dbWriter.upsertStateAccount(tx.dbtx, accountModel, stateID); err != nil {
			return err
		}
	}
	// if there are any storage nodes associated with this node, publish and index them
	for _, storageNode := range stateNode.StorageNodes {
		storageCIDStr, err := shared.PublishRaw(tx.dbtx, ipld.MEthStorageTrie, multihash.KECCAK_256, storageNode.NodeValue)
		if err != nil {
			return err
		}
		mhKey, _ := shared.MultihashKeyFromCIDString(storageCIDStr)
		storageModel := models.StorageNodeModel{
			Path:       storageNode.Path,
			StorageKey: common.BytesToHash(storageNode.LeafKey).String(),
			CID:        storageCIDStr,
			MhKey:      mhKey,
			NodeType:   ResolveFromNodeType(storageNode.NodeType),
		}
		if err := sdt.dbWriter.upsertStorageCID(tx.dbtx, storageModel, stateID); err != nil {
			return err
		}
	}

	return nil
}
