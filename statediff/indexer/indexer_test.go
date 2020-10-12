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

package indexer_test

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-ipfs-ds-help"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	ind "github.com/ethereum/go-ethereum/statediff/indexer"
	"github.com/ethereum/go-ethereum/statediff/indexer/mocks"
	eth "github.com/ethereum/go-ethereum/statediff/indexer/models"
	"github.com/ethereum/go-ethereum/statediff/indexer/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/shared"
)

func TestShared(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Indexer tests")
}

var _ = Describe("PublishAndIndexer", func() {
	var (
		db        *postgres.DB
		err       error
		indexer   *ind.StateDiffIndexer
		ipfsPgGet = `SELECT data FROM public.blocks
					WHERE key = $1`
	)
	BeforeEach(func() {
		db, err = shared.SetupDB()
		Expect(err).ToNot(HaveOccurred())
		indexer = ind.NewStateDiffIndexer(params.MainnetChainConfig, db)
		tx, err := indexer.PushBlock(
			mocks.MockBlock,
			mocks.MockReceipts,
			mocks.MockBlock.Difficulty())
		defer tx.Close()
		Expect(err).ToNot(HaveOccurred())
		for _, node := range mocks.StateDiffs {
			err = indexer.PushStateNode(tx, node)
			Expect(err).ToNot(HaveOccurred())
		}
		Expect(tx.BlockNumber).To(Equal(mocks.BlockNumber.Uint64()))
	})
	AfterEach(func() {
		ind.TearDownDB(db)
	})

	Describe("Publish", func() {
		It("Publishes and indexes header IPLDs in a single tx", func() {
			pgStr := `SELECT cid, td, reward, id
				FROM eth.header_cids
				WHERE block_number = $1`
			// check header was properly indexed
			type res struct {
				CID    string
				TD     string
				Reward string
				ID     int
			}
			header := new(res)
			err = db.QueryRowx(pgStr, 1).StructScan(header)
			Expect(err).ToNot(HaveOccurred())
			Expect(header.CID).To(Equal(mocks.HeaderCID.String()))
			Expect(header.TD).To(Equal(mocks.MockBlock.Difficulty().String()))
			Expect(header.Reward).To(Equal("5000000000000011250"))
			dc, err := cid.Decode(header.CID)
			Expect(err).ToNot(HaveOccurred())
			mhKey := dshelp.MultihashToDsKey(dc.Hash())
			prefixedKey := blockstore.BlockPrefix.String() + mhKey.String()
			var data []byte
			err = db.Get(&data, ipfsPgGet, prefixedKey)
			Expect(err).ToNot(HaveOccurred())
			Expect(data).To(Equal(mocks.MockHeaderRlp))
		})

		It("Publishes and indexes transaction IPLDs in a single tx", func() {
			// check that txs were properly indexed
			trxs := make([]string, 0)
			pgStr := `SELECT transaction_cids.cid FROM eth.transaction_cids INNER JOIN eth.header_cids ON (transaction_cids.header_id = header_cids.id)
				WHERE header_cids.block_number = $1`
			err = db.Select(&trxs, pgStr, 1)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(trxs)).To(Equal(3))
			Expect(shared.ListContainsString(trxs, mocks.Trx1CID.String())).To(BeTrue())
			Expect(shared.ListContainsString(trxs, mocks.Trx2CID.String())).To(BeTrue())
			Expect(shared.ListContainsString(trxs, mocks.Trx3CID.String())).To(BeTrue())
			// and published
			for _, c := range trxs {
				dc, err := cid.Decode(c)
				Expect(err).ToNot(HaveOccurred())
				mhKey := dshelp.MultihashToDsKey(dc.Hash())
				prefixedKey := blockstore.BlockPrefix.String() + mhKey.String()
				var data []byte
				err = db.Get(&data, ipfsPgGet, prefixedKey)
				Expect(err).ToNot(HaveOccurred())
				switch c {
				case mocks.Trx1CID.String():
					Expect(data).To(Equal(mocks.MockTransactions.GetRlp(0)))
				case mocks.Trx2CID.String():
					Expect(data).To(Equal(mocks.MockTransactions.GetRlp(1)))
				case mocks.Trx3CID.String():
					Expect(data).To(Equal(mocks.MockTransactions.GetRlp(2)))
				}
			}
		})

		It("Publishes and indexes receipt IPLDs in a single tx", func() {
			// check receipts were properly indexed
			rcts := make([]string, 0)
			pgStr := `SELECT receipt_cids.cid FROM eth.receipt_cids, eth.transaction_cids, eth.header_cids
				WHERE receipt_cids.tx_id = transaction_cids.id
				AND transaction_cids.header_id = header_cids.id
				AND header_cids.block_number = $1`
			err = db.Select(&rcts, pgStr, 1)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(rcts)).To(Equal(3))
			Expect(shared.ListContainsString(rcts, mocks.Rct1CID.String())).To(BeTrue())
			Expect(shared.ListContainsString(rcts, mocks.Rct2CID.String())).To(BeTrue())
			Expect(shared.ListContainsString(rcts, mocks.Rct3CID.String())).To(BeTrue())
			// and published
			for _, c := range rcts {
				dc, err := cid.Decode(c)
				Expect(err).ToNot(HaveOccurred())
				mhKey := dshelp.MultihashToDsKey(dc.Hash())
				prefixedKey := blockstore.BlockPrefix.String() + mhKey.String()
				var data []byte
				err = db.Get(&data, ipfsPgGet, prefixedKey)
				Expect(err).ToNot(HaveOccurred())
				switch c {
				case mocks.Rct1CID.String():
					Expect(data).To(Equal(mocks.MockReceipts.GetRlp(0)))
				case mocks.Rct2CID.String():
					Expect(data).To(Equal(mocks.MockReceipts.GetRlp(1)))
				case mocks.Rct3CID.String():
					Expect(data).To(Equal(mocks.MockReceipts.GetRlp(2)))
				}
			}
		})

		It("Publishes and indexes state IPLDs in a single tx", func() {
			// check that state nodes were properly indexed and published
			stateNodes := make([]eth.StateNodeModel, 0)
			pgStr := `SELECT state_cids.id, state_cids.cid, state_cids.state_leaf_key, state_cids.node_type, state_cids.state_path, state_cids.header_id
				FROM eth.state_cids INNER JOIN eth.header_cids ON (state_cids.header_id = header_cids.id)
				WHERE header_cids.block_number = $1`
			err = db.Select(&stateNodes, pgStr, 1)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(stateNodes)).To(Equal(2))
			for _, stateNode := range stateNodes {
				var data []byte
				dc, err := cid.Decode(stateNode.CID)
				Expect(err).ToNot(HaveOccurred())
				mhKey := dshelp.MultihashToDsKey(dc.Hash())
				prefixedKey := blockstore.BlockPrefix.String() + mhKey.String()
				err = db.Get(&data, ipfsPgGet, prefixedKey)
				Expect(err).ToNot(HaveOccurred())
				pgStr = `SELECT * from eth.state_accounts WHERE state_id = $1`
				var account eth.StateAccountModel
				err = db.Get(&account, pgStr, stateNode.ID)
				Expect(err).ToNot(HaveOccurred())
				if stateNode.CID == mocks.State1CID.String() {
					Expect(stateNode.NodeType).To(Equal(2))
					Expect(stateNode.StateKey).To(Equal(common.BytesToHash(mocks.ContractLeafKey).Hex()))
					Expect(stateNode.Path).To(Equal([]byte{'\x06'}))
					Expect(data).To(Equal(mocks.ContractLeafNode))
					Expect(account).To(Equal(eth.StateAccountModel{
						ID:          account.ID,
						StateID:     stateNode.ID,
						Balance:     "0",
						CodeHash:    mocks.ContractCodeHash.Bytes(),
						StorageRoot: mocks.ContractRoot,
						Nonce:       1,
					}))
				}
				if stateNode.CID == mocks.State2CID.String() {
					Expect(stateNode.NodeType).To(Equal(2))
					Expect(stateNode.StateKey).To(Equal(common.BytesToHash(mocks.AccountLeafKey).Hex()))
					Expect(stateNode.Path).To(Equal([]byte{'\x0c'}))
					Expect(data).To(Equal(mocks.AccountLeafNode))
					Expect(account).To(Equal(eth.StateAccountModel{
						ID:          account.ID,
						StateID:     stateNode.ID,
						Balance:     "1000",
						CodeHash:    mocks.AccountCodeHash.Bytes(),
						StorageRoot: mocks.AccountRoot,
						Nonce:       0,
					}))
				}
			}
			pgStr = `SELECT * from eth.state_accounts WHERE state_id = $1`
		})

		It("Publishes and indexes storage IPLDs in a single tx", func() {
			// check that storage nodes were properly indexed
			storageNodes := make([]eth.StorageNodeWithStateKeyModel, 0)
			pgStr := `SELECT storage_cids.cid, state_cids.state_leaf_key, storage_cids.storage_leaf_key, storage_cids.node_type, storage_cids.storage_path
				FROM eth.storage_cids, eth.state_cids, eth.header_cids
				WHERE storage_cids.state_id = state_cids.id
				AND state_cids.header_id = header_cids.id
				AND header_cids.block_number = $1`
			err = db.Select(&storageNodes, pgStr, 1)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(storageNodes)).To(Equal(1))
			Expect(storageNodes[0]).To(Equal(eth.StorageNodeWithStateKeyModel{
				CID:        mocks.StorageCID.String(),
				NodeType:   2,
				StorageKey: common.BytesToHash(mocks.StorageLeafKey).Hex(),
				StateKey:   common.BytesToHash(mocks.ContractLeafKey).Hex(),
				Path:       []byte{},
			}))
			var data []byte
			dc, err := cid.Decode(storageNodes[0].CID)
			Expect(err).ToNot(HaveOccurred())
			mhKey := dshelp.MultihashToDsKey(dc.Hash())
			prefixedKey := blockstore.BlockPrefix.String() + mhKey.String()
			err = db.Get(&data, ipfsPgGet, prefixedKey)
			Expect(err).ToNot(HaveOccurred())
			Expect(data).To(Equal(mocks.StorageLeafNode))
		})
	})
})
