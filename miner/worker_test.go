// Copyright 2018 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package miner

import (
	"math/big"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/clique"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/params"
)

const (
	// testCode is the testing contract binary code which will initialises some
	// variables in constructor
	testCode = "0x60806040527fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff0060005534801561003457600080fd5b5060fc806100436000396000f3fe6080604052348015600f57600080fd5b506004361060325760003560e01c80630c4dae8814603757806398a213cf146053575b600080fd5b603d607e565b6040518082815260200191505060405180910390f35b607c60048036036020811015606757600080fd5b81019080803590602001909291905050506084565b005b60005481565b806000819055507fe9e44f9f7da8c559de847a3232b57364adc0354f15a2cd8dc636d54396f9587a6000546040518082815260200191505060405180910390a15056fea265627a7a723058208ae31d9424f2d0bc2a3da1a5dd659db2d71ec322a17db8f87e19e209e3a1ff4a64736f6c634300050a0032"

	// testGas is the gas required for contract deployment.
	testGas = 144109
)

var (
	// Test chain configurations
	testTxPoolConfig                  core.TxPoolConfig
	ethashChainConfig                 *params.ChainConfig
	cliqueChainConfig                 *params.ChainConfig
	eip1559ChainConfig                *params.ChainConfig
	eip1559CliqueChainConfig          *params.ChainConfig
	eip1559FinalizedChainConfig       *params.ChainConfig
	eip1559FinalizedCliqueChainConfig *params.ChainConfig

	// Test accounts
	testBankKey, _  = crypto.GenerateKey()
	testBankAddress = crypto.PubkeyToAddress(testBankKey.PublicKey)
	testBankFunds   = big.NewInt(1000000000000000000)

	testUserKey, _  = crypto.GenerateKey()
	testUserAddress = crypto.PubkeyToAddress(testUserKey.PublicKey)

	// Test transactions
	pendingTxs                 []*types.Transaction
	newTxs                     []*types.Transaction
	pendingLegacyAndEIP1559Txs []*types.Transaction
	newLegacyAndEIP1559Txs     []*types.Transaction
	pendingEIP1559Txs          []*types.Transaction
	newEIP1559Txs              []*types.Transaction

	testConfig = &Config{
		Recommit: time.Second,
		GasFloor: params.GenesisGasLimit,
		GasCeil:  params.GenesisGasLimit,
	}
)

func init() {
	testTxPoolConfig = core.DefaultTxPoolConfig
	testTxPoolConfig.Journal = ""
	ethashChainConfig = params.TestChainConfig
	cliqueChainConfig = params.TestChainConfig
	cliqueChainConfig.Clique = &params.CliqueConfig{
		Period: 10,
		Epoch:  30000,
	}
	eip1559ChainConfig = params.EIP1559ChainConfig
	eip1559CliqueChainConfig = params.EIP1559ChainConfig
	eip1559CliqueChainConfig.Clique = &params.CliqueConfig{
		Period: 10,
		Epoch:  30000,
	}
	eip1559FinalizedChainConfig = params.EIP1559FinalizedChainConfig
	eip1559FinalizedCliqueChainConfig = params.EIP1559FinalizedChainConfig
	eip1559FinalizedCliqueChainConfig.Clique = &params.CliqueConfig{
		Period: 10,
		Epoch:  30000,
	}
	tx, _ := types.SignTx(types.NewTransaction(0, testUserAddress, big.NewInt(1000), params.TxGas, new(big.Int), nil, nil, nil), types.HomesteadSigner{}, testBankKey)
	pendingTxs = append(pendingTxs, tx)
	pendingLegacyAndEIP1559Txs = append(pendingLegacyAndEIP1559Txs, tx)
	tx, _ = types.SignTx(types.NewTransaction(1, testUserAddress, big.NewInt(1000), params.TxGas, new(big.Int), nil, nil, nil), types.HomesteadSigner{}, testBankKey)
	newTxs = append(newTxs, tx)
	newLegacyAndEIP1559Txs = append(newLegacyAndEIP1559Txs, tx)
	tx, _ = types.SignTx(types.NewTransaction(0, testUserAddress, big.NewInt(1000), params.TxGas, nil, nil, new(big.Int), new(big.Int)), types.HomesteadSigner{}, testBankKey)
	pendingEIP1559Txs = append(pendingEIP1559Txs, tx)
	tx, _ = types.SignTx(types.NewTransaction(1, testUserAddress, big.NewInt(1000), params.TxGas, nil, nil, new(big.Int), new(big.Int)), types.HomesteadSigner{}, testBankKey)
	newEIP1559Txs = append(newEIP1559Txs, tx)
	rand.Seed(time.Now().UnixNano())
}

// testWorkerBackend implements worker.Backend interfaces and wraps all information needed during the testing.
type testWorkerBackend struct {
	db         ethdb.Database
	txPool     *core.TxPool
	chain      *core.BlockChain
	testTxFeed event.Feed
	genesis    *core.Genesis
	uncleBlock *types.Block
}

func newTestWorkerBackend(t *testing.T, chainConfig *params.ChainConfig, engine consensus.Engine, db ethdb.Database, n int, baseFee *big.Int) *testWorkerBackend {
	var gspec = core.Genesis{
		Config:  chainConfig,
		Alloc:   core.GenesisAlloc{testBankAddress: {Balance: testBankFunds}},
		BaseFee: baseFee,
	}

	switch e := engine.(type) {
	case *clique.Clique:
		gspec.ExtraData = make([]byte, 32+common.AddressLength+crypto.SignatureLength)
		copy(gspec.ExtraData[32:32+common.AddressLength], testBankAddress.Bytes())
		e.Authorize(testBankAddress, func(account accounts.Account, s string, data []byte) ([]byte, error) {
			return crypto.Sign(crypto.Keccak256(data), testBankKey)
		})
	case *ethash.Ethash:
	default:
		t.Fatalf("unexpected consensus engine type: %T", engine)
	}
	genesis := gspec.MustCommit(db)

	chain, _ := core.NewBlockChain(db, &core.CacheConfig{TrieDirtyDisabled: true}, gspec.Config, engine, vm.Config{}, nil, nil)
	txpool := core.NewTxPool(testTxPoolConfig, chainConfig, chain)

	// Generate a small n-block chain and an uncle block for it
	if n > 0 {
		blocks, _ := core.GenerateChain(chainConfig, genesis, engine, db, n, func(i int, gen *core.BlockGen) {
			gen.SetCoinbase(testBankAddress)
		})
		if _, err := chain.InsertChain(blocks); err != nil {
			t.Fatalf("failed to insert origin chain: %v", err)
		}
	}
	parent := genesis
	if n > 0 {
		parent = chain.GetBlockByHash(chain.CurrentBlock().ParentHash())
	}
	blocks, _ := core.GenerateChain(chainConfig, parent, engine, db, 1, func(i int, gen *core.BlockGen) {
		gen.SetCoinbase(testUserAddress)
	})

	return &testWorkerBackend{
		db:         db,
		chain:      chain,
		txPool:     txpool,
		genesis:    &gspec,
		uncleBlock: blocks[0],
	}
}

func (b *testWorkerBackend) BlockChain() *core.BlockChain { return b.chain }
func (b *testWorkerBackend) TxPool() *core.TxPool         { return b.txPool }

func (b *testWorkerBackend) newRandomUncle() *types.Block {
	var parent *types.Block
	cur := b.chain.CurrentBlock()
	if cur.NumberU64() == 0 {
		parent = b.chain.Genesis()
	} else {
		parent = b.chain.GetBlockByHash(b.chain.CurrentBlock().ParentHash())
	}
	blocks, _ := core.GenerateChain(b.chain.Config(), parent, b.chain.Engine(), b.db, 1, func(i int, gen *core.BlockGen) {
		var addr = make([]byte, common.AddressLength)
		rand.Read(addr)
		gen.SetCoinbase(common.BytesToAddress(addr))
	})
	return blocks[0]
}

func (b *testWorkerBackend) newRandomTx(creation, eip1559 bool) *types.Transaction {
	var tx *types.Transaction
	if creation {
		if eip1559 {
			tx, _ = types.SignTx(types.NewContractCreation(b.txPool.Nonce(testBankAddress), big.NewInt(0), testGas, nil, common.FromHex(testCode), new(big.Int), new(big.Int)), types.HomesteadSigner{}, testBankKey)
		} else {
			tx, _ = types.SignTx(types.NewContractCreation(b.txPool.Nonce(testBankAddress), big.NewInt(0), testGas, new(big.Int), common.FromHex(testCode), nil, nil), types.HomesteadSigner{}, testBankKey)
		}
	} else {
		if eip1559 {
			tx, _ = types.SignTx(types.NewTransaction(b.txPool.Nonce(testBankAddress), testUserAddress, big.NewInt(1000), params.TxGas, nil, nil, new(big.Int), new(big.Int)), types.HomesteadSigner{}, testBankKey)
		} else {
			tx, _ = types.SignTx(types.NewTransaction(b.txPool.Nonce(testBankAddress), testUserAddress, big.NewInt(1000), params.TxGas, new(big.Int), nil, nil, nil), types.HomesteadSigner{}, testBankKey)
		}
	}
	return tx
}

func newTestWorker(t *testing.T, chainConfig *params.ChainConfig, engine consensus.Engine, db ethdb.Database, blocks int, baseFee *big.Int, txs []*types.Transaction) (*worker, *testWorkerBackend) {
	backend := newTestWorkerBackend(t, chainConfig, engine, db, blocks, baseFee)
	backend.txPool.AddLocals(txs)
	w := newWorker(testConfig, chainConfig, engine, backend, new(event.TypeMux), nil, false)
	w.setEtherbase(testBankAddress)
	return w, backend
}

func TestGenerateBlockAndImportEthash(t *testing.T) {
	testGenerateBlockAndImport(t, false, params.AllEthashProtocolChanges, nil, pendingTxs, false, false)
}
func TestGenerateBlockAndImportClique(t *testing.T) {
	testGenerateBlockAndImport(t, true, params.AllCliqueProtocolChanges, nil, pendingTxs, false, false)
}
func TestGenerateBlockAndImportEthashEIP1559(t *testing.T) {
	testGenerateBlockAndImport(t, false, params.EIP1559ChainConfig, new(big.Int), pendingLegacyAndEIP1559Txs, true, false)
}
func TestGenerateBlockAndImportCliqueEIP1559(t *testing.T) {
	testGenerateBlockAndImport(t, true, params.EIP1559ChainConfig, new(big.Int), pendingLegacyAndEIP1559Txs, true, false)
}
func TestGenerateBlockAndImportEthashEIP1559Finalized(t *testing.T) {
	testGenerateBlockAndImport(t, false, params.EIP1559FinalizedChainConfig, new(big.Int), pendingEIP1559Txs, true, true)
}
func TestGenerateBlockAndImportCliqueEIP1559Finalized(t *testing.T) {
	testGenerateBlockAndImport(t, true, params.EIP1559FinalizedChainConfig, new(big.Int), pendingEIP1559Txs, true, true)
}

func testGenerateBlockAndImport(t *testing.T, isClique bool, c *params.ChainConfig, baseFee *big.Int, pTxs []*types.Transaction, eip1559, eip1559Finalized bool) {
	var (
		engine consensus.Engine
		db     = rawdb.NewMemoryDatabase()
	)
	if isClique {
		c.Clique = &params.CliqueConfig{Period: 1, Epoch: 30000}
		engine = clique.New(c.Clique, db)
	} else {
		engine = ethash.NewFaker()
	}

	w, b := newTestWorker(t, c, engine, db, 0, baseFee, pTxs)
	defer w.close()

	// This test chain imports the mined blocks.
	db2 := rawdb.NewMemoryDatabase()
	b.genesis.MustCommit(db2)
	chain, _ := core.NewBlockChain(db2, nil, b.chain.Config(), engine, vm.Config{}, nil, nil)
	defer chain.Stop()

	// Ignore empty commit here for less noise.
	w.skipSealHook = func(task *task) bool {
		return len(task.receipts) == 0
	}

	// Wait for mined blocks.
	sub := w.mux.Subscribe(core.NewMinedBlockEvent{})
	defer sub.Unsubscribe()

	// Start mining!
	w.start()

	for i := 0; i < 5; i++ {
		b.txPool.AddLocal(b.newRandomTx(true, eip1559))
		b.txPool.AddLocal(b.newRandomTx(false, eip1559Finalized))
		w.postSideBlock(core.ChainSideEvent{Block: b.newRandomUncle()})
		w.postSideBlock(core.ChainSideEvent{Block: b.newRandomUncle()})

		select {
		case ev := <-sub.Chan():
			block := ev.Data.(core.NewMinedBlockEvent).Block
			if _, err := chain.InsertChain([]*types.Block{block}); err != nil {
				t.Fatalf("failed to insert new mined block %d: %v", block.NumberU64(), err)
			}
		case <-time.After(3 * time.Second): // Worker needs 1s to include new changes.
			t.Fatalf("timeout")
		}
	}
}

func TestEmptyWorkEthash(t *testing.T) {
	testEmptyWork(t, ethashChainConfig, ethash.NewFaker(), pendingTxs, nil)
}
func TestEmptyWorkClique(t *testing.T) {
	testEmptyWork(t, cliqueChainConfig, clique.New(cliqueChainConfig.Clique, rawdb.NewMemoryDatabase()), pendingTxs, nil)
}
func TestEmptyWorkEthashEIP1559(t *testing.T) {
	testEmptyWork(t, eip1559ChainConfig, ethash.NewFaker(), pendingLegacyAndEIP1559Txs, new(big.Int))
}
func TestEmptyWorkCliqueEIP1559(t *testing.T) {
	testEmptyWork(t, eip1559CliqueChainConfig, clique.New(cliqueChainConfig.Clique, rawdb.NewMemoryDatabase()), pendingLegacyAndEIP1559Txs, new(big.Int))
}
func TestEmptyWorkEthashEIP1559Finalized(t *testing.T) {
	testEmptyWork(t, eip1559FinalizedChainConfig, ethash.NewFaker(), pendingEIP1559Txs, new(big.Int))
}
func TestEmptyWorkCliqueEIP1559Finalized(t *testing.T) {
	testEmptyWork(t, eip1559FinalizedCliqueChainConfig, clique.New(cliqueChainConfig.Clique, rawdb.NewMemoryDatabase()), pendingEIP1559Txs, new(big.Int))
}

func testEmptyWork(t *testing.T, chainConfig *params.ChainConfig, engine consensus.Engine, pTxs []*types.Transaction, baseFee *big.Int) {
	defer engine.Close()

	w, _ := newTestWorker(t, chainConfig, engine, rawdb.NewMemoryDatabase(), 0, baseFee, pTxs)
	defer w.close()

	var (
		taskIndex int
		taskCh    = make(chan struct{}, 2)
	)
	checkEqual := func(t *testing.T, task *task, index int) {
		// The first empty work without any txs included
		receiptLen, balance := 0, big.NewInt(0)
		if index == 1 {
			// The second full work with 1 tx included
			receiptLen, balance = 1, big.NewInt(1000)
		}
		if len(task.receipts) != receiptLen {
			t.Fatalf("receipt number mismatch: have %d, want %d", len(task.receipts), receiptLen)
		}
		if task.state.GetBalance(testUserAddress).Cmp(balance) != 0 {
			t.Fatalf("account balance mismatch: have %d, want %d", task.state.GetBalance(testUserAddress), balance)
		}
	}
	w.newTaskHook = func(task *task) {
		if task.block.NumberU64() == 1 {
			checkEqual(t, task, taskIndex)
			taskIndex += 1
			taskCh <- struct{}{}
		}
	}
	w.skipSealHook = func(task *task) bool { return true }
	w.fullTaskHook = func() {
		time.Sleep(100 * time.Millisecond)
	}
	w.start() // Start mining!
	for i := 0; i < 2; i += 1 {
		select {
		case <-taskCh:
		case <-time.NewTimer(3 * time.Second).C:
			t.Error("new task timeout")
		}
	}
}

func TestStreamUncleBlock(t *testing.T) {
	testStreamUncleBlock(t, ethashChainConfig, pendingTxs, nil)
}
func TestStreamUncleBlockEIP1559(t *testing.T) {
	testStreamUncleBlock(t, eip1559ChainConfig, pendingLegacyAndEIP1559Txs, new(big.Int))
}
func TestStreamUncleBlockEIP1559Finalized(t *testing.T) {
	testStreamUncleBlock(t, eip1559FinalizedChainConfig, pendingEIP1559Txs, new(big.Int))
}

func testStreamUncleBlock(t *testing.T, chainConfig *params.ChainConfig, pTxs []*types.Transaction, baseFee *big.Int) {
	ethash := ethash.NewFaker()
	defer ethash.Close()

	w, b := newTestWorker(t, chainConfig, ethash, rawdb.NewMemoryDatabase(), 1, baseFee, pTxs)
	defer w.close()

	var taskCh = make(chan struct{})

	taskIndex := 0
	w.newTaskHook = func(task *task) {
		if task.block.NumberU64() == 2 {
			// The first task is an empty task, the second
			// one has 1 pending tx, the third one has 1 tx
			// and 1 uncle.
			if taskIndex == 2 {
				have := task.block.Header().UncleHash
				want := types.CalcUncleHash([]*types.Header{b.uncleBlock.Header()})
				if have != want {
					t.Errorf("uncle hash mismatch: have %s, want %s", have.Hex(), want.Hex())
				}
			}
			taskCh <- struct{}{}
			taskIndex += 1
		}
	}
	w.skipSealHook = func(task *task) bool {
		return true
	}
	w.fullTaskHook = func() {
		time.Sleep(100 * time.Millisecond)
	}
	w.start()

	for i := 0; i < 2; i += 1 {
		select {
		case <-taskCh:
		case <-time.NewTimer(time.Second).C:
			t.Error("new task timeout")
		}
	}

	w.postSideBlock(core.ChainSideEvent{Block: b.uncleBlock})

	select {
	case <-taskCh:
	case <-time.NewTimer(time.Second).C:
		t.Error("new task timeout")
	}
}

func TestRegenerateMiningBlockEthash(t *testing.T) {
	testRegenerateMiningBlock(t, ethashChainConfig, ethash.NewFaker(), pendingTxs, newTxs, nil)
}

func TestRegenerateMiningBlockClique(t *testing.T) {
	testRegenerateMiningBlock(t, cliqueChainConfig, clique.New(cliqueChainConfig.Clique, rawdb.NewMemoryDatabase()), pendingTxs, newTxs, nil)
}

func TestRegenerateMiningBlockEthashEIP1559(t *testing.T) {
	testRegenerateMiningBlock(t, eip1559ChainConfig, ethash.NewFaker(), pendingLegacyAndEIP1559Txs, newLegacyAndEIP1559Txs, new(big.Int))
}

func TestRegenerateMiningBlockCliqueEIP1559(t *testing.T) {
	testRegenerateMiningBlock(t, eip1559CliqueChainConfig, clique.New(cliqueChainConfig.Clique, rawdb.NewMemoryDatabase()), pendingLegacyAndEIP1559Txs, newLegacyAndEIP1559Txs, new(big.Int))
}

func TestRegenerateMiningBlockEthashEIP1559Finalized(t *testing.T) {
	testRegenerateMiningBlock(t, eip1559FinalizedChainConfig, ethash.NewFaker(), pendingEIP1559Txs, newEIP1559Txs, new(big.Int))
}

func TestRegenerateMiningBlockCliqueEIP1559Finalized(t *testing.T) {
	testRegenerateMiningBlock(t, eip1559FinalizedCliqueChainConfig, clique.New(cliqueChainConfig.Clique, rawdb.NewMemoryDatabase()), pendingEIP1559Txs, newEIP1559Txs, new(big.Int))
}

func testRegenerateMiningBlock(t *testing.T, chainConfig *params.ChainConfig, engine consensus.Engine, pTxs, nTxs []*types.Transaction, baseFee *big.Int) {
	defer engine.Close()

	w, b := newTestWorker(t, chainConfig, engine, rawdb.NewMemoryDatabase(), 0, baseFee, pTxs)
	defer w.close()

	var taskCh = make(chan struct{})

	taskIndex := 0
	w.newTaskHook = func(task *task) {
		if task.block.NumberU64() == 1 {
			// The first task is an empty task, the second
			// one has 1 pending tx, the third one has 2 txs
			if taskIndex == 2 {
				receiptLen, balance := 2, big.NewInt(2000)
				if len(task.receipts) != receiptLen {
					t.Errorf("receipt number mismatch: have %d, want %d", len(task.receipts), receiptLen)
				}
				if task.state.GetBalance(testUserAddress).Cmp(balance) != 0 {
					t.Errorf("account balance mismatch: have %d, want %d", task.state.GetBalance(testUserAddress), balance)
				}
			}
			taskCh <- struct{}{}
			taskIndex += 1
		}
	}
	w.skipSealHook = func(task *task) bool {
		return true
	}
	w.fullTaskHook = func() {
		time.Sleep(100 * time.Millisecond)
	}

	w.start()
	// Ignore the first two works
	for i := 0; i < 2; i += 1 {
		select {
		case <-taskCh:
		case <-time.NewTimer(time.Second).C:
			t.Error("new task timeout")
		}
	}
	b.txPool.AddLocals(nTxs)
	time.Sleep(time.Second)

	select {
	case <-taskCh:
	case <-time.NewTimer(time.Second).C:
		t.Error("new task timeout")
	}
}

func TestAdjustIntervalEthash(t *testing.T) {
	testAdjustInterval(t, ethashChainConfig, ethash.NewFaker(), pendingTxs, nil)
}

func TestAdjustIntervalClique(t *testing.T) {
	testAdjustInterval(t, cliqueChainConfig, clique.New(cliqueChainConfig.Clique, rawdb.NewMemoryDatabase()), pendingTxs, nil)
}

func TestAdjustIntervalEthashEIP1559(t *testing.T) {
	testAdjustInterval(t, eip1559ChainConfig, ethash.NewFaker(), pendingLegacyAndEIP1559Txs, new(big.Int))
}

func TestAdjustIntervalCliqueEIP1559(t *testing.T) {
	testAdjustInterval(t, eip1559CliqueChainConfig, clique.New(cliqueChainConfig.Clique, rawdb.NewMemoryDatabase()), pendingLegacyAndEIP1559Txs, new(big.Int))
}

func TestAdjustIntervalEthashEIP1559Finalized(t *testing.T) {
	testAdjustInterval(t, eip1559FinalizedChainConfig, ethash.NewFaker(), pendingEIP1559Txs, new(big.Int))
}

func TestAdjustIntervalCliqueEIP1559Finalized(t *testing.T) {
	testAdjustInterval(t, eip1559FinalizedCliqueChainConfig, clique.New(cliqueChainConfig.Clique, rawdb.NewMemoryDatabase()), pendingEIP1559Txs, new(big.Int))
}

func testAdjustInterval(t *testing.T, chainConfig *params.ChainConfig, engine consensus.Engine, pTxs []*types.Transaction, baseFee *big.Int) {
	defer engine.Close()

	w, _ := newTestWorker(t, chainConfig, engine, rawdb.NewMemoryDatabase(), 0, baseFee, pTxs)
	defer w.close()

	w.skipSealHook = func(task *task) bool {
		return true
	}
	w.fullTaskHook = func() {
		time.Sleep(100 * time.Millisecond)
	}
	var (
		progress = make(chan struct{}, 10)
		result   = make([]float64, 0, 10)
		index    = 0
		start    uint32
	)
	w.resubmitHook = func(minInterval time.Duration, recommitInterval time.Duration) {
		// Short circuit if interval checking hasn't started.
		if atomic.LoadUint32(&start) == 0 {
			return
		}
		var wantMinInterval, wantRecommitInterval time.Duration

		switch index {
		case 0:
			wantMinInterval, wantRecommitInterval = 3*time.Second, 3*time.Second
		case 1:
			origin := float64(3 * time.Second.Nanoseconds())
			estimate := origin*(1-intervalAdjustRatio) + intervalAdjustRatio*(origin/0.8+intervalAdjustBias)
			wantMinInterval, wantRecommitInterval = 3*time.Second, time.Duration(estimate)*time.Nanosecond
		case 2:
			estimate := result[index-1]
			min := float64(3 * time.Second.Nanoseconds())
			estimate = estimate*(1-intervalAdjustRatio) + intervalAdjustRatio*(min-intervalAdjustBias)
			wantMinInterval, wantRecommitInterval = 3*time.Second, time.Duration(estimate)*time.Nanosecond
		case 3:
			wantMinInterval, wantRecommitInterval = time.Second, time.Second
		}

		// Check interval
		if minInterval != wantMinInterval {
			t.Errorf("resubmit min interval mismatch: have %v, want %v ", minInterval, wantMinInterval)
		}
		if recommitInterval != wantRecommitInterval {
			t.Errorf("resubmit interval mismatch: have %v, want %v", recommitInterval, wantRecommitInterval)
		}
		result = append(result, float64(recommitInterval.Nanoseconds()))
		index += 1
		progress <- struct{}{}
	}
	w.start()

	time.Sleep(time.Second)
	atomic.StoreUint32(&start, 1)

	w.setRecommitInterval(3 * time.Second)
	select {
	case <-progress:
	case <-time.NewTimer(time.Second).C:
		t.Error("interval reset timeout")
	}

	w.resubmitAdjustCh <- &intervalAdjust{inc: true, ratio: 0.8}
	select {
	case <-progress:
	case <-time.NewTimer(time.Second).C:
		t.Error("interval reset timeout")
	}

	w.resubmitAdjustCh <- &intervalAdjust{inc: false}
	select {
	case <-progress:
	case <-time.NewTimer(time.Second).C:
		t.Error("interval reset timeout")
	}

	w.setRecommitInterval(500 * time.Millisecond)
	select {
	case <-progress:
	case <-time.NewTimer(time.Second).C:
		t.Error("interval reset timeout")
	}
}
