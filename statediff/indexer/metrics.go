package indexer

import (
	"database/sql"
	"strings"

	"github.com/ethereum/go-ethereum/metrics"
)

const (
	indexerNamespace = "indexer"
)

// Build a fully qualified metric name
func metricName(subsystem, name string) string {
	if name == "" {
		return ""
	}
	parts := []string{indexerNamespace, name}
	if subsystem != "" {
		parts = []string{indexerNamespace, subsystem, name}
	}
	// Prometheus uses _ but geth metrics uses / and replaces
	return strings.Join(parts, "/")
}

type indexerMetricsHandles struct {
	// The total number of processed blocks
	blocks metrics.Counter
	// The total number of processed transactions
	transactions metrics.Counter
	// The total number of processed receipts
	receipts metrics.Counter
	// Time spent waiting for free postgres tx
	tFreePostgres metrics.Timer
	// Postgres transaction commit duration
	tPostgresCommit metrics.Timer
	// Header processing time
	tHeaderProcessing metrics.Timer
	// Uncle processing time
	tUncleProcessing metrics.Timer
	// Tx and receipt processing time
	tTxAndRecProcessing metrics.Timer
	// State, storage, and code combined processing time
	tStateStoreCodeProcessing metrics.Timer
}

func RegisterIndexerMetrics(reg metrics.Registry) indexerMetricsHandles {
	ctx := indexerMetricsHandles{
		blocks:                    metrics.NewCounter(),
		transactions:              metrics.NewCounter(),
		receipts:                  metrics.NewCounter(),
		tFreePostgres:             metrics.NewTimer(),
		tPostgresCommit:           metrics.NewTimer(),
		tHeaderProcessing:         metrics.NewTimer(),
		tUncleProcessing:          metrics.NewTimer(),
		tTxAndRecProcessing:       metrics.NewTimer(),
		tStateStoreCodeProcessing: metrics.NewTimer(),
	}
	subsys := "" // todo
	reg.Register(metricName(subsys, "blocks"), ctx.blocks)
	reg.Register(metricName(subsys, "transactions"), ctx.transactions)
	reg.Register(metricName(subsys, "receipts"), ctx.receipts)
	reg.Register(metricName(subsys, "t_free_postgres"), ctx.tFreePostgres)
	reg.Register(metricName(subsys, "t_postgres_commit"), ctx.tPostgresCommit)
	reg.Register(metricName(subsys, "t_header_processing"), ctx.tHeaderProcessing)
	reg.Register(metricName(subsys, "t_uncle_processing"), ctx.tUncleProcessing)
	reg.Register(metricName(subsys, "t_tx_receipt_processing"), ctx.tTxAndRecProcessing)
	reg.Register(metricName(subsys, "t_state_store_code_processing"), ctx.tStateStoreCodeProcessing)
	return ctx
}

type dbMetrics struct {
	// Maximum number of open connections to the database
	maxOpen metrics.Gauge
	// The number of established connections both in use and idle
	open metrics.Gauge
	// The number of connections currently in use
	inUse metrics.Gauge
	// The number of idle connections
	idle metrics.Gauge
	// The total number of connections waited for
	waitedFor metrics.Counter
	// The total time blocked waiting for a new connection
	blockedSeconds metrics.Counter
	// The total number of connections closed due to SetMaxIdleConns
	closedMaxIdle metrics.Counter
	// The total number of connections closed due to SetConnMaxLifetime
	closedMaxLifetime metrics.Counter
}

func RegisterDBMetrics(reg metrics.Registry) dbMetrics {
	ctx := dbMetrics{
		maxOpen:           metrics.NewGauge(),
		open:              metrics.NewGauge(),
		inUse:             metrics.NewGauge(),
		idle:              metrics.NewGauge(),
		waitedFor:         metrics.NewCounter(),
		blockedSeconds:    metrics.NewCounter(),
		closedMaxIdle:     metrics.NewCounter(),
		closedMaxLifetime: metrics.NewCounter(),
	}
	subsys := "connections"
	reg.Register(metricName(subsys, "max_open_desc"), ctx.maxOpen)
	reg.Register(metricName(subsys, "open_desc"), ctx.open)
	reg.Register(metricName(subsys, "in_use_desc"), ctx.inUse)
	reg.Register(metricName(subsys, "idle_desc"), ctx.idle)
	reg.Register(metricName(subsys, "waited_for_desc"), ctx.waitedFor)
	reg.Register(metricName(subsys, "blocked_seconds_desc"), ctx.blockedSeconds)
	reg.Register(metricName(subsys, "closed_max_idle_desc"), ctx.closedMaxIdle)
	reg.Register(metricName(subsys, "closed_max_lifetime_desc"), ctx.closedMaxLifetime)
	return ctx
}

func (met *dbMetrics) Update(stats sql.DBStats) {
	met.maxOpen.Update(int64(stats.MaxOpenConnections))
	met.open.Update(int64(stats.OpenConnections))
	met.inUse.Update(int64(stats.InUse))
	met.idle.Update(int64(stats.Idle))
	met.waitedFor.Inc(int64(stats.WaitCount))
	met.blockedSeconds.Inc(int64(stats.WaitDuration.Seconds()))
	met.closedMaxIdle.Inc(int64(stats.MaxIdleClosed))
	met.closedMaxLifetime.Inc(int64(stats.MaxLifetimeClosed))
}
