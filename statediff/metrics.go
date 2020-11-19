package statediff

import (
	"strings"

	"github.com/ethereum/go-ethereum/metrics"
)

const (
	namespace = "statediff"
)

// Build a fully qualified metric name
func metricName(subsystem, name string) string {
	if name == "" {
		return ""
	}
	parts := []string{namespace, name}
	if subsystem != "" {
		parts = []string{namespace, subsystem, name}
	}
	// Prometheus uses _ but geth metrics uses / and replaces
	return strings.Join(parts, "/")
}

type statediffMetricsHandles struct {
	// Height of latest synced by core.BlockChain
	// FIXME
	lastSyncHeight metrics.Gauge
	// Height of the latest block received from chainEvent channel
	lastEventHeight metrics.Gauge
	// Height of latest state diff
	lastStatediffHeight metrics.Gauge
	// Current length of chainEvent channels
	serviceLoopChannelLen metrics.Gauge
	writeLoopChannelLen   metrics.Gauge
}

func RegisterStatediffMetrics(reg metrics.Registry) statediffMetricsHandles {
	ctx := statediffMetricsHandles{
		lastSyncHeight:        metrics.NewGauge(),
		lastEventHeight:       metrics.NewGauge(),
		lastStatediffHeight:   metrics.NewGauge(),
		serviceLoopChannelLen: metrics.NewGauge(),
		writeLoopChannelLen:   metrics.NewGauge(),
	}
	subsys := "" // todo
	reg.Register(metricName(subsys, "last_sync_height"), ctx.lastSyncHeight)
	reg.Register(metricName(subsys, "last_event_height"), ctx.lastEventHeight)
	reg.Register(metricName(subsys, "last_statediff_height"), ctx.lastStatediffHeight)
	reg.Register(metricName(subsys, "service_loop_channel_len"), ctx.serviceLoopChannelLen)
	reg.Register(metricName(subsys, "write_loop_channel_len"), ctx.writeLoopChannelLen)
	return ctx
}
