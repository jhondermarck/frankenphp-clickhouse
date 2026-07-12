package clickhousephp

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// ── Runtime observability (clickhouse_stats) ─────────────────────────────────
//
// In worker mode a FrankenPHP process is long-lived, so leaked cursors/batches
// and a saturated driver pool accumulate silently across requests. These
// process-wide counters and gauges give PHP a cheap snapshot for health checks
// and leak diagnosis without touching the ClickHouse server. Counters are
// bumped with sync/atomic from the exported functions; the snapshot is
// assembled in clickhouse_stats (clickhousearray.go, where the C array helpers
// live — cgo statics aren't shared across files).

var (
	statQueries       int64 // clickhouse_query_array calls
	statInserts       int64 // clickhouse_insert calls
	statExecs         int64 // clickhouse_exec calls
	statAsyncInserts  int64 // clickhouse_async_insert calls
	statCursorsOpened int64 // clickhouse_query_cursor calls
	statBatchesOpened int64 // clickhouse_batch_begin calls
	statErrors        int64 // errors surfaced to PHP via the error channel

	statLastReapUnix  int64 // unix time of the reaper's most recent sweep
	statLastReapCount int64 // handles reaped in that sweep

	statTimedOps        int64 // operations whose latency was recorded
	statQueryDurationUs int64 // summed operation latency (microseconds)
	statQueryMaxUs      int64 // slowest single operation (microseconds)

	processStart = time.Now()

	serverVerMu sync.Mutex
	serverVer   string // cached "major.minor.patch" of the default connection
)

// cacheServerVersion records the default connection's server version so
// clickhouse_stats can report it without a round-trip. ServerVersion() reads
// the value captured during the handshake — no network I/O.
func cacheServerVersion(conn clickhouse.Conn) {
	if conn == nil {
		return
	}
	if v, err := conn.ServerVersion(); err == nil {
		serverVerMu.Lock()
		serverVer = fmt.Sprintf("%d.%d.%d", v.Version.Major, v.Version.Minor, v.Version.Patch)
		serverVerMu.Unlock()
	}
}

// recordQueryDuration accumulates one operation's wall-clock latency into the
// process-wide timing aggregates (lock-free). Reported by clickhouse_stats so
// a worker can expose average and worst-case query latency.
func recordQueryDuration(d time.Duration) {
	us := d.Microseconds()
	atomic.AddInt64(&statTimedOps, 1)
	atomic.AddInt64(&statQueryDurationUs, us)
	for {
		cur := atomic.LoadInt64(&statQueryMaxUs)
		if us <= cur || atomic.CompareAndSwapInt64(&statQueryMaxUs, cur, us) {
			break
		}
	}
}

func boolToInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
