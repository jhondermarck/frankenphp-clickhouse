package clickhousephp

/*
#include <stdlib.h>
#include "clickhousephp.h"
*/
import "C"
import (
	_ "runtime/cgo"
	"sync"
	"time"
	"unsafe"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/dunglas/frankenphp"
)

func init() {
	frankenphp.RegisterExtension(unsafe.Pointer(&C.clickhousephp_module_entry))
}

// The driver's clickhouse.Conn is itself a thread-safe connection pool
// (max_open_conns / max_idle_conns / conn_max_lifetime are DSN params).
// One instance serves every PHP thread; poolMu only guards the swap on
// connect/disconnect.
var (
	pool        clickhouse.Conn
	connTimeout time.Duration // per-call timeout from the DSN (0 = none)
	poolMu      sync.Mutex
)

//export clickhouse_connect
func clickhouse_connect(dsn *C.zend_string) (ret unsafe.Pointer) {
	defer phpPanicGuard(&ret)
	dsnURL := frankenphp.GoString(unsafe.Pointer(dsn))
	conn, timeout, err := connectClickHouse(dsnURL)
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}

	poolMu.Lock()
	old := pool
	pool = conn
	connTimeout = timeout
	poolMu.Unlock()
	cacheServerVersion(conn)
	// Closing the previous pool outside the lock; its open cursors and
	// batches fail on their next operation, same as before.
	if old != nil {
		old.Close()
	}
	return frankenphp.PHPString("Ok", false)
}

//export clickhouse_disconnect
func clickhouse_disconnect() (ret unsafe.Pointer) {
	defer phpPanicGuard(&ret)
	poolMu.Lock()
	old := pool
	pool = nil
	connTimeout = 0
	poolMu.Unlock()
	if old == nil {
		return frankenphp.PHPString("ERROR: Client not connected", false)
	}
	old.Close()
	return frankenphp.PHPString("Ok", false)
}
