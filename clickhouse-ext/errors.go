package clickhousephp

/*
#include <stdlib.h>
#include "clickhousephp.h"
*/
import "C"
import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
	"unsafe"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/dunglas/frankenphp"
)

var (
	lastError     string
	lastErrorCode int32 // ClickHouse server error code, 0 if none
	lastErrorMu   sync.Mutex
)

// chErrorCode extracts the ClickHouse server error code, 0 if the
// error isn't a server exception.
func chErrorCode(err error) int32 {
	var ex *clickhouse.Exception
	if errors.As(err, &ex) {
		return ex.Code
	}
	return 0
}

// chError renders a driver error through the ERROR string protocol; a
// server error code is encoded as ERROR[code]: and surfaces as the
// RuntimeException code in the C bridge.
func chError(prefix string, err error) unsafe.Pointer {
	if code := chErrorCode(err); code != 0 {
		return frankenphp.PHPString(fmt.Sprintf("ERROR[%d]: %s%s", code, prefix, err.Error()), false)
	}
	return frankenphp.PHPString("ERROR: "+prefix+err.Error(), false)
}

// phpAssoc unwraps a PHP associative array GoValue into a Go map
// (frankenphp yields either map[string]any or AssociativeArray).
func phpAssoc(v any) (map[string]any, bool) {
	switch m := v.(type) {
	case map[string]any:
		return m, true
	case frankenphp.AssociativeArray[any]:
		return m.Map, true
	default:
		return nil, false
	}
}

// callSetup resolves the target connection and builds the context for
// one call from the optional options array:
//   - connection: handle from clickhouse_open() (default: the global one)
//   - settings:   map of ClickHouse query settings (max_execution_time…)
//   - query_id:   tag the query for system.query_log / KILL QUERY
//   - timeout:    Go duration overriding the connection's DSN timeout
//
// inheritConnTimeout applies the connection's DSN-level timeout when no
// explicit options.timeout is given. One-shot calls pass true; handle
// factories (cursors, batches) pass false — their context spans the
// handle's whole lifetime, and a DSN timeout meant for single queries
// would kill a long-lived export or incremental insert mid-flight.
func callSetup(options *C.zval, inheritConnTimeout bool) (clickhouse.Conn, context.Context, context.CancelFunc, error) {
	noop := func() {}

	var optMap map[string]any
	if options != nil {
		optAny, err := frankenphp.GoValue[any](unsafe.Pointer(options))
		if err != nil {
			return nil, nil, noop, fmt.Errorf("options: %s", err)
		}
		if optAny != nil {
			if m, ok := phpAssoc(optAny); ok {
				optMap = m
			} else if s, isSlice := optAny.([]any); !isSlice || len(s) != 0 {
				return nil, nil, noop, fmt.Errorf("options must be an associative array")
			}
		}
	}

	connID := int64(0)
	timeoutOverride := time.Duration(-1)
	var chOpts []clickhouse.QueryOption
	for k, v := range optMap {
		switch k {
		case "connection":
			n, ok := v.(int64)
			if !ok {
				return nil, nil, noop, fmt.Errorf("options.connection must be a handle from clickhouse_open()")
			}
			connID = n
		case "timeout":
			s, ok := v.(string)
			if !ok {
				return nil, nil, noop, fmt.Errorf("options.timeout must be a duration string (e.g. \"30s\")")
			}
			d, err := time.ParseDuration(s)
			if err != nil || d < 0 {
				return nil, nil, noop, fmt.Errorf("invalid options.timeout %q (use a Go duration, e.g. 30s)", s)
			}
			timeoutOverride = d
		case "query_id":
			s, ok := v.(string)
			if !ok || s == "" {
				return nil, nil, noop, fmt.Errorf("options.query_id must be a non-empty string")
			}
			chOpts = append(chOpts, clickhouse.WithQueryID(s))
		case "settings":
			sm, ok := phpAssoc(v)
			if !ok {
				return nil, nil, noop, fmt.Errorf("options.settings must be an associative array")
			}
			settings := clickhouse.Settings{}
			for sk, sv := range sm {
				settings[sk] = sv
			}
			chOpts = append(chOpts, clickhouse.WithSettings(settings))
		default:
			return nil, nil, noop, fmt.Errorf("unknown option %q (supported: connection, settings, query_id, timeout)", k)
		}
	}

	conn, timeout, err := resolveConn(connID)
	if err != nil {
		return nil, nil, noop, err
	}
	if !inheritConnTimeout {
		timeout = 0
	}
	if timeoutOverride >= 0 {
		timeout = timeoutOverride
	}

	ctx := context.Background()
	cancel := noop
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	}
	if len(chOpts) > 0 {
		ctx = clickhouse.Context(ctx, chOpts...)
	}
	return conn, ctx, cancel, nil
}

// ── Panic guards ──────────────────────────────────────────────────────────────
// A Go panic in an exported function would kill the whole FrankenPHP
// process; these convert it into a RuntimeException instead.

func phpPanicGuard(ret *unsafe.Pointer) {
	if r := recover(); r != nil {
		*ret = frankenphp.PHPString(fmt.Sprintf("ERROR: internal panic: %v", r), false)
	}
}

func nullPanicGuard(ret *unsafe.Pointer) {
	if r := recover(); r != nil {
		setLastError(fmt.Sprintf("internal panic: %v", r))
		*ret = nil
	}
}

func idPanicGuard(ret *C.int64_t) {
	if r := recover(); r != nil {
		setLastError(fmt.Sprintf("internal panic: %v", r))
		*ret = -1
	}
}

func setLastError(msg string) {
	lastErrorMu.Lock()
	lastError = msg
	lastErrorCode = 0
	lastErrorMu.Unlock()
}

// setChError records a driver error with its ClickHouse server code.
func setChError(prefix string, err error) {
	lastErrorMu.Lock()
	lastError = prefix + err.Error()
	lastErrorCode = chErrorCode(err)
	lastErrorMu.Unlock()
}

//export clickhouse_get_last_error_code
func clickhouse_get_last_error_code() C.int64_t {
	lastErrorMu.Lock()
	defer lastErrorMu.Unlock()
	return C.int64_t(lastErrorCode)
}

//export clickhouse_get_last_error
func clickhouse_get_last_error() unsafe.Pointer {
	lastErrorMu.Lock()
	err := lastError
	lastError = ""
	lastErrorCode = 0
	lastErrorMu.Unlock()
	if err == "" {
		return nil
	}
	return frankenphp.PHPString(err, false)
}
