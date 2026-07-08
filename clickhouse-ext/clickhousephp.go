package clickhousephp

/*
#include <stdlib.h>
#include "clickhousephp.h"
*/
import "C"
import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"os"
	"reflect"
	_ "runtime/cgo"
	"sort"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
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
	lastError   string
	lastErrorMu sync.Mutex
)

// callCtx returns the context for one ClickHouse call, honouring the
// `timeout` DSN parameter — without it a hung query blocks a PHP
// worker thread forever.
func callCtx() (context.Context, context.CancelFunc) {
	poolMu.Lock()
	d := connTimeout
	poolMu.Unlock()
	if d > 0 {
		return context.WithTimeout(context.Background(), d)
	}
	return context.Background(), func() {}
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

// buildCallCtx returns the context for one call, applying the optional
// per-call options array:
//   - settings: map of ClickHouse query settings (max_execution_time…)
//   - query_id: tag the query for system.query_log / KILL QUERY
//   - timeout:  Go duration overriding the DSN-level timeout
func buildCallCtx(options *C.zval) (context.Context, context.CancelFunc, error) {
	noop := func() {}

	var optMap map[string]any
	if options != nil {
		optAny, err := frankenphp.GoValue[any](unsafe.Pointer(options))
		if err != nil {
			return nil, noop, fmt.Errorf("options: %s", err)
		}
		if optAny != nil {
			if m, ok := phpAssoc(optAny); ok {
				optMap = m
			} else if s, isSlice := optAny.([]any); !isSlice || len(s) != 0 {
				return nil, noop, fmt.Errorf("options must be an associative array")
			}
		}
	}

	poolMu.Lock()
	timeout := connTimeout
	poolMu.Unlock()

	var chOpts []clickhouse.QueryOption
	for k, v := range optMap {
		switch k {
		case "timeout":
			s, ok := v.(string)
			if !ok {
				return nil, noop, fmt.Errorf("options.timeout must be a duration string (e.g. \"30s\")")
			}
			d, err := time.ParseDuration(s)
			if err != nil || d < 0 {
				return nil, noop, fmt.Errorf("invalid options.timeout %q (use a Go duration, e.g. 30s)", s)
			}
			timeout = d
		case "query_id":
			s, ok := v.(string)
			if !ok || s == "" {
				return nil, noop, fmt.Errorf("options.query_id must be a non-empty string")
			}
			chOpts = append(chOpts, clickhouse.WithQueryID(s))
		case "settings":
			sm, ok := phpAssoc(v)
			if !ok {
				return nil, noop, fmt.Errorf("options.settings must be an associative array")
			}
			settings := clickhouse.Settings{}
			for sk, sv := range sm {
				settings[sk] = sv
			}
			chOpts = append(chOpts, clickhouse.WithSettings(settings))
		default:
			return nil, noop, fmt.Errorf("unknown option %q (supported: settings, query_id, timeout)", k)
		}
	}

	ctx := context.Background()
	cancel := noop
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	}
	if len(chOpts) > 0 {
		ctx = clickhouse.Context(ctx, chOpts...)
	}
	return ctx, cancel, nil
}

// validIdent accepts plain or database-qualified identifiers
// ([A-Za-z0-9_.]) — insert concatenates table and column names into
// SQL, so anything else is rejected rather than escaped.
func validIdent(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '.':
		default:
			return false
		}
	}
	return true
}

// getConn snapshots the driver pool; per-query connection checkout is
// handled inside the driver.
func getConn() (clickhouse.Conn, error) {
	poolMu.Lock()
	defer poolMu.Unlock()
	if pool == nil {
		return nil, fmt.Errorf("Client not connected")
	}
	return pool, nil
}

func setLastError(msg string) {
	lastErrorMu.Lock()
	lastError = msg
	lastErrorMu.Unlock()
}

//export clickhouse_get_last_error
func clickhouse_get_last_error() unsafe.Pointer {
	lastErrorMu.Lock()
	err := lastError
	lastError = ""
	lastErrorMu.Unlock()
	if err == "" {
		return nil
	}
	return frankenphp.PHPString(err, false)
}

//export clickhouse_connect
func clickhouse_connect(dsn *C.zend_string) unsafe.Pointer {
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
	// Closing the previous pool outside the lock; its open cursors and
	// batches fail on their next operation, same as before.
	if old != nil {
		old.Close()
	}
	return frankenphp.PHPString("Ok", false)
}

//export clickhouse_disconnect
func clickhouse_disconnect() unsafe.Pointer {
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

// parseColumnNames extracts an optional PHP column list.
func parseColumnNames(columns *C.zval) ([]string, error) {
	if columns == nil {
		return nil, nil
	}
	colAny, err := frankenphp.GoValue[any](unsafe.Pointer(columns))
	if err != nil {
		return nil, fmt.Errorf("columns: %s", err)
	}
	if colAny == nil {
		return nil, nil
	}
	colSlice, ok := colAny.([]any)
	if !ok {
		return nil, fmt.Errorf("columns is not an array")
	}
	colNames := make([]string, len(colSlice))
	for i, c := range colSlice {
		s, ok := c.(string)
		if !ok {
			return nil, fmt.Errorf("column name is not a string")
		}
		colNames[i] = s
	}
	return colNames, nil
}

// buildInsertSQL validates identifiers (they are concatenated into SQL)
// and assembles the INSERT statement.
func buildInsertSQL(tableName string, colNames []string) (string, error) {
	if !validIdent(tableName) {
		return "", fmt.Errorf("invalid table name %q", tableName)
	}
	sql := "INSERT INTO " + tableName
	if len(colNames) > 0 {
		for _, col := range colNames {
			if !validIdent(col) {
				return "", fmt.Errorf("invalid column name %q", col)
			}
		}
		sql += " (" + strings.Join(colNames, ", ") + ")"
	}
	return sql, nil
}

// normalizeRows converts PHP values — associative rows, nested
// sequential rows, or a flat array with stride — into row slices.
// When inferCols is true and colNames is empty, associative rows
// populate colNames (sorted); nested rows without columns are allowed
// only when requireColsForNested is false (incremental batches, where
// the INSERT statement is already fixed).
func normalizeRows(values *C.zval, colNames []string, inferCols, requireColsForNested bool) ([][]any, []string, error) {
	valAny, err := frankenphp.GoValue[any](unsafe.Pointer(values))
	if err != nil {
		return nil, colNames, fmt.Errorf("%s", err)
	}
	flat, ok := valAny.([]any)
	if !ok {
		if _, isMap := phpAssoc(valAny); isMap {
			// A single associative row: wrap it.
			flat = []any{valAny}
		} else {
			return nil, colNames, fmt.Errorf("values is not an array")
		}
	}
	if len(flat) == 0 {
		return nil, colNames, nil
	}

	var rows [][]any
	if firstMap, ok := phpAssoc(flat[0]); ok {
		// Associative rows: [['id' => 1, 'name' => 'foo'], ...]
		if len(colNames) == 0 {
			if !inferCols {
				return nil, colNames, fmt.Errorf("columns required for associative rows (declare them at batch begin)")
			}
			colNames = make([]string, 0, len(firstMap))
			for k := range firstMap {
				colNames = append(colNames, k)
			}
			sort.Strings(colNames)
		}
		rows = make([][]any, len(flat))
		for j, item := range flat {
			m, ok := phpAssoc(item)
			if !ok {
				return nil, colNames, fmt.Errorf("row %d is not an associative array", j)
			}
			row := make([]any, len(colNames))
			for ci, col := range colNames {
				row[ci] = m[col]
			}
			rows[j] = row
		}
	} else if first, ok := flat[0].([]any); ok {
		// Nested sequential rows: [[v1, v2], [v3, v4]]
		if len(colNames) == 0 && requireColsForNested {
			return nil, colNames, fmt.Errorf("columns required for sequential arrays")
		}
		rows = make([][]any, len(flat))
		rows[0] = first
		for j := 1; j < len(flat); j++ {
			row, ok := flat[j].([]any)
			if !ok {
				return nil, colNames, fmt.Errorf("row %d is not an array", j)
			}
			rows[j] = row
		}
	} else {
		// Flat mode: [v1, v2, v3, v4, v5, v6]
		if len(colNames) == 0 {
			return nil, colNames, fmt.Errorf("columns required for flat values")
		}
		stride := len(colNames)
		if len(flat)%stride != 0 {
			return nil, colNames, fmt.Errorf("%d values not divisible by %d columns", len(flat), stride)
		}
		rows = make([][]any, len(flat)/stride)
		for i := 0; i < len(flat); i += stride {
			rows[i/stride] = flat[i : i+stride]
		}
	}
	return rows, colNames, nil
}

// appendRows pushes normalized rows into a driver batch.
func appendRows(batch driver.Batch, rows [][]any, stride int) error {
	for i, row := range rows {
		if stride > 0 && len(row) != stride {
			return fmt.Errorf("row %d has %d values, expected %d columns", i, len(row), stride)
		}
		if err := batch.Append(row...); err != nil {
			return fmt.Errorf("row %d: %s", i, err)
		}
	}
	return nil
}

//export clickhouse_insert
func clickhouse_insert(table *C.zend_string, values *C.zval, columns *C.zval, options *C.zval) unsafe.Pointer {
	client, err := getConn()
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}
	tableName := frankenphp.GoString(unsafe.Pointer(table))

	colNames, err := parseColumnNames(columns)
	if err != nil {
		return frankenphp.PHPString("Insert error: "+err.Error(), false)
	}
	rows, colNames, err := normalizeRows(values, colNames, true, true)
	if err != nil {
		return frankenphp.PHPString("Insert error: "+err.Error(), false)
	}
	if len(rows) == 0 {
		return frankenphp.PHPString("Ok", false)
	}
	insertSQL, err := buildInsertSQL(tableName, colNames)
	if err != nil {
		return frankenphp.PHPString("Insert error: "+err.Error(), false)
	}

	ctx, cancel, err := buildCallCtx(options)
	if err != nil {
		return frankenphp.PHPString("Insert error: "+err.Error(), false)
	}
	defer cancel()
	batch, err := client.PrepareBatch(ctx, insertSQL)
	if err != nil {
		return frankenphp.PHPString("Send error: "+err.Error(), false)
	}
	defer batch.Close()

	if err := appendRows(batch, rows, len(colNames)); err != nil {
		return frankenphp.PHPString("Send error: "+err.Error(), false)
	}
	if err := batch.Send(); err != nil {
		return frankenphp.PHPString("Send error: "+err.Error(), false)
	}
	return frankenphp.PHPString("Ok", false)
}

// buildQueryArgs converts a PHP params zval to clickhouse-go query arguments.
// Associative array → clickhouse.Named params, sequential → positional args.
func buildQueryArgs(params *C.zval) ([]any, error) {
	if params == nil {
		return nil, nil
	}
	val, err := frankenphp.GoValue[any](unsafe.Pointer(params))
	if err != nil {
		return nil, fmt.Errorf("params: %w", err)
	}
	if val == nil {
		return nil, nil
	}

	switch v := val.(type) {
	case []any:
		if len(v) == 0 {
			return nil, nil
		}
		return v, nil
	case map[string]any:
		if len(v) == 0 {
			return nil, nil
		}
		args := make([]any, 0, len(v))
		for key, value := range v {
			args = append(args, clickhouse.Named(key, value))
		}
		return args, nil
	case frankenphp.AssociativeArray[any]:
		if len(v.Map) == 0 {
			return nil, nil
		}
		args := make([]any, 0, len(v.Map))
		for key, value := range v.Map {
			args = append(args, clickhouse.Named(key, value))
		}
		return args, nil
	default:
		return nil, fmt.Errorf("params must be an array, got %T", val)
	}
}

//export clickhouse_exec
func clickhouse_exec(query *C.zend_string, params *C.zval, options *C.zval) unsafe.Pointer {
	client, err := getConn()
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}
	queryStr := frankenphp.GoString(unsafe.Pointer(query))

	args, err := buildQueryArgs(params)
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}

	ctx, cancel, err := buildCallCtx(options)
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}
	defer cancel()
	err = client.Exec(ctx, queryStr, args...)
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}
	return frankenphp.PHPString("Ok", false)
}

//export clickhouse_ping
func clickhouse_ping() unsafe.Pointer {
	client, err := getConn()
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}
	ctx, cancel := callCtx()
	defer cancel()
	if err := client.Ping(ctx); err != nil {
		return frankenphp.PHPString("ERROR: ping failed: "+err.Error(), false)
	}
	return frankenphp.PHPString("Ok", false)
}

//export clickhouse_server_version
func clickhouse_server_version() unsafe.Pointer {
	client, err := getConn()
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}
	v, err := client.ServerVersion()
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}
	return frankenphp.PHPString(fmt.Sprintf("%d.%d.%d", v.Version.Major, v.Version.Minor, v.Version.Patch), false)
}

//export clickhouse_query_array
func clickhouse_query_array(query *C.zend_string, params *C.zval, options *C.zval) unsafe.Pointer {
	client, err := getConn()
	if err != nil {
		setLastError(err.Error())
		return nil
	}
	queryStr := frankenphp.GoString(unsafe.Pointer(query))

	args, err := buildQueryArgs(params)
	if err != nil {
		setLastError(err.Error())
		return nil
	}

	ctx, cancel, err := buildCallCtx(options)
	if err != nil {
		setLastError(err.Error())
		return nil
	}
	defer cancel()
	rows, qerr := client.Query(ctx, queryStr, args...)
	if qerr != nil {
		setLastError(qerr.Error())
		return nil
	}
	defer func() {
		rows.Close()
	}()

	packer, err := newRowPacker(rows)
	if err != nil {
		setLastError(err.Error())
		return nil
	}
	if packer == nil { // zero-column result
		return newResultArray(0)
	}
	result, _, err := packer.packRows(rows, 0)
	if err != nil {
		setLastError(err.Error())
		return nil
	}
	return result
}

// ── Row packer (shared by query_array and streaming cursors) ─────────────────

// rowPacker holds the per-query column metadata and scan destinations,
// reused across packRows calls on the same result stream.
type rowPacker struct {
	cols  []string
	metas []colMeta
	dests []interface{}
}

// newRowPacker parses column metadata for a result stream. A nil packer
// with a nil error means the result has no columns.
func newRowPacker(rows driver.Rows) (*rowPacker, error) {
	cols := rows.Columns()
	colTypes := rows.ColumnTypes()
	n := len(cols)
	if n == 0 {
		return nil, nil
	}
	metas := make([]colMeta, n)
	dests := make([]interface{}, n)
	for i, ct := range colTypes {
		m, err := parseColMeta(ct.DatabaseTypeName())
		if err != nil {
			return nil, fmt.Errorf("column %q: %s", cols[i], err)
		}
		metas[i] = m
		dests[i] = allocScanDest(m)
		if dests[i] == nil {
			// Complex shapes (Map, nested arrays) scan into the driver's
			// native type and are converted generically via reflection.
			dests[i] = reflect.New(ct.ScanType()).Interface()
		}
	}
	return &rowPacker{cols: cols, metas: metas, dests: dests}, nil
}

// packRows drains up to max rows (max <= 0 → all remaining) into a fresh
// result array. Returns the array, whether the stream is exhausted, and
// any error — the partial array is freed before an error return.
func (p *rowPacker) packRows(rows driver.Rows, max int64) (unsafe.Pointer, bool, error) {
	n := len(p.cols)
	keys := make([]*C.zend_string, n)
	for i, col := range p.cols {
		keys[i] = makeKey(col)
	}
	// Each row array takes its own reference on the key strings; drop
	// ours once the result is built (or abandoned on error).
	defer func() {
		for _, k := range keys {
			releaseKey(k)
		}
	}()

	types := make([]C.uint8_t, n)
	soff := make([]C.uint32_t, n)
	slen := make([]C.uint32_t, n)
	ivals := make([]C.int64_t, n)
	uvals := make([]C.uint64_t, n)
	fvals := make([]C.double, n)
	sbuf := make([]byte, 0, n*64)

	// Start empty and let the packed hashtable grow — preallocating for a
	// large result wastes several MB on every small query in worker mode.
	result := newResultArray(0)

	var count int64
	done := false
	for {
		if max > 0 && count >= max {
			break
		}
		if !rows.Next() {
			done = true
			break
		}
		// Reset nullable pointers so the driver can set them to nil for NULL
		for i, m := range p.metas {
			if m.nullable {
				resetNullableDest(m.kind, p.dests[i])
			}
		}
		if err := rows.Scan(p.dests...); err != nil {
			freeResultArray(result)
			return nil, false, fmt.Errorf("scan failed: %s", err)
		}
		sbuf = sbuf[:0]
		for i, m := range p.metas {
			packCol(i, m, p.dests[i], types, soff, slen, ivals, uvals, fvals, &sbuf)
		}
		addGenericRow(result, keys, types, sbuf, soff, slen, ivals, uvals, fvals, n)
		count++
	}
	// A mid-stream failure (network cut, server error) ends the Next() loop
	// without error on Scan — surface it instead of returning truncated rows.
	if done {
		if err := rows.Err(); err != nil {
			freeResultArray(result)
			return nil, false, fmt.Errorf("query interrupted: %s", err)
		}
	}
	return result, done, nil
}

// ── Incremental insert batches ────────────────────────────────────────────────

// batchState is one open incremental insert; the driver socket is
// returned to the driver pool at send or abort.
type batchState struct {
	mu     sync.Mutex
	batch  driver.Batch
	cancel context.CancelFunc
	cols   []string
	done   bool
}

var (
	batchesMu sync.Mutex
	batches   = map[int64]*batchState{}
	batchSeq  int64
)

// releaseResources cancels the batch context. Callers must hold b.mu.
func (b *batchState) releaseResources() {
	if b.done {
		return
	}
	b.done = true
	b.cancel()
}

func getBatch(id int64) *batchState {
	batchesMu.Lock()
	defer batchesMu.Unlock()
	return batches[id]
}

func dropBatch(id int64) {
	batchesMu.Lock()
	delete(batches, id)
	batchesMu.Unlock()
}

//export clickhouse_batch_begin
func clickhouse_batch_begin(table *C.zend_string, columns *C.zval, options *C.zval) C.int64_t {
	client, err := getConn()
	if err != nil {
		setLastError(err.Error())
		return -1
	}
	tableName := frankenphp.GoString(unsafe.Pointer(table))

	colNames, err := parseColumnNames(columns)
	if err != nil {
		setLastError(err.Error())
		return -1
	}
	insertSQL, err := buildInsertSQL(tableName, colNames)
	if err != nil {
		setLastError(err.Error())
		return -1
	}
	// The context must outlive this call — it is cancelled at send/abort.
	ctx, cancel, err := buildCallCtx(options)
	if err != nil {
		setLastError(err.Error())
		return -1
	}
	batch, err := client.PrepareBatch(ctx, insertSQL)
	if err != nil {
		cancel()
		setLastError(err.Error())
		return -1
	}

	b := &batchState{batch: batch, cancel: cancel, cols: colNames}
	batchesMu.Lock()
	batchSeq++
	id := batchSeq
	batches[id] = b
	batchesMu.Unlock()
	return C.int64_t(id)
}

//export clickhouse_batch_append
func clickhouse_batch_append(id C.int64_t, values *C.zval) unsafe.Pointer {
	b := getBatch(int64(id))
	if b == nil {
		return frankenphp.PHPString(fmt.Sprintf("ERROR: unknown batch %d — already sent, aborted, or never opened", int64(id)), false)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done {
		return frankenphp.PHPString(fmt.Sprintf("ERROR: batch %d is closed", int64(id)), false)
	}

	rows, _, err := normalizeRows(values, b.cols, false, false)
	if err != nil {
		return frankenphp.PHPString("ERROR: append: "+err.Error(), false)
	}
	if err := appendRows(b.batch, rows, len(b.cols)); err != nil {
		return frankenphp.PHPString("ERROR: append: "+err.Error(), false)
	}
	return frankenphp.PHPString("Ok", false)
}

//export clickhouse_batch_flush
func clickhouse_batch_flush(id C.int64_t) unsafe.Pointer {
	b := getBatch(int64(id))
	if b == nil {
		return frankenphp.PHPString(fmt.Sprintf("ERROR: unknown batch %d", int64(id)), false)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done {
		return frankenphp.PHPString(fmt.Sprintf("ERROR: batch %d is closed", int64(id)), false)
	}
	if err := b.batch.Flush(); err != nil {
		// A failed flush leaves the stream in an undefined state — close it.
		b.releaseResources()
		dropBatch(int64(id))
		return frankenphp.PHPString("ERROR: flush: "+err.Error(), false)
	}
	return frankenphp.PHPString("Ok", false)
}

//export clickhouse_batch_send
func clickhouse_batch_send(id C.int64_t) unsafe.Pointer {
	b := getBatch(int64(id))
	if b == nil {
		return frankenphp.PHPString(fmt.Sprintf("ERROR: unknown batch %d", int64(id)), false)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done {
		return frankenphp.PHPString(fmt.Sprintf("ERROR: batch %d is closed", int64(id)), false)
	}
	err := b.batch.Send()
	b.releaseResources()
	dropBatch(int64(id))
	if err != nil {
		return frankenphp.PHPString("ERROR: send: "+err.Error(), false)
	}
	return frankenphp.PHPString("Ok", false)
}

//export clickhouse_batch_abort
func clickhouse_batch_abort(id C.int64_t) unsafe.Pointer {
	b := getBatch(int64(id))
	if b == nil {
		return frankenphp.PHPString(fmt.Sprintf("ERROR: unknown batch %d", int64(id)), false)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done {
		return frankenphp.PHPString(fmt.Sprintf("ERROR: batch %d is closed", int64(id)), false)
	}
	_ = b.batch.Abort()
	b.releaseResources()
	dropBatch(int64(id))
	return frankenphp.PHPString("Ok", false)
}

//export clickhouse_async_insert
func clickhouse_async_insert(query *C.zend_string, wait C.int, params *C.zval, options *C.zval) unsafe.Pointer {
	client, err := getConn()
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}
	queryStr := frankenphp.GoString(unsafe.Pointer(query))

	args, err := buildQueryArgs(params)
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}
	ctx, cancel, err := buildCallCtx(options)
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}
	defer cancel()
	if err := client.AsyncInsert(ctx, queryStr, wait != 0, args...); err != nil {
		return frankenphp.PHPString("ERROR: async insert: "+err.Error(), false)
	}
	return frankenphp.PHPString("Ok", false)
}

// ── Streaming cursors ─────────────────────────────────────────────────────────

// cursorState is one open streaming query; the driver socket it holds
// is returned to the driver pool when rows.Close() runs at exhaustion
// or close.
type cursorState struct {
	mu     sync.Mutex
	rows   driver.Rows
	cancel context.CancelFunc
	packer *rowPacker
	done   bool
}

var (
	cursorsMu sync.Mutex
	cursors   = map[int64]*cursorState{}
	cursorSeq int64
)

// releaseResources closes the stream. Callers must hold cur.mu.
func (cur *cursorState) releaseResources() {
	if cur.done {
		return
	}
	cur.done = true
	cur.rows.Close()
	cur.cancel()
}

//export clickhouse_query_cursor
func clickhouse_query_cursor(query *C.zend_string, params *C.zval, options *C.zval) C.int64_t {
	client, err := getConn()
	if err != nil {
		setLastError(err.Error())
		return -1
	}
	queryStr := frankenphp.GoString(unsafe.Pointer(query))

	args, err := buildQueryArgs(params)
	if err != nil {
		setLastError(err.Error())
		return -1
	}

	// The context must outlive this call — it is cancelled when the
	// cursor is exhausted or closed.
	ctx, cancel, err := buildCallCtx(options)
	if err != nil {
		setLastError(err.Error())
		return -1
	}
	rows, err := client.Query(ctx, queryStr, args...)
	if err != nil {
		cancel()
		setLastError(err.Error())
		return -1
	}
	packer, err := newRowPacker(rows)
	if err != nil {
		rows.Close()
		cancel()
		setLastError(err.Error())
		return -1
	}

	cur := &cursorState{rows: rows, cancel: cancel, packer: packer}
	if packer == nil { // zero-column result: nothing to stream
		cur.releaseResources()
	}
	cursorsMu.Lock()
	cursorSeq++
	id := cursorSeq
	cursors[id] = cur
	cursorsMu.Unlock()
	return C.int64_t(id)
}

//export clickhouse_cursor_fetch
func clickhouse_cursor_fetch(id C.int64_t, maxRows C.int64_t) unsafe.Pointer {
	cursorsMu.Lock()
	cur, ok := cursors[int64(id)]
	cursorsMu.Unlock()
	if !ok {
		setLastError(fmt.Sprintf("unknown cursor %d — already closed or never opened", int64(id)))
		return nil
	}

	cur.mu.Lock()
	defer cur.mu.Unlock()
	if cur.done {
		return newResultArray(0)
	}

	result, done, err := cur.packer.packRows(cur.rows, int64(maxRows))
	if err != nil {
		cur.releaseResources()
		cursorsMu.Lock()
		delete(cursors, int64(id))
		cursorsMu.Unlock()
		setLastError(err.Error())
		return nil
	}
	if done {
		// Free the stream eagerly; the cursor stays registered so later
		// fetches return empty arrays until clickhouse_cursor_close().
		cur.releaseResources()
	}
	return result
}

//export clickhouse_cursor_close
func clickhouse_cursor_close(id C.int64_t) unsafe.Pointer {
	cursorsMu.Lock()
	cur, ok := cursors[int64(id)]
	delete(cursors, int64(id))
	cursorsMu.Unlock()
	if !ok {
		return frankenphp.PHPString(fmt.Sprintf("ERROR: unknown cursor %d", int64(id)), false)
	}
	cur.mu.Lock()
	cur.releaseResources()
	cur.mu.Unlock()
	return frankenphp.PHPString("Ok", false)
}

// connectClickHouse builds a driver pool from the DSN. Driver-native
// parameters pass straight through clickhouse.ParseDSN: multi-host
// ("h1:9000,h2:9000"), connection_open_strategy (in_order/round_robin/
// random), max_open_conns, max_idle_conns, conn_max_lifetime,
// compress (lz4/zstd/gzip/none), secure, skip_verify, dial_timeout,
// read_timeout… Any other unknown parameter becomes a ClickHouse query
// setting (driver behavior), so extension-level parameters below are
// stripped first:
//
//	timeout      per-call timeout (Go duration)
//	ca_cert      path to a PEM CA bundle (implies TLS)
//	client_cert  path to a PEM client certificate (mutual TLS)
//	client_key   path to the matching client key
func connectClickHouse(dsn string) (clickhouse.Conn, time.Duration, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, 0, fmt.Errorf("invalid DSN: %w", err)
	}
	q := u.Query()

	timeout := time.Duration(0)
	if v := q.Get("timeout"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			return nil, 0, fmt.Errorf("invalid timeout %q (use a Go duration, e.g. 30s)", v)
		}
		timeout = d
	}
	caCert := q.Get("ca_cert")
	clientCert := q.Get("client_cert")
	clientKey := q.Get("client_key")
	for _, k := range []string{"timeout", "ca_cert", "client_cert", "client_key"} {
		q.Del(k)
	}
	u.RawQuery = q.Encode()

	opts, err := clickhouse.ParseDSN(u.String())
	if err != nil {
		return nil, 0, fmt.Errorf("invalid DSN: %w", err)
	}
	// Preserve the historic default: LZ4 on the native protocol unless
	// explicitly disabled (ParseDSN leaves compression off when the
	// param is absent; compress=false/none disables it).
	if opts.Compression == nil {
		opts.Compression = &clickhouse.Compression{Method: clickhouse.CompressionLZ4}
	}

	if caCert != "" || clientCert != "" || clientKey != "" {
		if opts.TLS == nil {
			opts.TLS = &tls.Config{}
		}
		if caCert != "" {
			pem, err := os.ReadFile(caCert)
			if err != nil {
				return nil, 0, fmt.Errorf("ca_cert: %w", err)
			}
			roots := x509.NewCertPool()
			if !roots.AppendCertsFromPEM(pem) {
				return nil, 0, fmt.Errorf("ca_cert: no PEM certificates in %s", caCert)
			}
			opts.TLS.RootCAs = roots
		}
		if clientCert != "" || clientKey != "" {
			if clientCert == "" || clientKey == "" {
				return nil, 0, fmt.Errorf("client_cert and client_key must both be set")
			}
			cert, err := tls.LoadX509KeyPair(clientCert, clientKey)
			if err != nil {
				return nil, 0, fmt.Errorf("client certificate: %w", err)
			}
			opts.TLS.Certificates = []tls.Certificate{cert}
		}
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, 0, fmt.Errorf("open failed: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return nil, 0, fmt.Errorf("ping failed: %w", err)
	}

	return conn, timeout, nil
}
