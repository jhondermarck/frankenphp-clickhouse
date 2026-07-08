package clickhousephp

/*
#include <stdlib.h>
#include "clickhousephp.h"
*/
import "C"
import (
	_ "runtime/cgo"
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"reflect"
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

const poolSize = 4

var (
	connPool    chan clickhouse.Conn
	connDSN     string
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

// acquireConn gets a connection from the pool, creating one on demand if needed.
func acquireConn() (clickhouse.Conn, error) {
	poolMu.Lock()
	pool := connPool
	dsn := connDSN
	poolMu.Unlock()

	if pool == nil {
		return nil, fmt.Errorf("Client not connected")
	}

	select {
	case c := <-pool:
		return c, nil
	default:
		return connectClickHouse(dsn)
	}
}

// releaseConn returns a connection to the pool, closing it if the pool is full.
func releaseConn(c clickhouse.Conn) {
	poolMu.Lock()
	pool := connPool
	poolMu.Unlock()

	if pool == nil {
		c.Close()
		return
	}
	select {
	case pool <- c:
	default:
		c.Close()
	}
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
	timeout, err := parseDSNTimeout(dsnURL)
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}
	client, err := connectClickHouse(dsnURL)
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}

	poolMu.Lock()
	// Close previous pool if reconnecting
	if connPool != nil {
		old := connPool
		connPool = nil
		poolMu.Unlock()
		close(old)
		for c := range old {
			c.Close()
		}
		poolMu.Lock()
	}
	connPool = make(chan clickhouse.Conn, poolSize)
	connPool <- client
	connDSN = dsnURL
	connTimeout = timeout
	poolMu.Unlock()

	return frankenphp.PHPString("Ok", false)
}

// parseDSNTimeout extracts the optional `timeout` DSN parameter
// (a Go duration, e.g. 30s). Zero means no per-call timeout.
func parseDSNTimeout(dsn string) (time.Duration, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return 0, fmt.Errorf("invalid DSN: %w", err)
	}
	v := parsed.Query().Get("timeout")
	if v == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("invalid timeout %q (use a Go duration, e.g. 30s)", v)
	}
	return d, nil
}

//export clickhouse_disconnect
func clickhouse_disconnect() unsafe.Pointer {
	poolMu.Lock()
	if connPool == nil {
		poolMu.Unlock()
		return frankenphp.PHPString("ERROR: Client not connected", false)
	}
	pool := connPool
	connPool = nil
	connDSN = ""
	connTimeout = 0
	poolMu.Unlock()

	close(pool)
	for c := range pool {
		c.Close()
	}
	return frankenphp.PHPString("Ok", false)
}

//export clickhouse_insert
func clickhouse_insert(table *C.zend_string, values *C.zval, columns *C.zval, options *C.zval) unsafe.Pointer {
	client, err := acquireConn()
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}
	defer releaseConn(client)
	tableName := frankenphp.GoString(unsafe.Pointer(table))
	if !validIdent(tableName) {
		return frankenphp.PHPString(fmt.Sprintf("Insert error: invalid table name %q", tableName), false)
	}

	// Parse columns (optional — may be nil or empty)
	var colNames []string
	if columns != nil {
		colAny, err := frankenphp.GoValue[any](unsafe.Pointer(columns))
		if err != nil {
			return frankenphp.PHPString("Insert error (columns): "+err.Error(), false)
		}
		if colAny != nil {
			colSlice, ok := colAny.([]any)
			if !ok {
				return frankenphp.PHPString("Insert error: columns is not an array", false)
			}
			colNames = make([]string, len(colSlice))
			for i, c := range colSlice {
				s, ok := c.(string)
				if !ok {
					return frankenphp.PHPString("Insert error: column name is not a string", false)
				}
				colNames[i] = s
			}
		}
	}

	// Parse values
	valAny, err := frankenphp.GoValue[any](unsafe.Pointer(values))
	if err != nil {
		return frankenphp.PHPString("Insert error: "+err.Error(), false)
	}
	flat, ok := valAny.([]any)
	if !ok {
		return frankenphp.PHPString("Insert error: values is not an array", false)
	}
	if len(flat) == 0 {
		return frankenphp.PHPString("Ok", false)
	}

	// Detect format: associative rows, nested sequential rows, or flat
	asMap := phpAssoc

	var rows [][]any
	if firstMap, ok := asMap(flat[0]); ok {
		// Associative rows: [['id' => 1, 'name' => 'foo'], ...]
		if len(colNames) == 0 {
			colNames = make([]string, 0, len(firstMap))
			for k := range firstMap {
				colNames = append(colNames, k)
			}
			sort.Strings(colNames)
		}
		rows = make([][]any, len(flat))
		for j, item := range flat {
			m, ok := asMap(item)
			if !ok {
				return frankenphp.PHPString(fmt.Sprintf("Insert error: row %d is not an associative array", j), false)
			}
			row := make([]any, len(colNames))
			for ci, col := range colNames {
				row[ci] = m[col]
			}
			rows[j] = row
		}
	} else if first, ok := flat[0].([]any); ok {
		// Nested sequential rows: [[v1, v2], [v3, v4]]
		if len(colNames) == 0 {
			return frankenphp.PHPString("Insert error: columns required for sequential arrays", false)
		}
		rows = make([][]any, len(flat))
		rows[0] = first
		for j := 1; j < len(flat); j++ {
			row, ok := flat[j].([]any)
			if !ok {
				return frankenphp.PHPString(fmt.Sprintf("Insert error: row %d is not an array", j), false)
			}
			rows[j] = row
		}
	} else {
		// Flat mode: [v1, v2, v3, v4, v5, v6]
		if len(colNames) == 0 {
			return frankenphp.PHPString("Insert error: columns required for flat values", false)
		}
		stride := len(colNames)
		if len(flat)%stride != 0 {
			return frankenphp.PHPString(fmt.Sprintf("Insert error: %d values not divisible by %d columns", len(flat), stride), false)
		}
		rows = make([][]any, len(flat)/stride)
		for i := 0; i < len(flat); i += stride {
			rows[i/stride] = flat[i : i+stride]
		}
	}

	// Build INSERT statement with column list — column names are
	// concatenated into SQL, so validate them like the table name.
	insertSQL := "INSERT INTO " + tableName
	if len(colNames) > 0 {
		for _, col := range colNames {
			if !validIdent(col) {
				return frankenphp.PHPString(fmt.Sprintf("Insert error: invalid column name %q", col), false)
			}
		}
		insertSQL += " (" + strings.Join(colNames, ", ") + ")"
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

	stride := len(colNames)
	for i, row := range rows {
		if stride > 0 && len(row) != stride {
			return frankenphp.PHPString(fmt.Sprintf("Insert error: row %d has %d values, expected %d columns", i, len(row), stride), false)
		}
		if err := batch.Append(row...); err != nil {
			return frankenphp.PHPString(fmt.Sprintf("Send error (row %d): %s", i, err.Error()), false)
		}
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
	client, err := acquireConn()
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}
	defer releaseConn(client)
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
	client, err := acquireConn()
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}
	defer releaseConn(client)
	ctx, cancel := callCtx()
	defer cancel()
	if err := client.Ping(ctx); err != nil {
		return frankenphp.PHPString("ERROR: ping failed: "+err.Error(), false)
	}
	return frankenphp.PHPString("Ok", false)
}

//export clickhouse_server_version
func clickhouse_server_version() unsafe.Pointer {
	client, err := acquireConn()
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}
	defer releaseConn(client)
	v, err := client.ServerVersion()
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}
	return frankenphp.PHPString(fmt.Sprintf("%d.%d.%d", v.Version.Major, v.Version.Minor, v.Version.Patch), false)
}

//export clickhouse_query_array
func clickhouse_query_array(query *C.zend_string, params *C.zval, options *C.zval) unsafe.Pointer {
	client, err := acquireConn()
	if err != nil {
		setLastError(err.Error())
		return nil
	}
	queryStr := frankenphp.GoString(unsafe.Pointer(query))

	args, err := buildQueryArgs(params)
	if err != nil {
		releaseConn(client)
		setLastError(err.Error())
		return nil
	}

	ctx, cancel, err := buildCallCtx(options)
	if err != nil {
		releaseConn(client)
		setLastError(err.Error())
		return nil
	}
	defer cancel()
	rows, qerr := client.Query(ctx, queryStr, args...)
	if qerr != nil {
		releaseConn(client)
		setLastError(qerr.Error())
		return nil
	}
	defer func() {
		rows.Close()
		releaseConn(client)
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

// ── Streaming cursors ─────────────────────────────────────────────────────────

// cursorState is one open streaming query. The pooled connection is held
// for the cursor's lifetime and released at exhaustion or close.
type cursorState struct {
	mu     sync.Mutex
	conn   clickhouse.Conn
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

// releaseResources closes the stream and returns the connection to the
// pool. Callers must hold cur.mu.
func (cur *cursorState) releaseResources() {
	if cur.done {
		return
	}
	cur.done = true
	cur.rows.Close()
	cur.cancel()
	releaseConn(cur.conn)
}

//export clickhouse_query_cursor
func clickhouse_query_cursor(query *C.zend_string, params *C.zval, options *C.zval) C.int64_t {
	client, err := acquireConn()
	if err != nil {
		setLastError(err.Error())
		return -1
	}
	queryStr := frankenphp.GoString(unsafe.Pointer(query))

	args, err := buildQueryArgs(params)
	if err != nil {
		releaseConn(client)
		setLastError(err.Error())
		return -1
	}

	// The context must outlive this call — it is cancelled when the
	// cursor is exhausted or closed.
	ctx, cancel, err := buildCallCtx(options)
	if err != nil {
		releaseConn(client)
		setLastError(err.Error())
		return -1
	}
	rows, err := client.Query(ctx, queryStr, args...)
	if err != nil {
		cancel()
		releaseConn(client)
		setLastError(err.Error())
		return -1
	}
	packer, err := newRowPacker(rows)
	if err != nil {
		rows.Close()
		cancel()
		releaseConn(client)
		setLastError(err.Error())
		return -1
	}

	cur := &cursorState{conn: client, rows: rows, cancel: cancel, packer: packer}
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

func connectClickHouse(dsn string) (clickhouse.Conn, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid DSN: %w", err)
	}

	address := parsed.Host
	if address == "" {
		return nil, fmt.Errorf("DSN missing host")
	}

	database := strings.TrimPrefix(parsed.Path, "/")
	if database == "" {
		database = "default"
	}

	username := "default"
	password := ""
	if parsed.User != nil {
		username = parsed.User.Username()
		if pw, ok := parsed.User.Password(); ok {
			password = pw
		}
	}

	tlsConfig := (*tls.Config)(nil)
	if parsed.Query().Get("secure") == "true" {
		skip := parsed.Query().Get("skip_verify") == "true"
		tlsConfig = &tls.Config{InsecureSkipVerify: skip}
	}

	compression := &clickhouse.Compression{Method: clickhouse.CompressionLZ4}
	if parsed.Query().Get("compress") == "false" {
		compression = nil
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{address},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		TLS:         tlsConfig,
		Compression: compression,
	})
	if err != nil {
		return nil, fmt.Errorf("open failed: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping failed: %w", err)
	}

	return conn, nil
}
