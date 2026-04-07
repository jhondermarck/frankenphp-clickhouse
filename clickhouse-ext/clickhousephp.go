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
	"sort"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/dunglas/frankenphp"
)

func init() {
	frankenphp.RegisterExtension(unsafe.Pointer(&C.clickhousephp_module_entry))
}

const poolSize = 4

var (
	connPool    chan clickhouse.Conn
	connDSN     string
	poolMu      sync.Mutex
	lastError   string
	lastErrorMu sync.Mutex
)

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
	poolMu.Unlock()

	return frankenphp.PHPString("Ok", false)
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
	poolMu.Unlock()

	close(pool)
	for c := range pool {
		c.Close()
	}
	return frankenphp.PHPString("Ok", false)
}

//export clickhouse_insert
func clickhouse_insert(table *C.zend_string, values *C.zval, columns *C.zval) unsafe.Pointer {
	client, err := acquireConn()
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}
	defer releaseConn(client)
	tableName := frankenphp.GoString(unsafe.Pointer(table))

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
	// Helper: extract map from any (handles both map[string]any and AssociativeArray)
	asMap := func(v any) (map[string]any, bool) {
		switch m := v.(type) {
		case map[string]any:
			return m, true
		case frankenphp.AssociativeArray[any]:
			return m.Map, true
		default:
			return nil, false
		}
	}

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

	// Build INSERT statement with column list
	insertSQL := "INSERT INTO " + tableName
	if len(colNames) > 0 {
		insertSQL += " (" + strings.Join(colNames, ", ") + ")"
	}

	ctx := context.Background()
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
func clickhouse_exec(query *C.zend_string, params *C.zval) unsafe.Pointer {
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

	err = client.Exec(context.Background(), queryStr, args...)
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}
	return frankenphp.PHPString("Ok", false)
}

//export clickhouse_query_array
func clickhouse_query_array(query *C.zend_string, params *C.zval) unsafe.Pointer {
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

	rows, qerr := client.Query(context.Background(), queryStr, args...)
	if qerr != nil {
		releaseConn(client)
		setLastError(qerr.Error())
		return nil
	}
	defer func() {
		rows.Close()
		releaseConn(client)
	}()

	cols := rows.Columns()
	colTypes := rows.ColumnTypes()
	n := len(cols)
	if n == 0 {
		return newResultArray(0)
	}

	metas := make([]colMeta, n)
	dests := make([]interface{}, n)
	for i, ct := range colTypes {
		m, err := parseColMeta(ct.DatabaseTypeName())
		if err != nil {
			return newResultArray(0)
		}
		metas[i] = m
		dests[i] = allocScanDest(m)
	}
	keys := make([]*C.zend_string, n)
	for i, col := range cols {
		keys[i] = internKey(col)
	}

	types := make([]C.uint8_t, n)
	soff  := make([]C.uint32_t, n)
	slen  := make([]C.uint32_t, n)
	ivals := make([]C.int64_t, n)
	uvals := make([]C.uint64_t, n)
	fvals := make([]C.double, n)
	sbuf  := make([]byte, 0, n*64)

	result := newResultArray(160000)

	for rows.Next() {
		// Reset nullable pointers so the driver can set them to nil for NULL
		for i, m := range metas {
			if m.nullable {
				resetNullableDest(m.kind, dests[i])
			}
		}
		if err := rows.Scan(dests...); err != nil {
			return result
		}
		sbuf = sbuf[:0]
		for i, m := range metas {
			packCol(i, m, dests[i], types, soff, slen, ivals, uvals, fvals, &sbuf)
		}
		addGenericRow(result, keys, types, sbuf, soff, slen, ivals, uvals, fvals, n)
	}

	return result
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
