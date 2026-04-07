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

	colAny, err := frankenphp.GoValue[any](unsafe.Pointer(columns))
	if err != nil {
		return frankenphp.PHPString("Insert error (columns): "+err.Error(), false)
	}
	colSlice, ok := colAny.([]any)
	if !ok {
		return frankenphp.PHPString("Insert error: columns is not an array", false)
	}
	stride := len(colSlice)
	if stride == 0 {
		return frankenphp.PHPString("Insert error: empty columns", false)
	}

	valAny, err := frankenphp.GoValue[any](unsafe.Pointer(values))
	if err != nil {
		return frankenphp.PHPString("Insert error: "+err.Error(), false)
	}
	flat, ok := valAny.([]any)
	if !ok {
		return frankenphp.PHPString("Insert error: values is not an array", false)
	}
	if len(flat)%stride != 0 {
		return frankenphp.PHPString(fmt.Sprintf("Insert error: %d values not divisible by %d columns", len(flat), stride), false)
	}

	// Build column list for the INSERT statement
	colNames := make([]string, stride)
	for i, c := range colSlice {
		s, ok := c.(string)
		if !ok {
			return frankenphp.PHPString("Insert error: column name is not a string", false)
		}
		colNames[i] = s
	}

	ctx := context.Background()
	batch, err := client.PrepareBatch(ctx, "INSERT INTO "+tableName+" ("+strings.Join(colNames, ", ")+")")
	if err != nil {
		return frankenphp.PHPString("Send error: "+err.Error(), false)
	}
	defer batch.Close()

	for i := 0; i < len(flat); i += stride {
		if err := batch.Append(flat[i : i+stride]...); err != nil {
			return frankenphp.PHPString(fmt.Sprintf("Send error (row %d): %s", i/stride, err.Error()), false)
		}
	}
	if err := batch.Send(); err != nil {
		return frankenphp.PHPString("Send error: "+err.Error(), false)
	}
	return frankenphp.PHPString("Ok", false)
}

//export clickhouse_exec
func clickhouse_exec(query *C.zend_string) unsafe.Pointer {
	client, err := acquireConn()
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}
	defer releaseConn(client)
	queryStr := frankenphp.GoString(unsafe.Pointer(query))
	err = client.Exec(context.Background(), queryStr)
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}
	return frankenphp.PHPString("Ok", false)
}

//export clickhouse_query_array
func clickhouse_query_array(query *C.zend_string) unsafe.Pointer {
	client, err := acquireConn()
	if err != nil {
		setLastError(err.Error())
		return nil
	}
	queryStr := frankenphp.GoString(unsafe.Pointer(query))
	rows, qerr := client.Query(context.Background(), queryStr)
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
