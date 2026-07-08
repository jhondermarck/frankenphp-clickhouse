package clickhousephp

/*
#include <stdlib.h>
#include "clickhousephp.h"
*/
import "C"
import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"
	"unsafe"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/dunglas/frankenphp"
)

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
			return nil, false, fmt.Errorf("scan failed: %w", err)
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
			return nil, false, fmt.Errorf("query interrupted: %w", err)
		}
	}
	return result, done, nil
}

// ── Idle-handle reaper ────────────────────────────────────────────────────────

// A PHP script that dies without closing a cursor or batch pins a
// driver socket for the process lifetime — FrankenPHP workers never
// restart between requests, and PHP has no destructor hook into this
// extension. Handles idle longer than handleIdleTTL are reaped; a later
// use of a reaped handle gets the usual "unknown cursor/batch" error.
const handleIdleTTL = 10 * time.Minute

func init() {
	go func() {
		for {
			time.Sleep(time.Minute)
			reapIdleHandles(time.Now())
		}
	}()
}

func reapIdleHandles(now time.Time) (reaped int) {
	cutoff := now.Add(-handleIdleTTL)

	cursorsMu.Lock()
	curSnapshot := make(map[int64]*cursorState, len(cursors))
	for id, cur := range cursors {
		curSnapshot[id] = cur
	}
	cursorsMu.Unlock()
	for id, cur := range curSnapshot {
		cur.mu.Lock()
		idle := cur.lastUsed.Before(cutoff)
		if idle {
			cur.releaseResources()
		}
		cur.mu.Unlock()
		if idle {
			cursorsMu.Lock()
			delete(cursors, id)
			cursorsMu.Unlock()
			reaped++
		}
	}

	batchesMu.Lock()
	bSnapshot := make(map[int64]*batchState, len(batches))
	for id, b := range batches {
		bSnapshot[id] = b
	}
	batchesMu.Unlock()
	for id, b := range bSnapshot {
		b.mu.Lock()
		idle := b.lastUsed.Before(cutoff)
		if idle && !b.done {
			_ = b.batch.Abort()
			b.releaseResources()
		}
		b.mu.Unlock()
		if idle {
			batchesMu.Lock()
			delete(batches, id)
			batchesMu.Unlock()
			reaped++
		}
	}
	return reaped
}

// ── Incremental insert batches ────────────────────────────────────────────────

// batchState is one open incremental insert; the driver socket is
// returned to the driver pool at send or abort.
type batchState struct {
	mu       sync.Mutex
	batch    driver.Batch
	cancel   context.CancelFunc
	cols     []string
	done     bool
	lastUsed time.Time
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
func clickhouse_batch_begin(table *C.zend_string, columns *C.zval, options *C.zval) (ret C.int64_t) {
	defer idPanicGuard(&ret)
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
	client, ctx, cancel, err := callSetup(options, false)
	if err != nil {
		setLastError(err.Error())
		return -1
	}
	batch, err := client.PrepareBatch(ctx, insertSQL)
	if err != nil {
		cancel()
		setChError("", err)
		return -1
	}

	b := &batchState{batch: batch, cancel: cancel, cols: colNames, lastUsed: time.Now()}
	batchesMu.Lock()
	batchSeq++
	id := batchSeq
	batches[id] = b
	batchesMu.Unlock()
	return C.int64_t(id)
}

//export clickhouse_batch_append
func clickhouse_batch_append(id C.int64_t, values *C.zval) (ret unsafe.Pointer) {
	defer phpPanicGuard(&ret)
	b := getBatch(int64(id))
	if b == nil {
		return frankenphp.PHPString(fmt.Sprintf("ERROR: unknown batch %d — already sent, aborted, or never opened", int64(id)), false)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastUsed = time.Now()
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
func clickhouse_batch_flush(id C.int64_t) (ret unsafe.Pointer) {
	defer phpPanicGuard(&ret)
	b := getBatch(int64(id))
	if b == nil {
		return frankenphp.PHPString(fmt.Sprintf("ERROR: unknown batch %d", int64(id)), false)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastUsed = time.Now()
	if b.done {
		return frankenphp.PHPString(fmt.Sprintf("ERROR: batch %d is closed", int64(id)), false)
	}
	if err := b.batch.Flush(); err != nil {
		// A failed flush leaves the stream in an undefined state — close it.
		b.releaseResources()
		dropBatch(int64(id))
		return chError("flush: ", err)
	}
	return frankenphp.PHPString("Ok", false)
}

//export clickhouse_batch_send
func clickhouse_batch_send(id C.int64_t) (ret unsafe.Pointer) {
	defer phpPanicGuard(&ret)
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
		return chError("send: ", err)
	}
	return frankenphp.PHPString("Ok", false)
}

//export clickhouse_batch_abort
func clickhouse_batch_abort(id C.int64_t) (ret unsafe.Pointer) {
	defer phpPanicGuard(&ret)
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

// ── Streaming cursors ─────────────────────────────────────────────────────────

// cursorState is one open streaming query; the driver socket it holds
// is returned to the driver pool when rows.Close() runs at exhaustion
// or close.
type cursorState struct {
	mu       sync.Mutex
	rows     driver.Rows
	cancel   context.CancelFunc
	packer   *rowPacker
	done     bool
	lastUsed time.Time
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
func clickhouse_query_cursor(query *C.zend_string, params *C.zval, options *C.zval) (ret C.int64_t) {
	defer idPanicGuard(&ret)
	queryStr := frankenphp.GoString(unsafe.Pointer(query))

	args, err := buildQueryArgs(params)
	if err != nil {
		setLastError(err.Error())
		return -1
	}

	// The context must outlive this call — it is cancelled when the
	// cursor is exhausted or closed.
	client, ctx, cancel, err := callSetup(options, false)
	if err != nil {
		setLastError(err.Error())
		return -1
	}
	rows, err := client.Query(ctx, queryStr, args...)
	if err != nil {
		cancel()
		setChError("", err)
		return -1
	}
	packer, err := newRowPacker(rows)
	if err != nil {
		rows.Close()
		cancel()
		setLastError(err.Error())
		return -1
	}

	cur := &cursorState{rows: rows, cancel: cancel, packer: packer, lastUsed: time.Now()}
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
func clickhouse_cursor_fetch(id C.int64_t, maxRows C.int64_t) (ret unsafe.Pointer) {
	defer nullPanicGuard(&ret)
	cursorsMu.Lock()
	cur, ok := cursors[int64(id)]
	cursorsMu.Unlock()
	if !ok {
		setLastError(fmt.Sprintf("unknown cursor %d — already closed or never opened", int64(id)))
		return nil
	}

	cur.mu.Lock()
	defer cur.mu.Unlock()
	cur.lastUsed = time.Now()
	if cur.done {
		return newResultArray(0)
	}

	result, done, err := cur.packer.packRows(cur.rows, int64(maxRows))
	if err != nil {
		cur.releaseResources()
		cursorsMu.Lock()
		delete(cursors, int64(id))
		cursorsMu.Unlock()
		setChError("", err)
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
func clickhouse_cursor_close(id C.int64_t) (ret unsafe.Pointer) {
	defer phpPanicGuard(&ret)
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
