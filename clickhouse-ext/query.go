package clickhousephp

/*
#include <stdlib.h>
#include "clickhousephp.h"
*/
import "C"
import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unsafe"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/dunglas/frankenphp"
)

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

// phpValue unwraps frankenphp container types into plain Go values —
// used when no target column type is known (query parameters).
func phpValue(v any) any {
	switch t := v.(type) {
	case frankenphp.AssociativeArray[any]:
		m := make(map[string]any, len(t.Map))
		for k, val := range t.Map {
			m[k] = phpValue(val)
		}
		return m
	case map[string]any:
		for k, val := range t {
			t[k] = phpValue(val)
		}
		return t
	case []any:
		for i, e := range t {
			t[i] = phpValue(e)
		}
		return t
	default:
		return v
	}
}

// phpValueTyped converts a PHP cell into the concrete Go type the driver
// expects for a column. The driver's Map/Array columns reject the generic
// map[string]any / []any that PHP arrays decode to (e.g. Map(String,String)
// needs map[string]string), so we rebuild the container against the
// column's ScanType. t may be nil (unknown type) — then we fall back to
// phpValue's plain unwrapping and let the driver convert scalars.
func phpValueTyped(v any, t reflect.Type) any {
	var assoc map[string]any
	switch tv := v.(type) {
	case frankenphp.AssociativeArray[any]:
		assoc = tv.Map
	case map[string]any:
		assoc = tv
	}
	if assoc != nil {
		if t != nil && t.Kind() == reflect.Map {
			m := reflect.MakeMapWithSize(t, len(assoc))
			kt, vt := t.Key(), t.Elem()
			for k, val := range assoc {
				kv := coerce(k, kt)
				vv := coerce(phpValueTyped(val, vt), vt)
				if kv.IsValid() && vv.IsValid() {
					m.SetMapIndex(kv, vv)
				}
			}
			return m.Interface()
		}
		out := make(map[string]any, len(assoc))
		for k, val := range assoc {
			out[k] = phpValue(val)
		}
		return out
	}
	if arr, ok := v.([]any); ok {
		if t != nil && t.Kind() == reflect.Slice {
			et := t.Elem()
			s := reflect.MakeSlice(t, len(arr), len(arr))
			for i, e := range arr {
				ev := coerce(phpValueTyped(e, et), et)
				if ev.IsValid() {
					s.Index(i).Set(ev)
				}
			}
			return s.Interface()
		}
		for i, e := range arr {
			arr[i] = phpValue(e)
		}
		return arr
	}
	return v
}

// coerce returns v as a reflect.Value of type t, converting numeric/string
// kinds where possible. Returns the zero Value if the conversion can't be
// done so the caller can skip rather than panic.
func coerce(v any, t reflect.Type) reflect.Value {
	// Nullable columns scan into a pointer type (Map(K,Nullable(V)) →
	// map[K]*V, Array(Nullable(V)) → []*V). A nil cell is a typed nil
	// pointer; a present cell is coerced to the element type and boxed.
	// Without this every nullable element would fail the conversion below
	// and be silently dropped.
	if t.Kind() == reflect.Pointer {
		if v == nil {
			return reflect.Zero(t)
		}
		ev := coerce(v, t.Elem())
		if !ev.IsValid() {
			return reflect.Value{}
		}
		p := reflect.New(t.Elem())
		p.Elem().Set(ev)
		return p
	}
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return reflect.Value{}
	}
	if rv.Type() == t || rv.Type().AssignableTo(t) {
		return rv
	}
	// PHP map keys arrive as strings; a numeric key column needs parsing.
	if rv.Kind() == reflect.String && t.Kind() != reflect.String {
		switch t.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if n, err := strconv.ParseInt(rv.String(), 10, 64); err == nil {
				return reflect.ValueOf(n).Convert(t)
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if n, err := strconv.ParseUint(rv.String(), 10, 64); err == nil {
				return reflect.ValueOf(n).Convert(t)
			}
		}
	}
	if rv.Type().ConvertibleTo(t) {
		return rv.Convert(t)
	}
	return reflect.Value{}
}

// appendRows pushes normalized rows into a driver batch, converting each
// cell to the concrete Go type its column expects.
func appendRows(batch driver.Batch, rows [][]any, stride int) error {
	cols := batch.Columns()
	for i, row := range rows {
		if stride > 0 && len(row) != stride {
			return fmt.Errorf("row %d has %d values, expected %d columns", i, len(row), stride)
		}
		for ci, cell := range row {
			var t reflect.Type
			if ci < len(cols) {
				t = cols[ci].ScanType()
			}
			row[ci] = phpValueTyped(cell, t)
		}
		if err := batch.Append(row...); err != nil {
			return fmt.Errorf("row %d: %w", i, err)
		}
	}
	return nil
}

//export clickhouse_insert
func clickhouse_insert(table *C.zend_string, values *C.zval, columns *C.zval, options *C.zval) (ret unsafe.Pointer) {
	defer phpPanicGuard(&ret)
	client, ctx, cancel, err := callSetup(options, true)
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}
	defer cancel()
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

	batch, err := client.PrepareBatch(ctx, insertSQL)
	if err != nil {
		return chError("insert prepare: ", err)
	}
	defer batch.Close()

	if err := appendRows(batch, rows, len(colNames)); err != nil {
		return chError("insert ", err)
	}
	if err := batch.Send(); err != nil {
		return chError("insert send: ", err)
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
		for i, e := range v {
			v[i] = phpValue(e)
		}
		return v, nil
	case map[string]any:
		if len(v) == 0 {
			return nil, nil
		}
		args := make([]any, 0, len(v))
		for key, value := range v {
			args = append(args, clickhouse.Named(key, phpValue(value)))
		}
		return args, nil
	case frankenphp.AssociativeArray[any]:
		if len(v.Map) == 0 {
			return nil, nil
		}
		args := make([]any, 0, len(v.Map))
		for key, value := range v.Map {
			args = append(args, clickhouse.Named(key, phpValue(value)))
		}
		return args, nil
	default:
		return nil, fmt.Errorf("params must be an array, got %T", val)
	}
}

//export clickhouse_exec
func clickhouse_exec(query *C.zend_string, params *C.zval, options *C.zval) (ret unsafe.Pointer) {
	defer phpPanicGuard(&ret)
	client, ctx, cancel, err := callSetup(options, true)
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}
	defer cancel()
	queryStr := frankenphp.GoString(unsafe.Pointer(query))

	args, err := buildQueryArgs(params)
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}
	if err := client.Exec(ctx, queryStr, args...); err != nil {
		return chError("", err)
	}
	return frankenphp.PHPString("Ok", false)
}

//export clickhouse_query_array
func clickhouse_query_array(query *C.zend_string, params *C.zval, options *C.zval) (ret unsafe.Pointer) {
	defer nullPanicGuard(&ret)
	client, ctx, cancel, err := callSetup(options, true)
	if err != nil {
		setLastError(err.Error())
		return nil
	}
	defer cancel()
	queryStr := frankenphp.GoString(unsafe.Pointer(query))

	args, err := buildQueryArgs(params)
	if err != nil {
		setLastError(err.Error())
		return nil
	}

	rows, qerr := client.Query(ctx, queryStr, args...)
	if qerr != nil {
		setChError("", qerr)
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
		setChError("", err)
		return nil
	}
	return result
}

//export clickhouse_async_insert
func clickhouse_async_insert(query *C.zend_string, wait C.int, params *C.zval, options *C.zval) (ret unsafe.Pointer) {
	defer phpPanicGuard(&ret)
	client, ctx, cancel, err := callSetup(options, true)
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}
	defer cancel()
	queryStr := frankenphp.GoString(unsafe.Pointer(query))

	args, err := buildQueryArgs(params)
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}
	if err := client.AsyncInsert(ctx, queryStr, wait != 0, args...); err != nil {
		return chError("async insert: ", err)
	}
	return frankenphp.PHPString("Ok", false)
}
