package clickhousephp

/*
#include <php.h>

// ── Generic query array ───────────────────────────────────────────────────────

// Column value type tags
#define CH_NULL   0  // SQL NULL
#define CH_STR    1  // string / DateTime (ISO string)
#define CH_INT    2  // int8..int64  → PHP int
#define CH_UINT   3  // uint8..uint64 → PHP int or float
#define CH_FLOAT  4  // float32/float64 → PHP float
#define CH_ARRAY  5  // nested zend_array* (pointer passed via uvals)

// ── Array element helpers ─────────────────────────────────────────────────────

static void ch_arr_add_str(zend_array* arr, const char* s, uint32_t len) {
    zval z; ZVAL_STR(&z, zend_string_init(s, len, 0));
    zend_hash_next_index_insert(arr, &z);
}

static void ch_arr_add_long(zend_array* arr, int64_t v) {
    zval z; ZVAL_LONG(&z, (zend_long)v);
    zend_hash_next_index_insert(arr, &z);
}

static void ch_arr_add_ulong(zend_array* arr, uint64_t v) {
    zval z;
    if (v <= (uint64_t)ZEND_LONG_MAX) ZVAL_LONG(&z, (zend_long)v);
    else ZVAL_DOUBLE(&z, (double)v);
    zend_hash_next_index_insert(arr, &z);
}

static void ch_arr_add_double(zend_array* arr, double v) {
    zval z; ZVAL_DOUBLE(&z, v);
    zend_hash_next_index_insert(arr, &z);
}

static void ch_arr_add_null(zend_array* arr) {
    zval z; ZVAL_NULL(&z);
    zend_hash_next_index_insert(arr, &z);
}

static void ch_arr_add_arr(zend_array* arr, zend_array* v) {
    zval z; ZVAL_ARR(&z, v);
    zend_hash_next_index_insert(arr, &z);
}

// ── Map (keyed) helpers ──────────────────────────────────────────────────────
// String keys go through zend_symtable_str_update so numeric strings get
// PHP's usual int-key coercion; integer keys use zend_hash_index_update.

static void ch_kv_add_str(zend_array* m, const char* k, size_t klen, const char* s, uint32_t len) {
    zval z; ZVAL_STR(&z, zend_string_init(s, len, 0));
    zend_symtable_str_update(m, k, klen, &z);
}
static void ch_kv_add_long(zend_array* m, const char* k, size_t klen, int64_t v) {
    zval z; ZVAL_LONG(&z, (zend_long)v);
    zend_symtable_str_update(m, k, klen, &z);
}
static void ch_kv_add_ulong(zend_array* m, const char* k, size_t klen, uint64_t v) {
    zval z;
    if (v <= (uint64_t)ZEND_LONG_MAX) ZVAL_LONG(&z, (zend_long)v);
    else ZVAL_DOUBLE(&z, (double)v);
    zend_symtable_str_update(m, k, klen, &z);
}
static void ch_kv_add_double(zend_array* m, const char* k, size_t klen, double v) {
    zval z; ZVAL_DOUBLE(&z, v);
    zend_symtable_str_update(m, k, klen, &z);
}
static void ch_kv_add_null(zend_array* m, const char* k, size_t klen) {
    zval z; ZVAL_NULL(&z);
    zend_symtable_str_update(m, k, klen, &z);
}
static void ch_kv_add_arr(zend_array* m, const char* k, size_t klen, zend_array* v) {
    zval z; ZVAL_ARR(&z, v);
    zend_symtable_str_update(m, k, klen, &z);
}

static void ch_kvi_add_str(zend_array* m, int64_t k, const char* s, uint32_t len) {
    zval z; ZVAL_STR(&z, zend_string_init(s, len, 0));
    zend_hash_index_update(m, (zend_ulong)k, &z);
}
static void ch_kvi_add_long(zend_array* m, int64_t k, int64_t v) {
    zval z; ZVAL_LONG(&z, (zend_long)v);
    zend_hash_index_update(m, (zend_ulong)k, &z);
}
static void ch_kvi_add_ulong(zend_array* m, int64_t k, uint64_t v) {
    zval z;
    if (v <= (uint64_t)ZEND_LONG_MAX) ZVAL_LONG(&z, (zend_long)v);
    else ZVAL_DOUBLE(&z, (double)v);
    zend_hash_index_update(m, (zend_ulong)k, &z);
}
static void ch_kvi_add_double(zend_array* m, int64_t k, double v) {
    zval z; ZVAL_DOUBLE(&z, v);
    zend_hash_index_update(m, (zend_ulong)k, &z);
}
static void ch_kvi_add_null(zend_array* m, int64_t k) {
    zval z; ZVAL_NULL(&z);
    zend_hash_index_update(m, (zend_ulong)k, &z);
}
static void ch_kvi_add_arr(zend_array* m, int64_t k, zend_array* v) {
    zval z; ZVAL_ARR(&z, v);
    zend_hash_index_update(m, (zend_ulong)k, &z);
}

// ── Column keys ──────────────────────────────────────────────────────────────

// Regular (non-interned) key string: each row array addrefs it on insert,
// and the creator drops its own reference after the result is built.
// Interned-permanent strings would leak for the process lifetime (one per
// distinct column alias) and mutate the shared intern table at runtime
// from worker threads.
static zend_string* ch_make_key(const char* name, size_t len) {
    return zend_string_init(name, len, 0);
}

static void ch_release_key(zend_string* key) {
    zend_string_release(key);
}

// Frees a result array abandoned mid-build after an error, so the
// exported function can return NULL (→ RuntimeException) without leaking.
static void ch_free_array(zend_array* arr) {
    zend_array_destroy(arr);
}

static zend_array* ch_new_array(uint32_t cap) {
    return zend_new_array(cap);
}

// ch_add_rows builds `rows` PHP row arrays in a single CGo crossing and
// appends them to res. The per-cell scratch is row-major: cell (r, i) lives at
// index r*n + i. Batching amortizes the ~fixed CGo call cost over many rows —
// the caller uses rows=1 only when a column packs a nested zend_array* (whose
// pointer must be consumed in the same call that built it).
static void ch_add_rows(
    zend_array*      res,
    zend_string**    keys,
    const uint8_t*   types,
    const char*      sbuf,
    const uint32_t*  soff,
    const uint32_t*  slen,
    const int64_t*   ivals,
    const uint64_t*  uvals,
    const double*    fvals,
    int              n,
    int              rows)
{
    for (int r = 0; r < rows; r++) {
        const int base = r * n;
        zval row;
        array_init_size(&row, (uint32_t)n);
        for (int i = 0; i < n; i++) {
            const int k = base + i;
            zval z;
            switch (types[k]) {
            case CH_STR:
                ZVAL_STR(&z, zend_string_init(sbuf + soff[k], slen[k], 0));
                break;
            case CH_INT:
                ZVAL_LONG(&z, (zend_long)ivals[k]);
                break;
            case CH_UINT:
                if (uvals[k] <= (uint64_t)ZEND_LONG_MAX)
                    ZVAL_LONG(&z, (zend_long)uvals[k]);
                else
                    ZVAL_DOUBLE(&z, (double)uvals[k]);
                break;
            case CH_FLOAT:
                ZVAL_DOUBLE(&z, fvals[k]);
                break;
            case CH_ARRAY:
                ZVAL_ARR(&z, (zend_array*)(uintptr_t)uvals[k]);
                break;
            default: // CH_NULL
                ZVAL_NULL(&z);
                break;
            }
            zend_hash_add_new(Z_ARRVAL(row), keys[i], &z);
        }
        zend_hash_next_index_insert(res, &row);
    }
}

// ch_add_columns is the columnar transpose of ch_add_rows: instead of one row
// array per row, it appends each cell to its column's array (cols[i]). Same
// row-major scratch (cell (r,i) at r*n+i), one CGo crossing per batch.
static void ch_add_columns(
    zend_array**     cols,
    const uint8_t*   types,
    const char*      sbuf,
    const uint32_t*  soff,
    const uint32_t*  slen,
    const int64_t*   ivals,
    const uint64_t*  uvals,
    const double*    fvals,
    int              n,
    int              rows)
{
    for (int r = 0; r < rows; r++) {
        const int base = r * n;
        for (int i = 0; i < n; i++) {
            const int k = base + i;
            zval z;
            switch (types[k]) {
            case CH_STR:
                ZVAL_STR(&z, zend_string_init(sbuf + soff[k], slen[k], 0));
                break;
            case CH_INT:
                ZVAL_LONG(&z, (zend_long)ivals[k]);
                break;
            case CH_UINT:
                if (uvals[k] <= (uint64_t)ZEND_LONG_MAX)
                    ZVAL_LONG(&z, (zend_long)uvals[k]);
                else
                    ZVAL_DOUBLE(&z, (double)uvals[k]);
                break;
            case CH_FLOAT:
                ZVAL_DOUBLE(&z, fvals[k]);
                break;
            case CH_ARRAY:
                ZVAL_ARR(&z, (zend_array*)(uintptr_t)uvals[k]);
                break;
            default: // CH_NULL
                ZVAL_NULL(&z);
                break;
            }
            zend_hash_next_index_insert(cols[i], &z);
        }
    }
}
*/
import "C"
import (
	"fmt"
	"math"
	"math/big"
	"net/netip"
	"reflect"
	"sort"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// ── Generic query array ───────────────────────────────────────────────────────

const (
	chNull  = C.uint8_t(0)
	chStr   = C.uint8_t(1)
	chInt   = C.uint8_t(2)
	chUInt  = C.uint8_t(3)
	chFloat = C.uint8_t(4)
	chArray = C.uint8_t(5)
)

func newResultArray(cap uint32) unsafe.Pointer {
	return unsafe.Pointer(C.ch_new_array(C.uint32_t(cap)))
}

func makeKey(name string) *C.zend_string {
	return C.ch_make_key(safeCStr(name), C.size_t(len(name)))
}

func releaseKey(key *C.zend_string) {
	C.ch_release_key(key)
}

// freeResultArray releases a partially built result array before an
// error return, so it doesn't leak when the function returns nil.
func freeResultArray(arr unsafe.Pointer) {
	C.ch_free_array((*C.zend_array)(arr))
}

var _emptySbuf = [1]byte{0}

func addGenericRows(arr unsafe.Pointer, keys []*C.zend_string,
	types []C.uint8_t, sbuf []byte,
	soff []C.uint32_t, slen []C.uint32_t,
	ivals []C.int64_t, uvals []C.uint64_t, fvals []C.double, n, rows int,
) {
	sp := (*C.char)(unsafe.Pointer(&_emptySbuf[0]))
	if len(sbuf) > 0 {
		sp = (*C.char)(unsafe.Pointer(unsafe.SliceData(sbuf)))
	}
	C.ch_add_rows(
		(*C.zend_array)(arr),
		unsafe.SliceData(keys),
		unsafe.SliceData(types),
		sp,
		unsafe.SliceData(soff),
		unsafe.SliceData(slen),
		unsafe.SliceData(ivals),
		unsafe.SliceData(uvals),
		unsafe.SliceData(fvals),
		C.int(n),
		C.int(rows),
	)
}

// addGenericColumns appends `rows` rows of row-major scratch into the per-column
// arrays (cols[i] gets column i's values), in one CGo crossing.
func addGenericColumns(cols []*C.zend_array,
	types []C.uint8_t, sbuf []byte,
	soff []C.uint32_t, slen []C.uint32_t,
	ivals []C.int64_t, uvals []C.uint64_t, fvals []C.double, n, rows int,
) {
	sp := (*C.char)(unsafe.Pointer(&_emptySbuf[0]))
	if len(sbuf) > 0 {
		sp = (*C.char)(unsafe.Pointer(unsafe.SliceData(sbuf)))
	}
	C.ch_add_columns(
		unsafe.SliceData(cols),
		unsafe.SliceData(types),
		sp,
		unsafe.SliceData(soff),
		unsafe.SliceData(slen),
		unsafe.SliceData(ivals),
		unsafe.SliceData(uvals),
		unsafe.SliceData(fvals),
		C.int(n),
		C.int(rows),
	)
}

// packCol converts the scanned value at column i into the typed arrays used by
// addGenericRow. String and DateTime values are appended to sbuf; numeric values
// go into ivals/uvals/fvals. The type tag is written to types[i].
func packCol(
	i int,
	m colMeta,
	dest interface{},
	types []C.uint8_t,
	soff []C.uint32_t,
	slen []C.uint32_t,
	ivals []C.int64_t,
	uvals []C.uint64_t,
	fvals []C.double,
	sbuf *[]byte,
) {
	off := C.uint32_t(len(*sbuf))

	// Unwrap nullable pointer; set NULL and return early if nil.
	if m.nullable {
		switch m.kind {
		case kindString:
			pp := dest.(*(*string))
			if *pp == nil {
				types[i] = chNull
				return
			}
			dest = *pp
		case kindDateTime:
			pp := dest.(*(*time.Time))
			if *pp == nil {
				types[i] = chNull
				return
			}
			dest = *pp
		case kindDateTime64:
			pp := dest.(*(*time.Time))
			if *pp == nil {
				types[i] = chNull
				return
			}
			dest = *pp
		case kindFloat32:
			pp := dest.(*(*float32))
			if *pp == nil {
				types[i] = chNull
				return
			}
			dest = *pp
		case kindFloat64:
			pp := dest.(*(*float64))
			if *pp == nil {
				types[i] = chNull
				return
			}
			dest = *pp
		case kindInt8:
			pp := dest.(*(*int8))
			if *pp == nil {
				types[i] = chNull
				return
			}
			dest = *pp
		case kindInt16:
			pp := dest.(*(*int16))
			if *pp == nil {
				types[i] = chNull
				return
			}
			dest = *pp
		case kindInt32:
			pp := dest.(*(*int32))
			if *pp == nil {
				types[i] = chNull
				return
			}
			dest = *pp
		case kindInt64:
			pp := dest.(*(*int64))
			if *pp == nil {
				types[i] = chNull
				return
			}
			dest = *pp
		case kindUInt8:
			pp := dest.(*(*uint8))
			if *pp == nil {
				types[i] = chNull
				return
			}
			dest = *pp
		case kindUInt16:
			pp := dest.(*(*uint16))
			if *pp == nil {
				types[i] = chNull
				return
			}
			dest = *pp
		case kindUInt32:
			pp := dest.(*(*uint32))
			if *pp == nil {
				types[i] = chNull
				return
			}
			dest = *pp
		case kindUInt64:
			pp := dest.(*(*uint64))
			if *pp == nil {
				types[i] = chNull
				return
			}
			dest = *pp
		case kindBool:
			pp := dest.(*(*bool))
			if *pp == nil {
				types[i] = chNull
				return
			}
			dest = *pp
		case kindUUID:
			pp := dest.(*(*uuid.UUID))
			if *pp == nil {
				types[i] = chNull
				return
			}
			dest = *pp
		case kindIPv4, kindIPv6:
			pp := dest.(*(*netip.Addr))
			if *pp == nil {
				types[i] = chNull
				return
			}
			dest = *pp
		case kindDecimal:
			pp := dest.(*(*decimal.Decimal))
			if *pp == nil {
				types[i] = chNull
				return
			}
			dest = *pp
		}
	}

	switch m.kind {
	case kindString:
		s := *(dest.(*string))
		*sbuf = append(*sbuf, s...)
		soff[i], slen[i], types[i] = off, C.uint32_t(len(s)), chStr
	case kindDateTime:
		*sbuf = appendClickHouseDateTime(*sbuf, *(dest.(*time.Time)))
		soff[i], slen[i], types[i] = off, C.uint32_t(uint32(len(*sbuf))-uint32(off)), chStr
	case kindDateTime64:
		*sbuf = appendClickHouseDateTime64(*sbuf, *(dest.(*time.Time)))
		soff[i], slen[i], types[i] = off, C.uint32_t(uint32(len(*sbuf))-uint32(off)), chStr
	case kindFloat32:
		fvals[i], types[i] = C.double(*(dest.(*float32))), chFloat
	case kindFloat64:
		fvals[i], types[i] = C.double(*(dest.(*float64))), chFloat
	case kindInt8:
		ivals[i], types[i] = C.int64_t(*(dest.(*int8))), chInt
	case kindInt16:
		ivals[i], types[i] = C.int64_t(*(dest.(*int16))), chInt
	case kindInt32:
		ivals[i], types[i] = C.int64_t(*(dest.(*int32))), chInt
	case kindInt64:
		ivals[i], types[i] = C.int64_t(*(dest.(*int64))), chInt
	case kindUInt8:
		uvals[i], types[i] = C.uint64_t(*(dest.(*uint8))), chUInt
	case kindUInt16:
		uvals[i], types[i] = C.uint64_t(*(dest.(*uint16))), chUInt
	case kindUInt32:
		uvals[i], types[i] = C.uint64_t(*(dest.(*uint32))), chUInt
	case kindUInt64:
		uvals[i], types[i] = C.uint64_t(*(dest.(*uint64))), chUInt
	case kindBool:
		v := *(dest.(*bool))
		if v {
			uvals[i], types[i] = 1, chUInt
		} else {
			uvals[i], types[i] = 0, chUInt
		}
	case kindUUID:
		s := (*(dest.(*uuid.UUID))).String()
		*sbuf = append(*sbuf, s...)
		soff[i], slen[i], types[i] = off, C.uint32_t(len(s)), chStr
	case kindIPv4, kindIPv6:
		s := (*(dest.(*netip.Addr))).String()
		*sbuf = append(*sbuf, s...)
		soff[i], slen[i], types[i] = off, C.uint32_t(len(s)), chStr
	case kindDecimal:
		s := (*(dest.(*decimal.Decimal))).String()
		*sbuf = append(*sbuf, s...)
		soff[i], slen[i], types[i] = off, C.uint32_t(len(s)), chStr
	case kindBigInt:
		// (U)Int128/256 scan into **big.Int (driver ScanType); nil = NULL.
		p := *(dest.(**big.Int))
		if p == nil {
			types[i] = chNull
			return
		}
		s := p.String()
		*sbuf = append(*sbuf, s...)
		soff[i], slen[i], types[i] = off, C.uint32_t(len(s)), chStr
	case kindJSON:
		// The JSON ScanType is a value type: dest is *chcol.JSON.
		var jm map[string]any
		switch j := dest.(type) {
		case *chcol.JSON:
			jm = j.NestedMap()
		case **chcol.JSON:
			if *j == nil {
				types[i] = chNull
				return
			}
			jm = (*j).NestedMap()
		}
		arr := buildAnyMap(jm, 0)
		uvals[i] = C.uint64_t(uintptr(arr))
		types[i] = chArray
	case kindArray:
		arr := buildPHPArray(dest, m.inner)
		uvals[i] = C.uint64_t(uintptr(arr))
		types[i] = chArray
	case kindMap:
		arr := buildReflectMap(reflect.ValueOf(dest).Elem(), &m)
		uvals[i] = C.uint64_t(uintptr(arr))
		types[i] = chArray
	case kindTuple:
		arr := buildTuple(reflect.ValueOf(dest).Elem(), &m)
		uvals[i] = C.uint64_t(uintptr(arr))
		types[i] = chArray
	case kindGeo:
		arr := buildGeo(reflect.ValueOf(dest).Elem())
		uvals[i] = C.uint64_t(uintptr(arr))
		types[i] = chArray
	case kindDynamic:
		// Variant(...) / Dynamic scan into chcol.Variant; resolve the concrete
		// value and pack it dynamically into this column's slot.
		var v any
		if d, ok := dest.(*chcol.Variant); ok {
			if d.Nil() {
				types[i] = chNull
				return
			}
			v = d.Any()
		} else {
			v = reflect.ValueOf(dest).Elem().Interface()
		}
		packAnyValue(i, v, types, soff, slen, ivals, uvals, fvals, sbuf)
	}
}

// packAnyValue writes a dynamically-typed value (from a Variant/Dynamic column)
// into column slot i: scalars go to the typed arrays like packCol, composites
// build a nested zend_array (reusing the JSON walker for their contents).
// Mirrors addAnyKV, but targets a column slot rather than a keyed insert.
func packAnyValue(
	i int, v any,
	types []C.uint8_t, soff []C.uint32_t, slen []C.uint32_t,
	ivals []C.int64_t, uvals []C.uint64_t, fvals []C.double, sbuf *[]byte,
) {
	if vt, ok := v.(chcol.Variant); ok {
		if vt.Nil() {
			types[i] = chNull
			return
		}
		v = vt.Any()
	}
	off := C.uint32_t(len(*sbuf))
	appendStr := func(s string) {
		*sbuf = append(*sbuf, s...)
		soff[i], slen[i], types[i] = off, C.uint32_t(len(s)), chStr
	}
	switch t := v.(type) {
	case nil:
		types[i] = chNull
	case string:
		appendStr(t)
	case bool:
		if t {
			uvals[i] = 1
		} else {
			uvals[i] = 0
		}
		types[i] = chUInt
	case int:
		ivals[i], types[i] = C.int64_t(t), chInt
	case int8:
		ivals[i], types[i] = C.int64_t(t), chInt
	case int16:
		ivals[i], types[i] = C.int64_t(t), chInt
	case int32:
		ivals[i], types[i] = C.int64_t(t), chInt
	case int64:
		ivals[i], types[i] = C.int64_t(t), chInt
	case uint:
		uvals[i], types[i] = C.uint64_t(t), chUInt
	case uint8:
		uvals[i], types[i] = C.uint64_t(t), chUInt
	case uint16:
		uvals[i], types[i] = C.uint64_t(t), chUInt
	case uint32:
		uvals[i], types[i] = C.uint64_t(t), chUInt
	case uint64:
		uvals[i], types[i] = C.uint64_t(t), chUInt
	case float32:
		fvals[i], types[i] = C.double(t), chFloat
	case float64:
		fvals[i], types[i] = C.double(t), chFloat
	case time.Time:
		appendStr(string(appendClickHouseDateTime64(nil, t)))
	case *big.Int:
		appendStr(t.String())
	case decimal.Decimal:
		appendStr(t.String())
	case uuid.UUID:
		appendStr(t.String())
	case netip.Addr:
		appendStr(t.String())
	case map[string]any:
		uvals[i], types[i] = C.uint64_t(uintptr(buildAnyMap(t, 0))), chArray
	case chcol.JSON:
		uvals[i], types[i] = C.uint64_t(uintptr(buildAnyMap(t.NestedMap(), 0))), chArray
	case *chcol.JSON:
		if t == nil {
			types[i] = chNull
			return
		}
		uvals[i], types[i] = C.uint64_t(uintptr(buildAnyMap(t.NestedMap(), 0))), chArray
	default:
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Pointer:
			if rv.IsNil() {
				types[i] = chNull
				return
			}
			packAnyValue(i, rv.Elem().Interface(), types, soff, slen, ivals, uvals, fvals, sbuf)
		case reflect.Slice, reflect.Array:
			sub := C.ch_new_array(C.uint32_t(rv.Len()))
			for j := 0; j < rv.Len(); j++ {
				addAnyKV(sub, phpKey{isInt: true, i: int64(j)}, rv.Index(j).Interface(), 1)
			}
			uvals[i], types[i] = C.uint64_t(uintptr(unsafe.Pointer(sub))), chArray
		case reflect.Map:
			sub := C.ch_new_array(C.uint32_t(rv.Len()))
			iter := rv.MapRange()
			for iter.Next() {
				addAnyKV(sub, phpKey{s: fmt.Sprintf("%v", iter.Key().Interface())}, iter.Value().Interface(), 1)
			}
			uvals[i], types[i] = C.uint64_t(uintptr(unsafe.Pointer(sub))), chArray
		default:
			appendStr(fmt.Sprintf("%v", v))
		}
	}
}

// buildGeo converts an orb.* geo value into a nested PHP array. A Point is a
// [2]float64 (Go array) → [x, y]; every other geo type is a slice nesting down
// to Point (Ring/LineString → [][2], Polygon/MultiLineString → [][][2],
// MultiPolygon → [][][][2]). The Go value's kind — array vs slice — drives the
// recursion, so all six types share this one builder.
func buildGeo(v reflect.Value) unsafe.Pointer {
	for v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return unsafe.Pointer(C.ch_new_array(0))
		}
		v = v.Elem()
	}
	if v.Kind() == reflect.Array { // Point: [x, y]
		arr := C.ch_new_array(C.uint32_t(v.Len()))
		for i := 0; i < v.Len(); i++ {
			C.ch_arr_add_double(arr, C.double(v.Index(i).Float()))
		}
		return unsafe.Pointer(arr)
	}
	n := v.Len() // Ring/LineString/Polygon/MultiPolygon/MultiLineString
	arr := C.ch_new_array(C.uint32_t(n))
	for i := 0; i < n; i++ {
		C.ch_arr_add_arr(arr, (*C.zend_array)(buildGeo(v.Index(i))))
	}
	return unsafe.Pointer(arr)
}

// ── Array builder ────────────────────────────────────────────────────────────

func arrAddStr(arr *C.zend_array, s string) {
	C.ch_arr_add_str(arr, safeCStr(s), C.uint32_t(len(s)))
}

func buildPHPArray(dest interface{}, inner *colMeta) unsafe.Pointer {
	if inner == nil {
		return unsafe.Pointer(C.ch_new_array(0))
	}
	// Nested arrays, maps, tuples, geo and dynamic have no typed fast path.
	if inner.kind == kindArray || inner.kind == kindMap || inner.kind == kindTuple || inner.kind == kindGeo || inner.kind == kindDynamic {
		return buildReflectArray(reflect.ValueOf(dest).Elem(), inner)
	}
	if inner.nullable {
		return buildNullableArray(dest, inner)
	}
	return buildNonNullableArray(dest, inner)
}

func buildNonNullableArray(dest interface{}, inner *colMeta) unsafe.Pointer {
	switch inner.kind {
	case kindString:
		s := *(dest.(*[]string))
		arr := C.ch_new_array(C.uint32_t(len(s)))
		for _, v := range s {
			arrAddStr(arr, v)
		}
		return unsafe.Pointer(arr)
	case kindDateTime:
		s := *(dest.(*[]time.Time))
		arr := C.ch_new_array(C.uint32_t(len(s)))
		for _, v := range s {
			str := string(appendClickHouseDateTime(nil, v))
			arrAddStr(arr, str)
		}
		return unsafe.Pointer(arr)
	case kindDateTime64:
		s := *(dest.(*[]time.Time))
		arr := C.ch_new_array(C.uint32_t(len(s)))
		for _, v := range s {
			str := string(appendClickHouseDateTime64(nil, v))
			arrAddStr(arr, str)
		}
		return unsafe.Pointer(arr)
	case kindFloat32:
		s := *(dest.(*[]float32))
		arr := C.ch_new_array(C.uint32_t(len(s)))
		for _, v := range s {
			C.ch_arr_add_double(arr, C.double(v))
		}
		return unsafe.Pointer(arr)
	case kindFloat64:
		s := *(dest.(*[]float64))
		arr := C.ch_new_array(C.uint32_t(len(s)))
		for _, v := range s {
			C.ch_arr_add_double(arr, C.double(v))
		}
		return unsafe.Pointer(arr)
	case kindInt8:
		s := *(dest.(*[]int8))
		arr := C.ch_new_array(C.uint32_t(len(s)))
		for _, v := range s {
			C.ch_arr_add_long(arr, C.int64_t(v))
		}
		return unsafe.Pointer(arr)
	case kindInt16:
		s := *(dest.(*[]int16))
		arr := C.ch_new_array(C.uint32_t(len(s)))
		for _, v := range s {
			C.ch_arr_add_long(arr, C.int64_t(v))
		}
		return unsafe.Pointer(arr)
	case kindInt32:
		s := *(dest.(*[]int32))
		arr := C.ch_new_array(C.uint32_t(len(s)))
		for _, v := range s {
			C.ch_arr_add_long(arr, C.int64_t(v))
		}
		return unsafe.Pointer(arr)
	case kindInt64:
		s := *(dest.(*[]int64))
		arr := C.ch_new_array(C.uint32_t(len(s)))
		for _, v := range s {
			C.ch_arr_add_long(arr, C.int64_t(v))
		}
		return unsafe.Pointer(arr)
	case kindUInt8:
		s := *(dest.(*[]uint8))
		arr := C.ch_new_array(C.uint32_t(len(s)))
		for _, v := range s {
			C.ch_arr_add_ulong(arr, C.uint64_t(v))
		}
		return unsafe.Pointer(arr)
	case kindUInt16:
		s := *(dest.(*[]uint16))
		arr := C.ch_new_array(C.uint32_t(len(s)))
		for _, v := range s {
			C.ch_arr_add_ulong(arr, C.uint64_t(v))
		}
		return unsafe.Pointer(arr)
	case kindUInt32:
		s := *(dest.(*[]uint32))
		arr := C.ch_new_array(C.uint32_t(len(s)))
		for _, v := range s {
			C.ch_arr_add_ulong(arr, C.uint64_t(v))
		}
		return unsafe.Pointer(arr)
	case kindUInt64:
		s := *(dest.(*[]uint64))
		arr := C.ch_new_array(C.uint32_t(len(s)))
		for _, v := range s {
			C.ch_arr_add_ulong(arr, C.uint64_t(v))
		}
		return unsafe.Pointer(arr)
	case kindBool:
		s := *(dest.(*[]bool))
		arr := C.ch_new_array(C.uint32_t(len(s)))
		for _, v := range s {
			if v {
				C.ch_arr_add_ulong(arr, 1)
			} else {
				C.ch_arr_add_ulong(arr, 0)
			}
		}
		return unsafe.Pointer(arr)
	case kindUUID:
		s := *(dest.(*[]uuid.UUID))
		arr := C.ch_new_array(C.uint32_t(len(s)))
		for _, v := range s {
			str := v.String()
			arrAddStr(arr, str)
		}
		return unsafe.Pointer(arr)
	case kindIPv4, kindIPv6:
		s := *(dest.(*[]netip.Addr))
		arr := C.ch_new_array(C.uint32_t(len(s)))
		for _, v := range s {
			str := v.String()
			arrAddStr(arr, str)
		}
		return unsafe.Pointer(arr)
	case kindDecimal:
		s := *(dest.(*[]decimal.Decimal))
		arr := C.ch_new_array(C.uint32_t(len(s)))
		for _, v := range s {
			str := v.String()
			arrAddStr(arr, str)
		}
		return unsafe.Pointer(arr)
	}
	// No typed path for this element type — generic reflection instead
	// of a silently empty array.
	return buildReflectArray(reflect.ValueOf(dest).Elem(), inner)
}

func buildNullableArray(dest interface{}, inner *colMeta) unsafe.Pointer {
	switch inner.kind {
	case kindString:
		s := *(dest.(*[]*string))
		arr := C.ch_new_array(C.uint32_t(len(s)))
		for _, v := range s {
			if v == nil {
				C.ch_arr_add_null(arr)
			} else {
				arrAddStr(arr, *v)
			}
		}
		return unsafe.Pointer(arr)
	case kindInt32:
		s := *(dest.(*[]*int32))
		arr := C.ch_new_array(C.uint32_t(len(s)))
		for _, v := range s {
			if v == nil {
				C.ch_arr_add_null(arr)
			} else {
				C.ch_arr_add_long(arr, C.int64_t(*v))
			}
		}
		return unsafe.Pointer(arr)
	case kindInt64:
		s := *(dest.(*[]*int64))
		arr := C.ch_new_array(C.uint32_t(len(s)))
		for _, v := range s {
			if v == nil {
				C.ch_arr_add_null(arr)
			} else {
				C.ch_arr_add_long(arr, C.int64_t(*v))
			}
		}
		return unsafe.Pointer(arr)
	case kindUInt32:
		s := *(dest.(*[]*uint32))
		arr := C.ch_new_array(C.uint32_t(len(s)))
		for _, v := range s {
			if v == nil {
				C.ch_arr_add_null(arr)
			} else {
				C.ch_arr_add_ulong(arr, C.uint64_t(*v))
			}
		}
		return unsafe.Pointer(arr)
	case kindFloat64:
		s := *(dest.(*[]*float64))
		arr := C.ch_new_array(C.uint32_t(len(s)))
		for _, v := range s {
			if v == nil {
				C.ch_arr_add_null(arr)
			} else {
				C.ch_arr_add_double(arr, C.double(*v))
			}
		}
		return unsafe.Pointer(arr)
	case kindDateTime:
		s := *(dest.(*[]*time.Time))
		arr := C.ch_new_array(C.uint32_t(len(s)))
		for _, v := range s {
			if v == nil {
				C.ch_arr_add_null(arr)
			} else {
				str := string(appendClickHouseDateTime(nil, *v))
				arrAddStr(arr, str)
			}
		}
		return unsafe.Pointer(arr)
	case kindDateTime64:
		s := *(dest.(*[]*time.Time))
		arr := C.ch_new_array(C.uint32_t(len(s)))
		for _, v := range s {
			if v == nil {
				C.ch_arr_add_null(arr)
			} else {
				str := string(appendClickHouseDateTime64(nil, *v))
				arrAddStr(arr, str)
			}
		}
		return unsafe.Pointer(arr)
	case kindUUID:
		s := *(dest.(*[]*uuid.UUID))
		arr := C.ch_new_array(C.uint32_t(len(s)))
		for _, v := range s {
			if v == nil {
				C.ch_arr_add_null(arr)
			} else {
				str := v.String()
				arrAddStr(arr, str)
			}
		}
		return unsafe.Pointer(arr)
	}
	// No typed path for this nullable element type — generic reflection
	// instead of a silently empty array (bool, uint64, decimal, IP…).
	return buildReflectArray(reflect.ValueOf(dest).Elem(), inner)
}

// ── Reflect-based builder (Map, nested arrays, uncommon combos) ──────────────
//
// The typed builders above cover the hot flat-array cases without
// reflection. Everything else — Map(K, V), Array(Array(T)), Array(Map),
// and array element types the typed switches don't list — goes through
// this generic recursive path, driven by the parsed colMeta tree.

// phpKey is a resolved PHP array key — int when the ClickHouse key type
// maps to a PHP int, string otherwise.
type phpKey struct {
	isInt bool
	i     int64
	s     string
}

func mapKeyOf(k reflect.Value, m *colMeta) phpKey {
	switch m.kind {
	case kindInt8, kindInt16, kindInt32, kindInt64:
		return phpKey{isInt: true, i: k.Int()}
	case kindUInt8, kindUInt16, kindUInt32, kindUInt64:
		u := k.Uint()
		if u <= math.MaxInt64 {
			return phpKey{isInt: true, i: int64(u)}
		}
		return phpKey{s: strconv.FormatUint(u, 10)}
	default:
		return phpKey{s: reflectScalarString(k, m)}
	}
}

// reflectScalarString renders a scalar reflect value with the same
// formatting as packCol (ClickHouse date format, canonical UUID…).
func reflectScalarString(v reflect.Value, m *colMeta) string {
	switch m.kind {
	case kindDateTime:
		return string(appendClickHouseDateTime(nil, v.Interface().(time.Time)))
	case kindDateTime64:
		return string(appendClickHouseDateTime64(nil, v.Interface().(time.Time)))
	case kindUUID:
		return v.Interface().(uuid.UUID).String()
	case kindIPv4, kindIPv6:
		return v.Interface().(netip.Addr).String()
	case kindDecimal:
		return v.Interface().(decimal.Decimal).String()
	case kindBigInt:
		switch b := v.Interface().(type) {
		case *big.Int:
			return b.String()
		case big.Int:
			return b.String()
		}
		return v.String()
	default:
		return v.String()
	}
}

// ── Dynamically-typed values (JSON trees) ─────────────────────────────────────

// maxJSONDepth caps recursion when materializing a JSON tree into PHP
// arrays. JSON values come from the server, but a pathologically deep
// document should fail a leaf rather than overflow the Go stack.
const maxJSONDepth = 128

// buildAnyMap converts a JSON NestedMap into a PHP associative array
// (keys sorted for deterministic output).
func buildAnyMap(m map[string]any, depth int) unsafe.Pointer {
	arr := C.ch_new_array(C.uint32_t(len(m)))
	if depth > maxJSONDepth {
		return unsafe.Pointer(arr)
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		addAnyKV((*C.zend_array)(arr), phpKey{s: k}, m[k], depth)
	}
	return unsafe.Pointer(arr)
}

// addAnyKV adds one dynamically-typed value (JSON leaf or subtree)
// under a key. Lists use sequential int keys, which PHP stores as a
// packed indexed array.
func addAnyKV(arr *C.zend_array, k phpKey, v any, depth int) {
	if depth > maxJSONDepth {
		kvAddNull(arr, k)
		return
	}
	if vt, ok := v.(chcol.Variant); ok {
		if vt.Nil() {
			kvAddNull(arr, k)
			return
		}
		v = vt.Any()
	}
	switch t := v.(type) {
	case nil:
		kvAddNull(arr, k)
	case string:
		kvAddStr(arr, k, t)
	case bool:
		if t {
			kvAddULong(arr, k, 1)
		} else {
			kvAddULong(arr, k, 0)
		}
	case int:
		kvAddLong(arr, k, int64(t))
	case int8:
		kvAddLong(arr, k, int64(t))
	case int16:
		kvAddLong(arr, k, int64(t))
	case int32:
		kvAddLong(arr, k, int64(t))
	case int64:
		kvAddLong(arr, k, t)
	case uint:
		kvAddULong(arr, k, uint64(t))
	case uint8:
		kvAddULong(arr, k, uint64(t))
	case uint16:
		kvAddULong(arr, k, uint64(t))
	case uint32:
		kvAddULong(arr, k, uint64(t))
	case uint64:
		kvAddULong(arr, k, t)
	case float32:
		kvAddDouble(arr, k, float64(t))
	case float64:
		kvAddDouble(arr, k, t)
	case time.Time:
		kvAddStr(arr, k, string(appendClickHouseDateTime64(nil, t)))
	case *big.Int:
		kvAddStr(arr, k, t.String())
	case decimal.Decimal:
		kvAddStr(arr, k, t.String())
	case uuid.UUID:
		kvAddStr(arr, k, t.String())
	case netip.Addr:
		kvAddStr(arr, k, t.String())
	case map[string]any:
		kvAddArr(arr, k, (*C.zend_array)(buildAnyMap(t, depth+1)))
	case chcol.JSON:
		// A nested object (e.g. an element of a JSON array of objects)
		// surfaces as its own JSON value — recurse into its NestedMap.
		kvAddArr(arr, k, (*C.zend_array)(buildAnyMap(t.NestedMap(), depth+1)))
	case *chcol.JSON:
		if t == nil {
			kvAddNull(arr, k)
			return
		}
		kvAddArr(arr, k, (*C.zend_array)(buildAnyMap(t.NestedMap(), depth+1)))
	case []any:
		sub := C.ch_new_array(C.uint32_t(len(t)))
		for j, e := range t {
			addAnyKV((*C.zend_array)(sub), phpKey{isInt: true, i: int64(j)}, e, depth+1)
		}
		kvAddArr(arr, k, (*C.zend_array)(sub))
	default:
		// Dynamic values arrive in many concrete shapes ([]*Variant,
		// typed slices, nested maps…) — walk them via reflection.
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Pointer:
			if rv.IsNil() {
				kvAddNull(arr, k)
				return
			}
			addAnyKV(arr, k, rv.Elem().Interface(), depth+1)
		case reflect.Slice, reflect.Array:
			sub := C.ch_new_array(C.uint32_t(rv.Len()))
			for j := 0; j < rv.Len(); j++ {
				addAnyKV((*C.zend_array)(sub), phpKey{isInt: true, i: int64(j)}, rv.Index(j).Interface(), depth+1)
			}
			kvAddArr(arr, k, (*C.zend_array)(sub))
		case reflect.Map:
			sub := C.ch_new_array(C.uint32_t(rv.Len()))
			iter := rv.MapRange()
			for iter.Next() {
				addAnyKV((*C.zend_array)(sub), phpKey{s: fmt.Sprintf("%v", iter.Key().Interface())}, iter.Value().Interface(), depth+1)
			}
			kvAddArr(arr, k, (*C.zend_array)(sub))
		default:
			kvAddStr(arr, k, fmt.Sprintf("%v", t))
		}
	}
}

func kvAddStr(m *C.zend_array, k phpKey, s string) {
	if k.isInt {
		C.ch_kvi_add_str(m, C.int64_t(k.i), safeCStr(s), C.uint32_t(len(s)))
	} else {
		C.ch_kv_add_str(m, safeCStr(k.s), C.size_t(len(k.s)), safeCStr(s), C.uint32_t(len(s)))
	}
}

func kvAddLong(m *C.zend_array, k phpKey, v int64) {
	if k.isInt {
		C.ch_kvi_add_long(m, C.int64_t(k.i), C.int64_t(v))
	} else {
		C.ch_kv_add_long(m, safeCStr(k.s), C.size_t(len(k.s)), C.int64_t(v))
	}
}

func kvAddULong(m *C.zend_array, k phpKey, v uint64) {
	if k.isInt {
		C.ch_kvi_add_ulong(m, C.int64_t(k.i), C.uint64_t(v))
	} else {
		C.ch_kv_add_ulong(m, safeCStr(k.s), C.size_t(len(k.s)), C.uint64_t(v))
	}
}

func kvAddDouble(m *C.zend_array, k phpKey, v float64) {
	if k.isInt {
		C.ch_kvi_add_double(m, C.int64_t(k.i), C.double(v))
	} else {
		C.ch_kv_add_double(m, safeCStr(k.s), C.size_t(len(k.s)), C.double(v))
	}
}

func kvAddNull(m *C.zend_array, k phpKey) {
	if k.isInt {
		C.ch_kvi_add_null(m, C.int64_t(k.i))
	} else {
		C.ch_kv_add_null(m, safeCStr(k.s), C.size_t(len(k.s)))
	}
}

func kvAddArr(m *C.zend_array, k phpKey, v *C.zend_array) {
	if k.isInt {
		C.ch_kvi_add_arr(m, C.int64_t(k.i), v)
	} else {
		C.ch_kv_add_arr(m, safeCStr(k.s), C.size_t(len(k.s)), v)
	}
}

// buildReflectArray converts a reflect slice into an indexed PHP array,
// recursing into nested arrays and maps.
func buildReflectArray(v reflect.Value, elem *colMeta) unsafe.Pointer {
	n := v.Len()
	arr := C.ch_new_array(C.uint32_t(n))
	for j := 0; j < n; j++ {
		e := v.Index(j)
		if elem.nullable {
			if e.IsNil() {
				C.ch_arr_add_null(arr)
				continue
			}
			e = e.Elem()
		}
		switch elem.kind {
		case kindArray:
			C.ch_arr_add_arr(arr, (*C.zend_array)(buildReflectArray(e, elem.inner)))
		case kindMap:
			C.ch_arr_add_arr(arr, (*C.zend_array)(buildReflectMap(e, elem)))
		case kindTuple:
			C.ch_arr_add_arr(arr, (*C.zend_array)(buildTuple(e, elem)))
		case kindGeo:
			C.ch_arr_add_arr(arr, (*C.zend_array)(buildGeo(e)))
		case kindDynamic:
			// Element is a chcol.Variant — resolve dynamically (sequential int
			// keys keep the array packed, same as ch_arr_add_*).
			addAnyKV(arr, phpKey{isInt: true, i: int64(j)}, e.Interface(), 1)
		case kindString:
			arrAddStr(arr, e.String())
		case kindInt8, kindInt16, kindInt32, kindInt64:
			C.ch_arr_add_long(arr, C.int64_t(e.Int()))
		case kindUInt8, kindUInt16, kindUInt32, kindUInt64:
			C.ch_arr_add_ulong(arr, C.uint64_t(e.Uint()))
		case kindFloat32, kindFloat64:
			C.ch_arr_add_double(arr, C.double(e.Float()))
		case kindBool:
			if e.Bool() {
				C.ch_arr_add_ulong(arr, 1)
			} else {
				C.ch_arr_add_ulong(arr, 0)
			}
		default: // DateTime*, UUID, IP, Decimal → formatted strings
			arrAddStr(arr, reflectScalarString(e, elem))
		}
	}
	return unsafe.Pointer(arr)
}

// buildReflectMap converts a reflect map (Map(K, V)) into a PHP array
// keyed by the ClickHouse key column. Keys are sorted (ints numerically,
// strings lexically) — Go map iteration order is random, and the driver
// has already dropped the server-side insertion order anyway.
func buildReflectMap(v reflect.Value, m *colMeta) unsafe.Pointer {
	arr := C.ch_new_array(C.uint32_t(v.Len()))

	mapKeys := v.MapKeys()
	keys := make([]phpKey, len(mapKeys))
	for i, mk := range mapKeys {
		keys[i] = mapKeyOf(mk, m.inner)
	}
	order := make([]int, len(mapKeys))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool {
		ka, kb := keys[order[a]], keys[order[b]]
		if ka.isInt != kb.isInt {
			return ka.isInt // int keys first
		}
		if ka.isInt {
			return ka.i < kb.i
		}
		return ka.s < kb.s
	})

	for _, idx := range order {
		k := keys[idx]
		e := v.MapIndex(mapKeys[idx])
		val := m.value
		if val.nullable {
			if e.IsNil() {
				kvAddNull(arr, k)
				continue
			}
			e = e.Elem()
		}
		switch val.kind {
		case kindArray:
			kvAddArr(arr, k, (*C.zend_array)(buildReflectArray(e, val.inner)))
		case kindMap:
			kvAddArr(arr, k, (*C.zend_array)(buildReflectMap(e, val)))
		case kindTuple:
			kvAddArr(arr, k, (*C.zend_array)(buildTuple(e, val)))
		case kindGeo:
			kvAddArr(arr, k, (*C.zend_array)(buildGeo(e)))
		case kindDynamic:
			addAnyKV(arr, k, e.Interface(), 1)
		case kindString:
			kvAddStr(arr, k, e.String())
		case kindInt8, kindInt16, kindInt32, kindInt64:
			kvAddLong(arr, k, e.Int())
		case kindUInt8, kindUInt16, kindUInt32, kindUInt64:
			kvAddULong(arr, k, e.Uint())
		case kindFloat32, kindFloat64:
			kvAddDouble(arr, k, e.Float())
		case kindBool:
			if e.Bool() {
				kvAddULong(arr, k, 1)
			} else {
				kvAddULong(arr, k, 0)
			}
		default: // DateTime*, UUID, IP, Decimal → formatted strings
			kvAddStr(arr, k, reflectScalarString(e, val))
		}
	}
	return unsafe.Pointer(arr)
}

// buildTuple converts a scanned Tuple value into a PHP array. Named tuples
// (driver ScanType map[string]any) become an associative array keyed by field
// name; unnamed tuples (ScanType []any) become an indexed array in field
// order. Each field is rendered per its own colMeta, so a DateTime field
// formats like a DateTime column, a nested Array like an Array column, etc.
func buildTuple(v reflect.Value, m *colMeta) unsafe.Pointer {
	for v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return unsafe.Pointer(C.ch_new_array(0))
		}
		v = v.Elem()
	}
	arr := C.ch_new_array(C.uint32_t(len(m.fields)))
	for i := range m.fields {
		f := &m.fields[i]
		if m.named {
			k := phpKey{s: f.name}
			var e reflect.Value
			if v.Kind() == reflect.Map {
				e = v.MapIndex(reflect.ValueOf(f.name))
			}
			if !e.IsValid() {
				kvAddNull(arr, k)
				continue
			}
			addTupleField(arr, k, e, &f.meta)
		} else {
			k := phpKey{isInt: true, i: int64(i)}
			if v.Kind() != reflect.Slice || i >= v.Len() {
				kvAddNull(arr, k)
				continue
			}
			addTupleField(arr, k, v.Index(i), &f.meta)
		}
	}
	return unsafe.Pointer(arr)
}

// addTupleField inserts one tuple field value e under key k, formatting it
// per its colMeta. Mirrors the element switches in buildReflectArray /
// buildReflectMap but drives insertion through the phpKey helpers so it
// serves both named (string keys) and unnamed (int keys) tuples.
func addTupleField(arr *C.zend_array, k phpKey, e reflect.Value, m *colMeta) {
	if e.Kind() == reflect.Interface {
		e = e.Elem()
	}
	if m.nullable {
		if !e.IsValid() || (e.Kind() == reflect.Pointer && e.IsNil()) {
			kvAddNull(arr, k)
			return
		}
		if e.Kind() == reflect.Pointer {
			e = e.Elem()
		}
	}
	if !e.IsValid() {
		kvAddNull(arr, k)
		return
	}
	switch m.kind {
	case kindArray:
		kvAddArr(arr, k, (*C.zend_array)(buildReflectArray(e, m.inner)))
	case kindMap:
		kvAddArr(arr, k, (*C.zend_array)(buildReflectMap(e, m)))
	case kindTuple:
		kvAddArr(arr, k, (*C.zend_array)(buildTuple(e, m)))
	case kindGeo:
		kvAddArr(arr, k, (*C.zend_array)(buildGeo(e)))
	case kindJSON, kindDynamic:
		// JSON / Variant / Dynamic inside a tuple surface as chcol values —
		// reuse the dynamic walker.
		addAnyKV(arr, k, e.Interface(), 0)
	case kindString:
		kvAddStr(arr, k, e.String())
	case kindInt8, kindInt16, kindInt32, kindInt64:
		kvAddLong(arr, k, e.Int())
	case kindUInt8, kindUInt16, kindUInt32, kindUInt64:
		kvAddULong(arr, k, e.Uint())
	case kindFloat32, kindFloat64:
		kvAddDouble(arr, k, e.Float())
	case kindBool:
		if e.Bool() {
			kvAddULong(arr, k, 1)
		} else {
			kvAddULong(arr, k, 0)
		}
	default: // DateTime*, UUID, IP, Decimal, BigInt → formatted strings
		kvAddStr(arr, k, reflectScalarString(e, m))
	}
}

// ── Runtime stats snapshot ─────────────────────────────────────────────────
//
// Lives here (not stats.go) because it builds a PHP array through the cgo
// helpers above, which are file-static. The counter state it reads is defined
// in stats.go.

//export clickhouse_stats
func clickhouse_stats() (ret unsafe.Pointer) {
	defer nullPanicGuard(&ret)

	root := C.ch_new_array(6)

	poolMu.Lock()
	p := pool
	poolMu.Unlock()
	connected := p != nil

	kvAddLong(root, phpKey{s: "connected"}, boolToInt(connected))
	kvAddLong(root, phpKey{s: "uptime_seconds"}, int64(time.Since(processStart).Seconds()))
	serverVerMu.Lock()
	sv := serverVer
	serverVerMu.Unlock()
	kvAddStr(root, phpKey{s: "server_version"}, sv)

	connsMu.Lock()
	nconn := int64(len(conns))
	connsMu.Unlock()
	kvAddLong(root, phpKey{s: "named_connections"}, nconn)

	// Open handles + reaper state — the primary leak signal.
	cursorsMu.Lock()
	nCursors := int64(len(cursors))
	cursorsMu.Unlock()
	batchesMu.Lock()
	nBatches := int64(len(batches))
	batchesMu.Unlock()
	h := C.ch_new_array(5)
	kvAddLong(h, phpKey{s: "cursors_open"}, nCursors)
	kvAddLong(h, phpKey{s: "batches_open"}, nBatches)
	kvAddLong(h, phpKey{s: "idle_ttl_seconds"}, int64(handleIdleTTL.Seconds()))
	kvAddLong(h, phpKey{s: "last_reap_unix"}, atomic.LoadInt64(&statLastReapUnix))
	kvAddLong(h, phpKey{s: "last_reap_count"}, atomic.LoadInt64(&statLastReapCount))
	kvAddArr(root, phpKey{s: "handles"}, h)

	// Driver pool gauges (empty object when not connected).
	pl := C.ch_new_array(4)
	if connected {
		s := p.Stats()
		kvAddLong(pl, phpKey{s: "open"}, int64(s.Open))
		kvAddLong(pl, phpKey{s: "idle"}, int64(s.Idle))
		kvAddLong(pl, phpKey{s: "max_open_conns"}, int64(s.MaxOpenConns))
		kvAddLong(pl, phpKey{s: "max_idle_conns"}, int64(s.MaxIdleConns))
	}
	kvAddArr(root, phpKey{s: "pool"}, pl)

	// Lifetime counters since process boot.
	c := C.ch_new_array(7)
	kvAddLong(c, phpKey{s: "queries"}, atomic.LoadInt64(&statQueries))
	kvAddLong(c, phpKey{s: "inserts"}, atomic.LoadInt64(&statInserts))
	kvAddLong(c, phpKey{s: "execs"}, atomic.LoadInt64(&statExecs))
	kvAddLong(c, phpKey{s: "async_inserts"}, atomic.LoadInt64(&statAsyncInserts))
	kvAddLong(c, phpKey{s: "cursors_opened"}, atomic.LoadInt64(&statCursorsOpened))
	kvAddLong(c, phpKey{s: "batches_opened"}, atomic.LoadInt64(&statBatchesOpened))
	kvAddLong(c, phpKey{s: "errors"}, atomic.LoadInt64(&statErrors))
	kvAddArr(root, phpKey{s: "counters"}, c)

	// Aggregate query latency (µs) — operations timed, summed, and worst-case.
	// avg = total_us / operations. Cheap lock-free accounting; no per-query
	// push callback (calling PHP from the extension is fragile in worker mode).
	tm := C.ch_new_array(3)
	kvAddLong(tm, phpKey{s: "operations"}, atomic.LoadInt64(&statTimedOps))
	kvAddLong(tm, phpKey{s: "total_us"}, atomic.LoadInt64(&statQueryDurationUs))
	kvAddLong(tm, phpKey{s: "max_us"}, atomic.LoadInt64(&statQueryMaxUs))
	kvAddArr(root, phpKey{s: "timing"}, tm)

	return unsafe.Pointer(root)
}

// ── Shared helpers ────────────────────────────────────────────────────────────

var _zeroByte = [1]byte{0}

func safeCStr(s string) *C.char {
	if len(s) == 0 {
		return (*C.char)(unsafe.Pointer(&_zeroByte[0]))
	}
	return (*C.char)(unsafe.Pointer(unsafe.StringData(s)))
}
