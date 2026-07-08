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

static void ch_add_row(
    zend_array*      res,
    zend_string**    keys,
    const uint8_t*   types,
    const char*      sbuf,
    const uint32_t*  soff,
    const uint32_t*  slen,
    const int64_t*   ivals,
    const uint64_t*  uvals,
    const double*    fvals,
    int              n)
{
    zval row;
    array_init_size(&row, (uint32_t)n);
    for (int i = 0; i < n; i++) {
        zval z;
        switch (types[i]) {
        case CH_STR:
            ZVAL_STR(&z, zend_string_init(sbuf + soff[i], slen[i], 0));
            break;
        case CH_INT:
            ZVAL_LONG(&z, (zend_long)ivals[i]);
            break;
        case CH_UINT:
            if (uvals[i] <= (uint64_t)ZEND_LONG_MAX)
                ZVAL_LONG(&z, (zend_long)uvals[i]);
            else
                ZVAL_DOUBLE(&z, (double)uvals[i]);
            break;
        case CH_FLOAT:
            ZVAL_DOUBLE(&z, fvals[i]);
            break;
        case CH_ARRAY:
            ZVAL_ARR(&z, (zend_array*)(uintptr_t)uvals[i]);
            break;
        default: // CH_NULL
            ZVAL_NULL(&z);
            break;
        }
        zend_hash_add_new(Z_ARRVAL(row), keys[i], &z);
    }
    zend_hash_next_index_insert(res, &row);
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

func addGenericRow(arr unsafe.Pointer, keys []*C.zend_string,
	types []C.uint8_t, sbuf []byte,
	soff []C.uint32_t, slen []C.uint32_t,
	ivals []C.int64_t, uvals []C.uint64_t, fvals []C.double, n int,
) {
	sp := (*C.char)(unsafe.Pointer(&_emptySbuf[0]))
	if len(sbuf) > 0 {
		sp = (*C.char)(unsafe.Pointer(unsafe.SliceData(sbuf)))
	}
	C.ch_add_row(
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
		arr := buildAnyMap(jm)
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
	}
}

// ── Array builder ────────────────────────────────────────────────────────────

func arrAddStr(arr *C.zend_array, s string) {
	C.ch_arr_add_str(arr, safeCStr(s), C.uint32_t(len(s)))
}

func buildPHPArray(dest interface{}, inner *colMeta) unsafe.Pointer {
	if inner == nil {
		return unsafe.Pointer(C.ch_new_array(0))
	}
	// Nested arrays and maps have no typed fast path — go generic.
	if inner.kind == kindArray || inner.kind == kindMap {
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

// buildAnyMap converts a JSON NestedMap into a PHP associative array
// (keys sorted for deterministic output).
func buildAnyMap(m map[string]any) unsafe.Pointer {
	arr := C.ch_new_array(C.uint32_t(len(m)))
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		addAnyKV((*C.zend_array)(arr), phpKey{s: k}, m[k])
	}
	return unsafe.Pointer(arr)
}

// addAnyKV adds one dynamically-typed value (JSON leaf or subtree)
// under a key. Lists use sequential int keys, which PHP stores as a
// packed indexed array.
func addAnyKV(arr *C.zend_array, k phpKey, v any) {
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
		kvAddArr(arr, k, (*C.zend_array)(buildAnyMap(t)))
	case []any:
		sub := C.ch_new_array(C.uint32_t(len(t)))
		for j, e := range t {
			addAnyKV((*C.zend_array)(sub), phpKey{isInt: true, i: int64(j)}, e)
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
			addAnyKV(arr, k, rv.Elem().Interface())
		case reflect.Slice, reflect.Array:
			sub := C.ch_new_array(C.uint32_t(rv.Len()))
			for j := 0; j < rv.Len(); j++ {
				addAnyKV((*C.zend_array)(sub), phpKey{isInt: true, i: int64(j)}, rv.Index(j).Interface())
			}
			kvAddArr(arr, k, (*C.zend_array)(sub))
		case reflect.Map:
			sub := C.ch_new_array(C.uint32_t(rv.Len()))
			iter := rv.MapRange()
			for iter.Next() {
				addAnyKV((*C.zend_array)(sub), phpKey{s: fmt.Sprintf("%v", iter.Key().Interface())}, iter.Value().Interface())
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

// ── Shared helpers ────────────────────────────────────────────────────────────

var _zeroByte = [1]byte{0}

func safeCStr(s string) *C.char {
	if len(s) == 0 {
		return (*C.char)(unsafe.Pointer(&_zeroByte[0]))
	}
	return (*C.char)(unsafe.Pointer(unsafe.StringData(s)))
}
