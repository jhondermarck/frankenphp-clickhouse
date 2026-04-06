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

// ── Key interning ────────────────────────────────────────────────────────────

static zend_string* ch_intern_key(const char* name, size_t len) {
    return zend_string_init_interned(name, len, 1);
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
	"net/netip"
	"time"
	"unsafe"

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

func internKey(name string) *C.zend_string {
	return C.ch_intern_key(safeCStr(name), C.size_t(len(name)))
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
	i     int,
	m     colMeta,
	dest  interface{},
	types []C.uint8_t,
	soff  []C.uint32_t,
	slen  []C.uint32_t,
	ivals []C.int64_t,
	uvals []C.uint64_t,
	fvals []C.double,
	sbuf  *[]byte,
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
		*sbuf = appendTimeRaw(*sbuf, *(dest.(*time.Time)))
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
	case kindArray:
		arr := buildPHPArray(dest, m.inner)
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
			str := string(appendTimeRaw(nil, v))
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
	return unsafe.Pointer(C.ch_new_array(0))
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
	// Fallback: unsupported nullable inner type → empty array
	return unsafe.Pointer(C.ch_new_array(0))
}

// ── Shared helpers ────────────────────────────────────────────────────────────

var _zeroByte = [1]byte{0}

func safeCStr(s string) *C.char {
	if len(s) == 0 {
		return (*C.char)(unsafe.Pointer(&_zeroByte[0]))
	}
	return (*C.char)(unsafe.Pointer(unsafe.StringData(s)))
}
