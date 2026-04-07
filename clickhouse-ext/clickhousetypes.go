package clickhousephp

import (
	"fmt"
	"net/netip"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type colKind int

const (
	kindString colKind = iota
	kindDateTime
	kindDateTime64
	kindFloat32
	kindFloat64
	kindInt8
	kindInt16
	kindInt32
	kindInt64
	kindUInt8
	kindUInt16
	kindUInt32
	kindUInt64
	kindBool
	kindUUID
	kindIPv4
	kindIPv6
	kindDecimal
	kindArray
)

type colMeta struct {
	kind     colKind
	nullable bool
	inner    *colMeta // for Array(T)
}

func parseColMeta(dbType string) (colMeta, error) {
	raw := dbType
	nullable := false
	if strings.HasPrefix(raw, "Nullable(") {
		nullable = true
		raw = raw[len("Nullable(") : len(raw)-1]
	}
	if strings.HasPrefix(raw, "LowCardinality(") {
		raw = raw[len("LowCardinality(") : len(raw)-1]
	}
	if strings.HasPrefix(raw, "Array(") {
		innerType := raw[len("Array(") : len(raw)-1]
		inner, err := parseColMeta(innerType)
		if err != nil {
			return colMeta{}, err
		}
		return colMeta{kind: kindArray, nullable: nullable, inner: &inner}, nil
	}
	if strings.HasPrefix(raw, "DateTime64") {
		return colMeta{kind: kindDateTime64, nullable: nullable}, nil
	}
	if strings.HasPrefix(raw, "Enum8") || strings.HasPrefix(raw, "Enum16") {
		raw = "String" // driver returns enum name as string
	}
	if strings.HasPrefix(raw, "Decimal") {
		raw = "Decimal"
	}
	var k colKind
	switch raw {
	case "String", "FixedString":
		k = kindString
	case "DateTime", "Date", "Date32":
		k = kindDateTime
	case "Float32":
		k = kindFloat32
	case "Float64":
		k = kindFloat64
	case "Int8":
		k = kindInt8
	case "Int16":
		k = kindInt16
	case "Int32":
		k = kindInt32
	case "Int64":
		k = kindInt64
	case "UInt8":
		k = kindUInt8
	case "UInt16":
		k = kindUInt16
	case "UInt32":
		k = kindUInt32
	case "UInt64":
		k = kindUInt64
	case "Bool":
		k = kindBool
	case "UUID":
		k = kindUUID
	case "IPv4":
		k = kindIPv4
	case "IPv6":
		k = kindIPv6
	case "Decimal":
		k = kindDecimal
	default:
		return colMeta{}, fmt.Errorf("unsupported type: %s", dbType)
	}
	return colMeta{kind: k, nullable: nullable}, nil
}

func allocScanDest(m colMeta) interface{} {
	if m.nullable {
		switch m.kind {
		case kindString:
			return new(*string)
		case kindDateTime:
			return new(*time.Time)
		case kindDateTime64:
			return new(*time.Time)
		case kindFloat32:
			return new(*float32)
		case kindFloat64:
			return new(*float64)
		case kindInt8:
			return new(*int8)
		case kindInt16:
			return new(*int16)
		case kindInt32:
			return new(*int32)
		case kindInt64:
			return new(*int64)
		case kindUInt8:
			return new(*uint8)
		case kindUInt16:
			return new(*uint16)
		case kindUInt32:
			return new(*uint32)
		case kindUInt64:
			return new(*uint64)
		case kindBool:
			return new(*bool)
		case kindUUID:
			return new(*uuid.UUID)
		case kindIPv4, kindIPv6:
			return new(*netip.Addr)
		case kindDecimal:
			return new(*decimal.Decimal)
		}
	}
	switch m.kind {
	case kindString:
		return new(string)
	case kindDateTime:
		return new(time.Time)
	case kindDateTime64:
		return new(time.Time)
	case kindFloat32:
		return new(float32)
	case kindFloat64:
		return new(float64)
	case kindInt8:
		return new(int8)
	case kindInt16:
		return new(int16)
	case kindInt32:
		return new(int32)
	case kindInt64:
		return new(int64)
	case kindUInt8:
		return new(uint8)
	case kindUInt16:
		return new(uint16)
	case kindUInt32:
		return new(uint32)
	case kindUInt64:
		return new(uint64)
	case kindBool:
		return new(bool)
	case kindUUID:
		return new(uuid.UUID)
	case kindIPv4, kindIPv6:
		return new(netip.Addr)
	case kindDecimal:
		return new(decimal.Decimal)
	case kindArray:
		return allocArrayScanDest(m.inner)
	}
	return new(string)
}

func allocArrayScanDest(inner *colMeta) interface{} {
	if inner == nil {
		return new(interface{})
	}
	if inner.nullable {
		switch inner.kind {
		case kindString:
			return new([]*string)
		case kindDateTime:
			return new([]*time.Time)
		case kindDateTime64:
			return new([]*time.Time)
		case kindFloat32:
			return new([]*float32)
		case kindFloat64:
			return new([]*float64)
		case kindInt8:
			return new([]*int8)
		case kindInt16:
			return new([]*int16)
		case kindInt32:
			return new([]*int32)
		case kindInt64:
			return new([]*int64)
		case kindUInt8:
			return new([]*uint8)
		case kindUInt16:
			return new([]*uint16)
		case kindUInt32:
			return new([]*uint32)
		case kindUInt64:
			return new([]*uint64)
		case kindBool:
			return new([]*bool)
		case kindUUID:
			return new([]*uuid.UUID)
		case kindIPv4, kindIPv6:
			return new([]*netip.Addr)
		case kindDecimal:
			return new([]*decimal.Decimal)
		}
	}
	switch inner.kind {
	case kindString:
		return new([]string)
	case kindDateTime:
		return new([]time.Time)
	case kindDateTime64:
		return new([]time.Time)
	case kindFloat32:
		return new([]float32)
	case kindFloat64:
		return new([]float64)
	case kindInt8:
		return new([]int8)
	case kindInt16:
		return new([]int16)
	case kindInt32:
		return new([]int32)
	case kindInt64:
		return new([]int64)
	case kindUInt8:
		return new([]uint8)
	case kindUInt16:
		return new([]uint16)
	case kindUInt32:
		return new([]uint32)
	case kindUInt64:
		return new([]uint64)
	case kindBool:
		return new([]bool)
	case kindUUID:
		return new([]uuid.UUID)
	case kindIPv4, kindIPv6:
		return new([]netip.Addr)
	case kindDecimal:
		return new([]decimal.Decimal)
	}
	return new(interface{})
}

// resetNullableDest sets the inner pointer of a nullable scan destination to nil.
// This is needed because some drivers (e.g. clickhouse-go for UUID) don't reset
// the pointer when scanning NULL after a non-NULL row.
func resetNullableDest(k colKind, dest interface{}) {
	switch k {
	case kindString:
		*(dest.(*(*string))) = nil
	case kindDateTime:
		*(dest.(*(*time.Time))) = nil
	case kindDateTime64:
		*(dest.(*(*time.Time))) = nil
	case kindFloat32:
		*(dest.(*(*float32))) = nil
	case kindFloat64:
		*(dest.(*(*float64))) = nil
	case kindInt8:
		*(dest.(*(*int8))) = nil
	case kindInt16:
		*(dest.(*(*int16))) = nil
	case kindInt32:
		*(dest.(*(*int32))) = nil
	case kindInt64:
		*(dest.(*(*int64))) = nil
	case kindUInt8:
		*(dest.(*(*uint8))) = nil
	case kindUInt16:
		*(dest.(*(*uint16))) = nil
	case kindUInt32:
		*(dest.(*(*uint32))) = nil
	case kindUInt64:
		*(dest.(*(*uint64))) = nil
	case kindBool:
		*(dest.(*(*bool))) = nil
	case kindUUID:
		*(dest.(*(*uuid.UUID))) = nil
	case kindIPv4, kindIPv6:
		*(dest.(*(*netip.Addr))) = nil
	case kindDecimal:
		*(dest.(*(*decimal.Decimal))) = nil
	}
}

// appendClickHouseDateTime appends t as "YYYY-MM-DD HH:MM:SS" to b.
func appendClickHouseDateTime(b []byte, t time.Time) []byte {
	year, month, day := t.Date()
	hour, min, sec := t.Clock()
	return append(b,
		byte('0'+year/1000),
		byte('0'+(year/100)%10),
		byte('0'+(year/10)%10),
		byte('0'+year%10),
		'-',
		byte('0'+int(month)/10),
		byte('0'+int(month)%10),
		'-',
		byte('0'+day/10),
		byte('0'+day%10),
		' ',
		byte('0'+hour/10),
		byte('0'+hour%10),
		':',
		byte('0'+min/10),
		byte('0'+min%10),
		':',
		byte('0'+sec/10),
		byte('0'+sec%10),
	)
}

// appendClickHouseDateTime64 appends t as "YYYY-MM-DD HH:MM:SS.nnnnnn" to b.
// Always includes 6-digit microsecond precision.
func appendClickHouseDateTime64(b []byte, t time.Time) []byte {
	b = appendClickHouseDateTime(b, t)
	us := t.Nanosecond() / 1000 // microseconds
	return append(b, '.',
		byte('0'+us/100000),
		byte('0'+(us/10000)%10),
		byte('0'+(us/1000)%10),
		byte('0'+(us/100)%10),
		byte('0'+(us/10)%10),
		byte('0'+us%10),
	)
}
