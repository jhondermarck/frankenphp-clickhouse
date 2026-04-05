package clickhousephp

import (
	"fmt"
	"strings"
	"time"
)

type colKind int

const (
	kindString colKind = iota
	kindDateTime
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
)

type colMeta struct {
	kind     colKind
	nullable bool
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
	if strings.HasPrefix(raw, "DateTime64") {
		raw = "DateTime"
	}
	var k colKind
	switch raw {
	case "String", "FixedString":
		k = kindString
	case "DateTime":
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
		}
	}
	switch m.kind {
	case kindString:
		return new(string)
	case kindDateTime:
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
	}
	return new(string)
}

// appendTimeRaw appends t in RFC3339Nano format (without quotes) to b.
func appendTimeRaw(b []byte, t time.Time) []byte {
	year, month, day := t.Date()
	hour, min, sec := t.Clock()
	ns := t.Nanosecond()
	_, offset := t.Zone()

	b = append(b,
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
		'T',
		byte('0'+hour/10),
		byte('0'+hour%10),
		':',
		byte('0'+min/10),
		byte('0'+min%10),
		':',
		byte('0'+sec/10),
		byte('0'+sec%10),
	)
	if ns != 0 {
		var d [9]byte
		n := ns
		for i := 8; i >= 0; i-- {
			d[i] = byte('0' + n%10)
			n /= 10
		}
		end := 9
		for end > 1 && d[end-1] == '0' {
			end--
		}
		b = append(b, '.')
		b = append(b, d[:end]...)
	}
	if offset == 0 {
		b = append(b, 'Z')
	} else {
		if offset < 0 {
			b = append(b, '-')
			offset = -offset
		} else {
			b = append(b, '+')
		}
		h := offset / 3600
		m := (offset % 3600) / 60
		b = append(b, byte('0'+h/10), byte('0'+h%10), ':', byte('0'+m/10), byte('0'+m%10))
	}
	return b
}
