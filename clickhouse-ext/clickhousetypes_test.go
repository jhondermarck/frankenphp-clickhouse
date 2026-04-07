package clickhousephp

import (
	"testing"
	"time"
)

func TestAppendClickHouseDateTime(t *testing.T) {
	tests := []struct {
		name string
		t    time.Time
		want string
	}{
		{"UTC", time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC), "2024-01-01 08:00:00"},
		{"end of year", time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC), "2024-12-31 23:59:59"},
		{"with TZ", time.Date(2024, 3, 15, 12, 30, 45, 0, time.FixedZone("CET", 2*3600)), "2024-03-15 12:30:45"},
	}
	for _, tt := range tests {
		got := string(appendClickHouseDateTime(nil, tt.t))
		if got != tt.want {
			t.Errorf("%s: got %q, want %q", tt.name, got, tt.want)
		}
	}
}

func TestAppendClickHouseDateTime64(t *testing.T) {
	tests := []struct {
		name string
		t    time.Time
		want string
	}{
		{"no subsec", time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC), "2024-01-01 08:00:00.000000"},
		{"500ms", time.Date(2024, 1, 1, 8, 0, 0, 500_000_000, time.UTC), "2024-01-01 08:00:00.500000"},
		{"123456us", time.Date(2024, 1, 1, 8, 0, 0, 123_456_000, time.UTC), "2024-01-01 08:00:00.123456"},
		{"999999us", time.Date(2024, 1, 1, 8, 0, 0, 999_999_000, time.UTC), "2024-01-01 08:00:00.999999"},
	}
	for _, tt := range tests {
		got := string(appendClickHouseDateTime64(nil, tt.t))
		if got != tt.want {
			t.Errorf("%s: got %q, want %q", tt.name, got, tt.want)
		}
	}
}

func BenchmarkAppendClickHouseDateTime(b *testing.B) {
	buf := make([]byte, 0, 32)
	t := time.Date(2024, 3, 15, 12, 30, 45, 0, time.UTC)
	for i := 0; i < b.N; i++ {
		buf = appendClickHouseDateTime(buf[:0], t)
	}
}
