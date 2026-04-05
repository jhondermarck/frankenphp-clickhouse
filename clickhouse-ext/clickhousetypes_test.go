package clickhousephp

import (
	"testing"
	"time"
)

func TestAppendTimeRaw(t *testing.T) {
	cases := []time.Time{
		time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		time.Date(2024, 6, 1, 0, 0, 0, 500000000, time.UTC),
		time.Date(2023, 12, 31, 23, 59, 59, 999999999, time.UTC),
		time.Date(2024, 1, 15, 10, 30, 0, 123000000, time.UTC),
		time.Date(2024, 1, 15, 10, 30, 0, 0, time.FixedZone("", 7200)),
		time.Date(2024, 1, 15, 10, 30, 0, 0, time.FixedZone("", -19800)),
	}

	for _, tt := range cases {
		t.Run(tt.Format(time.RFC3339Nano), func(t *testing.T) {
			got := string(appendTimeRaw(nil, tt))
			want := tt.Format(time.RFC3339Nano)
			if got != want {
				t.Errorf("appendTimeRaw\n  got  %s\n  want %s", got, want)
			}
		})
	}
}

func BenchmarkAppendTimeRaw(b *testing.B) {
	t := time.Date(2024, 3, 15, 12, 30, 45, 0, time.UTC)
	var buf [32]byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		appendTimeRaw(buf[:0], t)
	}
}
