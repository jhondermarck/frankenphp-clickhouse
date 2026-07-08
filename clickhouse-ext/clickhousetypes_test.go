package clickhousephp

import (
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
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

func TestParseColMeta(t *testing.T) {
	cases := []struct {
		dbType   string
		kind     colKind
		nullable bool
	}{
		{"String", kindString, false},
		{"FixedString(16)", kindString, false},
		{"Enum8('a' = 1, 'b' = 2)", kindString, false},
		{"DateTime", kindDateTime, false},
		{"DateTime('Europe/Paris')", kindDateTime, false},
		{"DateTime64(3, 'UTC')", kindDateTime64, false},
		{"Date", kindDateTime, false},
		{"Date32", kindDateTime, false},
		{"UUID", kindUUID, false},
		{"Bool", kindBool, false},
		{"Decimal(18, 4)", kindDecimal, false},
		{"Int128", kindBigInt, false},
		{"UInt256", kindBigInt, false},
		{"Nullable(Int128)", kindBigInt, true},
		{"JSON", kindJSON, false},
		{"JSON(max_dynamic_paths=100)", kindJSON, false},
		{"Nullable(String)", kindString, true},
		{"LowCardinality(String)", kindString, false},
		{"LowCardinality(Nullable(String))", kindString, true},
		{"Nullable(LowCardinality(String))", kindString, true},
		{"LowCardinality(FixedString(3))", kindString, false},
	}
	for _, tt := range cases {
		t.Run(tt.dbType, func(t *testing.T) {
			m, err := parseColMeta(tt.dbType)
			if err != nil {
				t.Fatalf("parseColMeta(%q) error: %v", tt.dbType, err)
			}
			if m.kind != tt.kind || m.nullable != tt.nullable {
				t.Errorf("parseColMeta(%q) = {kind:%d nullable:%v}, want {kind:%d nullable:%v}",
					tt.dbType, m.kind, m.nullable, tt.kind, tt.nullable)
			}
		})
	}
}

func TestParseColMetaArray(t *testing.T) {
	m, err := parseColMeta("Array(LowCardinality(Nullable(String)))")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.kind != kindArray || m.inner == nil || m.inner.kind != kindString || !m.inner.nullable {
		t.Errorf("Array(LowCardinality(Nullable(String))) parsed incorrectly: %+v", m)
	}
}

func TestParseColMetaUnsupported(t *testing.T) {
	for _, dbType := range []string{"Tuple(UInt8)", "Nothing", "Map(String)"} {
		if _, err := parseColMeta(dbType); err == nil {
			t.Errorf("parseColMeta(%q) should return an error", dbType)
		}
	}
}

func TestParseColMetaMap(t *testing.T) {
	cases := []struct {
		dbType  string
		keyKind colKind
		valKind colKind
	}{
		{"Map(String, String)", kindString, kindString},
		{"Map(String, UInt64)", kindString, kindUInt64},
		{"Map(UInt8, String)", kindUInt8, kindString},
		{"Map(LowCardinality(String), String)", kindString, kindString},
		{"Map(String, Array(String))", kindString, kindArray},
		{"Map(String, Map(String, UInt32))", kindString, kindMap},
		{"Map(String, Decimal(18, 4))", kindString, kindDecimal},
		{"Map(String, Enum8('a' = 1, 'b' = 2))", kindString, kindString},
	}
	for _, tt := range cases {
		t.Run(tt.dbType, func(t *testing.T) {
			m, err := parseColMeta(tt.dbType)
			if err != nil {
				t.Fatalf("parseColMeta(%q) error: %v", tt.dbType, err)
			}
			if m.kind != kindMap {
				t.Fatalf("kind = %d, want kindMap", m.kind)
			}
			if m.inner == nil || m.inner.kind != tt.keyKind {
				t.Errorf("key kind mismatch: %+v, want %d", m.inner, tt.keyKind)
			}
			if m.value == nil || m.value.kind != tt.valKind {
				t.Errorf("value kind mismatch: %+v, want %d", m.value, tt.valKind)
			}
		})
	}

	// Nullable map value
	m, err := parseColMeta("Map(String, Nullable(String))")
	if err != nil {
		t.Fatalf("Nullable value: %v", err)
	}
	if !m.value.nullable {
		t.Error("Map(String, Nullable(String)) value should be nullable")
	}
}

func TestSplitTopLevelComma(t *testing.T) {
	cases := []struct{ in, k, v string }{
		{"String, UInt64", "String", "UInt64"},
		{"String, Enum8('a' = 1, 'b' = 2)", "String", "Enum8('a' = 1, 'b' = 2)"},
		{"LowCardinality(String), Array(String)", "LowCardinality(String)", "Array(String)"},
		{"String, Map(String, String)", "String", "Map(String, String)"},
	}
	for _, tt := range cases {
		k, v, ok := splitTopLevelComma(tt.in)
		if !ok || k != tt.k || v != tt.v {
			t.Errorf("splitTopLevelComma(%q) = (%q, %q, %v), want (%q, %q)", tt.in, k, v, ok, tt.k, tt.v)
		}
	}
	if _, _, ok := splitTopLevelComma("NoCommaHere"); ok {
		t.Error("splitTopLevelComma without top-level comma should return ok=false")
	}
}

// fakeRows is a minimal driver.Rows for exercising the idle reaper.
type fakeRows struct{ closed bool }

func (f *fakeRows) Next() bool                       { return false }
func (f *fakeRows) HasData() bool                    { return false }
func (f *fakeRows) Scan(dest ...any) error           { return nil }
func (f *fakeRows) ScanStruct(dest any) error        { return nil }
func (f *fakeRows) ColumnTypes() []driver.ColumnType { return nil }
func (f *fakeRows) Totals(dest ...any) error         { return nil }
func (f *fakeRows) Columns() []string                { return nil }
func (f *fakeRows) Close() error                     { f.closed = true; return nil }
func (f *fakeRows) Err() error                       { return nil }

func TestReapIdleHandles(t *testing.T) {
	idle := &cursorState{rows: &fakeRows{}, cancel: func() {}, lastUsed: time.Now().Add(-time.Hour)}
	fresh := &cursorState{rows: &fakeRows{}, cancel: func() {}, lastUsed: time.Now()}
	cursorsMu.Lock()
	cursors[9001] = idle
	cursors[9002] = fresh
	cursorsMu.Unlock()
	defer func() {
		cursorsMu.Lock()
		delete(cursors, 9001)
		delete(cursors, 9002)
		cursorsMu.Unlock()
	}()

	if n := reapIdleHandles(time.Now()); n != 1 {
		t.Fatalf("reaped %d handles, want 1", n)
	}
	if !idle.rows.(*fakeRows).closed {
		t.Error("idle cursor's rows were not closed")
	}
	if !idle.done {
		t.Error("idle cursor not marked done")
	}
	cursorsMu.Lock()
	_, idleThere := cursors[9001]
	_, freshThere := cursors[9002]
	cursorsMu.Unlock()
	if idleThere {
		t.Error("idle cursor still registered")
	}
	if !freshThere {
		t.Error("fresh cursor was wrongly reaped")
	}
}

// TestRegistryConcurrency hammers the cursor/batch registries and the reaper
// from many goroutines to shake out data races under -race.
func TestRegistryConcurrency(t *testing.T) {
	var writers, reaper sync.WaitGroup
	stop := make(chan struct{})

	// Reaper sweeping continuously (separate WaitGroup: it only exits after
	// the writers are done and stop is closed).
	reaper.Add(1)
	go func() {
		defer reaper.Done()
		for {
			select {
			case <-stop:
				return
			default:
				reapIdleHandles(time.Now())
			}
		}
	}()

	// Writers registering and dropping cursors + batches concurrently.
	for g := 0; g < 8; g++ {
		writers.Add(1)
		go func(base int64) {
			defer writers.Done()
			for i := int64(0); i < 1000; i++ {
				id := base*100000 + i
				cur := &cursorState{rows: &fakeRows{}, cancel: func() {}, lastUsed: time.Now()}
				cursorsMu.Lock()
				cursors[id] = cur
				cursorsMu.Unlock()

				b := &batchState{cancel: func() {}, lastUsed: time.Now(), done: true}
				batchesMu.Lock()
				batches[id] = b
				batchesMu.Unlock()

				cur.mu.Lock()
				cur.releaseResources()
				cur.mu.Unlock()
				cursorsMu.Lock()
				delete(cursors, id)
				cursorsMu.Unlock()
				batchesMu.Lock()
				delete(batches, id)
				batchesMu.Unlock()
			}
		}(int64(g))
	}
	writers.Wait()
	close(stop)
	reaper.Wait()
}
