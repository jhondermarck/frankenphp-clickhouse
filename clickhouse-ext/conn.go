package clickhousephp

/*
#include <stdlib.h>
#include "clickhousephp.h"
*/
import "C"
import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"os"
	"sync"
	"time"
	"unsafe"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/dunglas/frankenphp"
)

// ── Extra named connections (clickhouse_open / clickhouse_close) ──────────────

type extraConn struct {
	conn    clickhouse.Conn
	timeout time.Duration
}

var (
	connsMu sync.Mutex
	conns   = map[int64]*extraConn{}
	connSeq int64
)

// resolveConn returns the connection for an optional handle (<= 0 →
// the default connection) plus its DSN-level timeout.
func resolveConn(id int64) (clickhouse.Conn, time.Duration, error) {
	if id <= 0 {
		poolMu.Lock()
		defer poolMu.Unlock()
		if pool == nil {
			return nil, 0, fmt.Errorf("Client not connected")
		}
		return pool, connTimeout, nil
	}
	connsMu.Lock()
	defer connsMu.Unlock()
	ec, ok := conns[id]
	if !ok {
		return nil, 0, fmt.Errorf("unknown connection %d", id)
	}
	return ec.conn, ec.timeout, nil
}

//export clickhouse_ping
func clickhouse_ping(connID C.int64_t) (ret unsafe.Pointer) {
	defer phpPanicGuard(&ret)
	client, timeout, err := resolveConn(int64(connID))
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}
	ctx := context.Background()
	cancel := func() {}
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()
	if err := client.Ping(ctx); err != nil {
		return chError("ping failed: ", err)
	}
	return frankenphp.PHPString("Ok", false)
}

//export clickhouse_server_version
func clickhouse_server_version(connID C.int64_t) (ret unsafe.Pointer) {
	defer phpPanicGuard(&ret)
	client, _, err := resolveConn(int64(connID))
	if err != nil {
		return frankenphp.PHPString("ERROR: "+err.Error(), false)
	}
	v, err := client.ServerVersion()
	if err != nil {
		return chError("", err)
	}
	return frankenphp.PHPString(fmt.Sprintf("%d.%d.%d", v.Version.Major, v.Version.Minor, v.Version.Patch), false)
}

//export clickhouse_open
func clickhouse_open(dsn *C.zend_string) (ret C.int64_t) {
	defer idPanicGuard(&ret)
	dsnURL := frankenphp.GoString(unsafe.Pointer(dsn))
	conn, timeout, err := connectClickHouse(dsnURL)
	if err != nil {
		setChError("", err)
		return -1
	}
	connsMu.Lock()
	connSeq++
	id := connSeq
	conns[id] = &extraConn{conn: conn, timeout: timeout}
	connsMu.Unlock()
	return C.int64_t(id)
}

//export clickhouse_close
func clickhouse_close(id C.int64_t) (ret unsafe.Pointer) {
	defer phpPanicGuard(&ret)
	connsMu.Lock()
	ec, ok := conns[int64(id)]
	delete(conns, int64(id))
	connsMu.Unlock()
	if !ok {
		return frankenphp.PHPString(fmt.Sprintf("ERROR: unknown connection %d", int64(id)), false)
	}
	ec.conn.Close()
	return frankenphp.PHPString("Ok", false)
}

// connectClickHouse builds a driver pool from the DSN. Driver-native
// parameters pass straight through clickhouse.ParseDSN: multi-host
// ("h1:9000,h2:9000"), connection_open_strategy (in_order/round_robin/
// random), max_open_conns, max_idle_conns, conn_max_lifetime,
// compress (lz4/zstd/gzip/none), secure, skip_verify, dial_timeout,
// read_timeout… Any other unknown parameter becomes a ClickHouse query
// setting (driver behavior), so extension-level parameters below are
// stripped first:
//
//	timeout      per-call timeout (Go duration)
//	ca_cert      path to a PEM CA bundle (implies TLS)
//	client_cert  path to a PEM client certificate (mutual TLS)
//	client_key   path to the matching client key
//
// SECURITY: the DSN is trusted configuration. ca_cert/client_cert/client_key
// are read from the host filesystem, so a DSN built from untrusted input
// would be an arbitrary-file-read probe. Never construct a DSN from
// user-supplied data — treat it like a connection string in a config file.
func connectClickHouse(dsn string) (clickhouse.Conn, time.Duration, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, 0, fmt.Errorf("invalid DSN: %w", err)
	}
	q := u.Query()

	timeout := time.Duration(0)
	if v := q.Get("timeout"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			return nil, 0, fmt.Errorf("invalid timeout %q (use a Go duration, e.g. 30s)", v)
		}
		timeout = d
	}
	caCert := q.Get("ca_cert")
	clientCert := q.Get("client_cert")
	clientKey := q.Get("client_key")
	for _, k := range []string{"timeout", "ca_cert", "client_cert", "client_key"} {
		q.Del(k)
	}
	u.RawQuery = q.Encode()

	opts, err := clickhouse.ParseDSN(u.String())
	if err != nil {
		return nil, 0, fmt.Errorf("invalid DSN: %w", err)
	}
	// Preserve the historic default: LZ4 on the native protocol unless
	// explicitly disabled (ParseDSN leaves compression off when the
	// param is absent; compress=false/none disables it).
	if opts.Compression == nil {
		opts.Compression = &clickhouse.Compression{Method: clickhouse.CompressionLZ4}
	}

	if caCert != "" || clientCert != "" || clientKey != "" {
		if opts.TLS == nil {
			opts.TLS = &tls.Config{}
		}
		if caCert != "" {
			pem, err := os.ReadFile(caCert)
			if err != nil {
				return nil, 0, fmt.Errorf("ca_cert: %w", err)
			}
			roots := x509.NewCertPool()
			if !roots.AppendCertsFromPEM(pem) {
				return nil, 0, fmt.Errorf("ca_cert: no PEM certificates in %s", caCert)
			}
			opts.TLS.RootCAs = roots
		}
		if clientCert != "" || clientKey != "" {
			if clientCert == "" || clientKey == "" {
				return nil, 0, fmt.Errorf("client_cert and client_key must both be set")
			}
			cert, err := tls.LoadX509KeyPair(clientCert, clientKey)
			if err != nil {
				return nil, 0, fmt.Errorf("client certificate: %w", err)
			}
			opts.TLS.Certificates = []tls.Certificate{cert}
		}
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, 0, fmt.Errorf("open failed: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return nil, 0, fmt.Errorf("ping failed: %w", err)
	}

	return conn, timeout, nil
}
