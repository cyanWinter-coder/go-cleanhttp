// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package cleanhttp

import (
	"errors"
	"io"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// ErrGenerationRetired is returned to requests started through a retired
// Generation. Retiring a generation never interrupts requests that are
// already in flight; it only rejects new ones, with a real error rather than
// a fabricated response.
var ErrGenerationRetired = errors.New("cleanhttp: transport generation is retired")

// ErrDrainTimeout is returned by Generation.Drain when the generation still
// has active requests after the drain deadline. The generation keeps waiting
// and a later Drain call can be used to retry.
var ErrDrainTimeout = errors.New("cleanhttp: transport generation did not drain before the deadline")

// generationSeq provides process-wide unique generation IDs.
var generationSeq atomic.Uint64

// dialConfig captures the net.Dialer settings used to build a Transport so
// they can be reported in Summary; they cannot be read back out of an
// http.Transport once the DialContext func is set.
type dialConfig struct {
	timeout   time.Duration
	keepAlive time.Duration
}

// defaultPooledTransport builds the pooled transport used by both
// DefaultPooledTransport and the default Manager factory, so the clean
// defaults (proxy from environment, TLS handshake timeout, keep-alive) stay
// in sync between the stateless factories and managed generations.
func defaultPooledTransport() (*http.Transport, dialConfig) {
	dial := dialConfig{timeout: 30 * time.Second, keepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   dial.timeout,
			KeepAlive: dial.keepAlive,
			DualStack: true, //nolint:staticcheck
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
		MaxIdleConnsPerHost:   runtime.GOMAXPROCS(0) + 1,
	}
	return transport, dial
}

// Summary describes the health and configuration of a Generation. Transport
// fields are read back from the live transport, so they reflect any
// customization applied after construction.
type Summary struct {
	Generation uint64

	// Active is the number of requests currently in flight on this
	// generation. Retired is true once the generation has stopped accepting
	// new requests. Drained is true once Drain has completed and the idle
	// connections have been closed.
	Active  int
	Retired bool
	Drained bool

	MaxIdleConns          int
	MaxIdleConnsPerHost   int
	IdleConnTimeout       time.Duration
	TLSHandshakeTimeout   time.Duration
	ExpectContinueTimeout time.Duration
	DisableKeepAlives     bool
	ForceAttemptHTTP2     bool

	// ProxyFromEnvironment reports whether the transport resolves proxies
	// from the environment (its Proxy func is set).
	ProxyFromEnvironment bool

	// DialTimeout and DialKeepAlive are the net.Dialer settings the
	// transport was built with. They are zero for generations created from
	// a custom factory, where they cannot be determined.
	DialTimeout   time.Duration
	DialKeepAlive time.Duration
}

// Generation is a single, uniquely identified incarnation of a managed
// Transport and Client. Requests started through a generation are tracked so
// the generation can be drained safely once it is superseded: retiring stops
// new requests, and Drain waits for in-flight ones to finish before closing
// idle connections.
type Generation struct {
	id        uint64
	transport *http.Transport
	client    *http.Client
	dial      dialConfig

	mu       sync.Mutex
	active   int
	retired  bool
	idleDone bool // idle channel has been closed
	closed   bool // Drain completed and idle connections were closed
	idle     chan struct{}
}

func newGeneration(transport *http.Transport, dial dialConfig) *Generation {
	g := &Generation{
		id:        generationSeq.Add(1),
		transport: transport,
		dial:      dial,
		idle:      make(chan struct{}),
	}
	g.client = &http.Client{Transport: &generationRoundTripper{gen: g}}
	return g
}

// ID returns the generation's unique, monotonically increasing ID.
func (g *Generation) ID() uint64 {
	return g.id
}

// Client returns the http.Client bound to this generation's Transport.
func (g *Generation) Client() *http.Client {
	return g.client
}

// Transport returns the http.Transport backing this generation.
func (g *Generation) Transport() *http.Transport {
	return g.transport
}

// Active returns the number of requests currently in flight.
func (g *Generation) Active() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.active
}

// Summary returns a snapshot of the generation's health and configuration.
func (g *Generation) Summary() Summary {
	g.mu.Lock()
	defer g.mu.Unlock()
	return Summary{
		Generation:            g.id,
		Active:                g.active,
		Retired:               g.retired,
		Drained:               g.closed,
		MaxIdleConns:          g.transport.MaxIdleConns,
		MaxIdleConnsPerHost:   g.transport.MaxIdleConnsPerHost,
		IdleConnTimeout:       g.transport.IdleConnTimeout,
		TLSHandshakeTimeout:   g.transport.TLSHandshakeTimeout,
		ExpectContinueTimeout: g.transport.ExpectContinueTimeout,
		DisableKeepAlives:     g.transport.DisableKeepAlives,
		ForceAttemptHTTP2:     g.transport.ForceAttemptHTTP2,
		ProxyFromEnvironment:  g.transport.Proxy != nil,
		DialTimeout:           g.dial.timeout,
		DialKeepAlive:         g.dial.keepAlive,
	}
}

// retire stops the generation from accepting new requests. In-flight
// requests are untouched. It is idempotent.
func (g *Generation) retire() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.retired {
		return
	}
	g.retired = true
	if g.active == 0 {
		g.signalIdleLocked()
	}
}

// Drain retires the generation and waits for in-flight requests to complete,
// then closes all of the generation's idle connections. It returns the
// number of requests still in flight (zero on success).
//
// If timeout is positive and the generation has not drained by then, Drain
// returns the number of still-active requests along with ErrDrainTimeout;
// the new generation is never blocked by this. A timeout does not give up on
// the generation: a later Drain call resumes waiting. A non-positive timeout
// waits indefinitely.
//
// Drain is idempotent: once a generation has drained, repeated calls return
// immediately with (0, nil), and concurrent calls are safe.
func (g *Generation) Drain(timeout time.Duration) (int, error) {
	g.retire()

	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return 0, nil
	}
	idle := g.idle
	g.mu.Unlock()

	var deadline <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		deadline = timer.C
	}

	select {
	case <-idle:
		// All in-flight requests have completed, so only idle connections
		// remain; closing them cannot interrupt or fabricate a response.
		g.transport.CloseIdleConnections()
		g.mu.Lock()
		g.closed = true
		g.mu.Unlock()
		return 0, nil
	case <-deadline:
		return g.Active(), ErrDrainTimeout
	}
}

// enter registers a new request, or reports that the generation is retired.
func (g *Generation) enter() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.retired {
		return false
	}
	g.active++
	return true
}

// exit unregisters a completed request.
func (g *Generation) exit() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.active--
	if g.active == 0 && g.retired {
		g.signalIdleLocked()
	}
}

// signalIdleLocked closes the idle channel exactly once. g.mu must be held.
func (g *Generation) signalIdleLocked() {
	if !g.idleDone {
		g.idleDone = true
		close(g.idle)
	}
}

// generationRoundTripper counts in-flight requests on a Generation so it can
// be drained safely, and rejects new requests once the generation retires.
type generationRoundTripper struct {
	gen *Generation
}

func (rt *generationRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	g := rt.gen
	if !g.enter() {
		return nil, ErrGenerationRetired
	}

	resp, err := g.transport.RoundTrip(req)
	if err != nil {
		g.exit()
		return nil, err
	}

	// The request stays "active" until the body is consumed or closed, since
	// the connection is in use until then.
	resp.Body = &trackedBody{ReadCloser: resp.Body, gen: g}
	return resp, nil
}

// trackedBody decrements the generation's in-flight count when the body is
// fully read or closed, whichever comes first.
type trackedBody struct {
	io.ReadCloser
	once sync.Once
	gen  *Generation
}

func (b *trackedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		b.once.Do(b.gen.exit)
	}
	return n, err
}

func (b *trackedBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.gen.exit)
	return err
}

// Manager hands out Transport generations so that TLS or connection-pool
// settings can be hot-reloaded under load: Publish swaps in a fresh
// generation for subsequently started requests while the previous generation
// drains its in-flight requests in the background.
//
// The zero value is not usable; construct one with NewManager or
// NewManagerWithFactory.
type Manager struct {
	factory func() (*http.Transport, dialConfig)

	mu      sync.RWMutex
	current *Generation
}

// NewManager returns a Manager whose generations are built with the same
// clean defaults as DefaultPooledTransport: proxy from environment, 30s dial
// timeout and keep-alive, 10s TLS handshake timeout, and bounded idle
// connection pools.
func NewManager() *Manager {
	return newManager(defaultPooledTransport)
}

// NewManagerWithFactory returns a Manager whose generations are built by
// factory. A nil factory is equivalent to NewManager. Summaries of
// custom-built generations report zero dial settings, since they cannot be
// read back from an http.Transport.
func NewManagerWithFactory(factory func() *http.Transport) *Manager {
	if factory == nil {
		return newManager(defaultPooledTransport)
	}
	return newManager(func() (*http.Transport, dialConfig) {
		return factory(), dialConfig{}
	})
}

func newManager(factory func() (*http.Transport, dialConfig)) *Manager {
	m := &Manager{factory: factory}
	m.current = m.build()
	return m
}

func (m *Manager) build() *Generation {
	transport, dial := m.factory()
	return newGeneration(transport, dial)
}

// Client returns the Client of the current generation. Requests started
// through it use the most recently published transport configuration.
func (m *Manager) Client() *http.Client {
	return m.Current().client
}

// Current returns the current generation.
func (m *Manager) Current() *Generation {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.current
}

// Publish installs a fresh generation and returns the superseded one, now
// retired: it rejects new requests with ErrGenerationRetired while its
// in-flight requests finish undisturbed. The caller should Drain the
// returned generation to close its idle connections once they are no longer
// in use. Publish itself never blocks on the old generation.
func (m *Manager) Publish() *Generation {
	m.mu.Lock()
	prev := m.current
	m.current = m.build()
	m.mu.Unlock()

	prev.retire()
	return prev
}
