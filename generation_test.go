// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package cleanhttp

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestGenerationIDsAreUnique(t *testing.T) {
	m1 := NewManager()
	m2 := NewManager()
	prev := m1.Publish()

	seen := map[uint64]bool{}
	for _, id := range []uint64{m1.Current().ID(), m2.Current().ID(), prev.ID()} {
		if id == 0 {
			t.Fatal("generation ID should be non-zero")
		}
		if seen[id] {
			t.Fatalf("duplicate generation ID %d", id)
		}
		seen[id] = true
	}
}

func TestGenerationSummaryReflectsDefaults(t *testing.T) {
	m := NewManager()
	s := m.Current().Summary()

	if s.Generation != m.Current().ID() {
		t.Fatalf("summary generation %d, want %d", s.Generation, m.Current().ID())
	}
	if s.Active != 0 || s.Retired || s.Drained {
		t.Fatalf("fresh generation should be idle and live: %+v", s)
	}
	if s.MaxIdleConns != 100 {
		t.Fatalf("MaxIdleConns = %d, want 100", s.MaxIdleConns)
	}
	if s.MaxIdleConnsPerHost != runtime.GOMAXPROCS(0)+1 {
		t.Fatalf("MaxIdleConnsPerHost = %d, want %d", s.MaxIdleConnsPerHost, runtime.GOMAXPROCS(0)+1)
	}
	if s.IdleConnTimeout != 90*time.Second {
		t.Fatalf("IdleConnTimeout = %v, want 90s", s.IdleConnTimeout)
	}
	if s.TLSHandshakeTimeout != 10*time.Second {
		t.Fatalf("TLSHandshakeTimeout = %v, want 10s", s.TLSHandshakeTimeout)
	}
	if s.ExpectContinueTimeout != 1*time.Second {
		t.Fatalf("ExpectContinueTimeout = %v, want 1s", s.ExpectContinueTimeout)
	}
	if s.DisableKeepAlives {
		t.Fatal("DisableKeepAlives should be false for pooled generations")
	}
	if !s.ForceAttemptHTTP2 {
		t.Fatal("ForceAttemptHTTP2 should be true")
	}
	if !s.ProxyFromEnvironment {
		t.Fatal("ProxyFromEnvironment should be true")
	}
	if s.DialTimeout != 30*time.Second {
		t.Fatalf("DialTimeout = %v, want 30s", s.DialTimeout)
	}
	if s.DialKeepAlive != 30*time.Second {
		t.Fatalf("DialKeepAlive = %v, want 30s", s.DialKeepAlive)
	}
}

func TestPublishRetiresPreviousGeneration(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	}))
	defer srv.Close()

	m := NewManager()
	oldClient := m.Client()
	old := m.Publish()

	// The retired generation rejects new requests with a real error rather
	// than fabricating a response.
	if _, err := oldClient.Get(srv.URL); !errors.Is(err, ErrGenerationRetired) {
		t.Fatalf("expected ErrGenerationRetired from retired generation, got %v", err)
	}

	// Requests started after Publish use the new generation.
	resp, err := m.Client().Get(srv.URL)
	if err != nil {
		t.Fatalf("new generation request failed: %v", err)
	}
	resp.Body.Close()

	if m.Current().ID() == old.ID() {
		t.Fatal("Publish should install a new generation")
	}
	if !old.Summary().Retired {
		t.Fatal("previous generation should be retired after Publish")
	}

	if n, err := old.Drain(2 * time.Second); err != nil || n != 0 {
		t.Fatalf("drain of idle generation: n=%d err=%v", n, err)
	}
}

func TestDrainWaitsForInflightRequests(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		w.Write([]byte("real-response"))
	}))
	defer srv.Close()

	m := NewManager()
	client := m.Client()

	type result struct {
		body string
		err  error
	}
	resCh := make(chan result, 1)
	go func() {
		resp, err := client.Get(srv.URL)
		if err != nil {
			resCh <- result{err: err}
			return
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		resCh <- result{body: string(b), err: err}
	}()

	<-entered
	old := m.Publish()

	if got := old.Active(); got != 1 {
		t.Fatalf("Active = %d, want 1", got)
	}

	drainCh := make(chan error, 1)
	go func() {
		n, err := old.Drain(5 * time.Second)
		if err == nil && n != 0 {
			err = fmt.Errorf("drain returned %d remaining", n)
		}
		drainCh <- err
	}()

	// The drain must not complete while the request is still in flight.
	select {
	case err := <-drainCh:
		t.Fatalf("drain finished before in-flight request completed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(release)

	if err := <-drainCh; err != nil {
		t.Fatalf("drain failed: %v", err)
	}

	res := <-resCh
	if res.err != nil {
		t.Fatalf("in-flight request failed during drain: %v", res.err)
	}
	if res.body != "real-response" {
		t.Fatalf("in-flight request got %q, want the real server response", res.body)
	}
	if !old.Summary().Drained {
		t.Fatal("generation should report drained after successful Drain")
	}
}

func TestDrainTimeoutReturnsActiveCount(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		fmt.Fprintln(w, "done")
	}))
	defer slow.Close()

	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	}))
	defer fast.Close()

	m := NewManager()
	client := m.Client()

	reqDone := make(chan error, 1)
	go func() {
		resp, err := client.Get(slow.URL)
		if err == nil {
			resp.Body.Close()
		}
		reqDone <- err
	}()

	<-entered
	old := m.Publish()

	remaining, err := old.Drain(50 * time.Millisecond)
	if !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("expected ErrDrainTimeout, got %v", err)
	}
	if remaining != 1 {
		t.Fatalf("remaining = %d, want 1", remaining)
	}

	// The new generation is not blocked by the stuck old one.
	resp, err := m.Client().Get(fast.URL)
	if err != nil {
		t.Fatalf("new generation request failed while old generation stuck: %v", err)
	}
	resp.Body.Close()

	// Let the stuck request finish; a retried drain then succeeds.
	close(release)
	if err := <-reqDone; err != nil {
		t.Fatalf("in-flight request failed: %v", err)
	}
	if n, err := old.Drain(2 * time.Second); err != nil || n != 0 {
		t.Fatalf("retry drain: n=%d err=%v", n, err)
	}
}

func TestDrainIsIdempotent(t *testing.T) {
	m := NewManager()
	old := m.Publish()

	// Concurrent drains of the same generation are all safe.
	var wg sync.WaitGroup
	errs := make([]error, 3)
	remaining := make([]int, 3)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			remaining[i], errs[i] = old.Drain(2 * time.Second)
		}(i)
	}
	wg.Wait()
	for i := range errs {
		if errs[i] != nil || remaining[i] != 0 {
			t.Fatalf("drain %d: n=%d err=%v", i, remaining[i], errs[i])
		}
	}

	// Repeated close of the same generation is a no-op.
	if n, err := old.Drain(2 * time.Second); err != nil || n != 0 {
		t.Fatalf("repeated drain: n=%d err=%v", n, err)
	}
}

// TestProxyFromEnvironmentThroughNewTransport verifies that a generation's
// transport still resolves proxies from the environment. http.ProxyFromEnvironment
// caches the environment process-wide on first use, so the check runs in a
// child process with a controlled environment.
func TestProxyFromEnvironmentThroughNewTransport(t *testing.T) {
	const childEnv = "GO_CLEANHTTP_PROXY_TEST_CHILD"

	if os.Getenv(childEnv) == "1" {
		m := NewManager()
		resp, err := m.Client().Get("http://cleanhttp-proxy-test.invalid/")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if string(body) != "via-proxy" {
			fmt.Fprintf(os.Stderr, "unexpected body %q: request did not go through proxy\n", body)
			os.Exit(1)
		}
		os.Exit(0)
	}

	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !r.URL.IsAbs() {
			// A direct (non-proxied) request would have a relative URL.
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Write([]byte("via-proxy"))
	}))
	defer proxy.Close()

	var env []string
	for _, e := range os.Environ() {
		k := strings.SplitN(e, "=", 2)[0]
		if strings.EqualFold(k, "http_proxy") || strings.EqualFold(k, "https_proxy") || strings.EqualFold(k, "no_proxy") {
			continue
		}
		env = append(env, e)
	}
	env = append(env,
		childEnv+"=1",
		"HTTP_PROXY="+proxy.URL,
		"NO_PROXY=",
	)

	cmd := exec.Command(os.Args[0], "-test.run", "^TestProxyFromEnvironmentThroughNewTransport$")
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("child process failed: %v\n%s", err, out)
	}
}
