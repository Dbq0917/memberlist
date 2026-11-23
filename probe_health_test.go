// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"testing"
	"time"
)

func TestProbeHealth_FailureCounting(t *testing.T) {
	h := &probeHealth{}

	if got := h.consecutiveFailures(); got != 0 {
		t.Fatalf("expected 0 failures initially, got %d", got)
	}

	if got := h.recordFailure(); got != 1 {
		t.Fatalf("expected first failure count 1, got %d", got)
	}
	if got := h.recordFailure(); got != 2 {
		t.Fatalf("expected second failure count 2, got %d", got)
	}

	// A successful probe must reset the streak so an isolated blip never
	// accumulates toward the suspicion threshold.
	h.observeRTT(5 * time.Millisecond)
	if got := h.consecutiveFailures(); got != 0 {
		t.Fatalf("expected failures reset after success, got %d", got)
	}

	h.recordFailure()
	h.resetFailures()
	if got := h.consecutiveFailures(); got != 0 {
		t.Fatalf("expected failures reset, got %d", got)
	}
}

func TestProbeHealth_AdaptiveTimeoutWarmup(t *testing.T) {
	h := &probeHealth{}
	base := 500 * time.Millisecond
	minTimeout := 50 * time.Millisecond

	// Before enough samples are collected the static base timeout must be
	// used so an uninitialized estimate cannot trigger a false positive.
	h.observeRTT(time.Millisecond)
	h.observeRTT(time.Millisecond)
	if got := h.adaptiveTimeout(base, minTimeout, 4); got != base {
		t.Fatalf("expected base timeout during warmup, got %v", got)
	}
}

func TestProbeHealth_AdaptiveTimeoutTracksRTT(t *testing.T) {
	h := &probeHealth{}
	base := 500 * time.Millisecond
	minTimeout := 20 * time.Millisecond

	// Feed a stable, low-latency RTT. After warmup the adaptive timeout must
	// be well below the static base, which is what speeds up detection on a
	// healthy network.
	for range 10 {
		h.observeRTT(2 * time.Millisecond)
	}
	got := h.adaptiveTimeout(base, minTimeout, 4)
	if got >= base {
		t.Fatalf("expected adaptive timeout below base %v, got %v", base, got)
	}
	if got < minTimeout {
		t.Fatalf("expected adaptive timeout clamped to min %v, got %v", minTimeout, got)
	}
}

func TestProbeHealth_AdaptiveTimeoutClampedToBase(t *testing.T) {
	h := &probeHealth{}
	base := 500 * time.Millisecond
	minTimeout := 50 * time.Millisecond

	// A wildly increasing RTT produces a large variance estimate; the result
	// must never exceed the configured base timeout.
	rtts := []time.Duration{2 * time.Millisecond, 100 * time.Millisecond, 5 * time.Millisecond, 120 * time.Millisecond}
	for _, rtt := range rtts {
		h.observeRTT(rtt)
	}
	if got := h.adaptiveTimeout(base, minTimeout, 4); got > base {
		t.Fatalf("expected timeout clamped to base %v, got %v", base, got)
	}
}

func TestProbeHealth_AdaptiveTimeoutToleratesJitter(t *testing.T) {
	h := &probeHealth{}
	base := 500 * time.Millisecond
	minTimeout := 10 * time.Millisecond

	// Stable baseline RTT with an occasional slower sample. A single jittery
	// probe must still be answered within the computed timeout (srtt plus the
	// variance safety margin), i.e. ordinary jitter is not judged a failure.
	for range 8 {
		h.observeRTT(5 * time.Millisecond)
	}
	for range 4 {
		h.observeRTT(20 * time.Millisecond)
	}

	timeout := h.adaptiveTimeout(base, minTimeout, 4)
	srtt, rttvar := h.snapshot()
	expected := srtt + time.Duration(rttBeta*float64(rttvar))
	if timeout != expected {
		t.Fatalf("expected timeout %v (srtt %v + margin), got %v", expected, srtt, timeout)
	}
	if timeout < 20*time.Millisecond {
		t.Fatalf("expected variance margin to cover a 20ms jittery RTT, got %v", timeout)
	}
}

// snapshot returns the current smoothed RTT and variance for test
// assertions.
func (h *probeHealth) snapshot() (srtt, rttvar time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.srtt, h.rttvar
}

func TestProbeRetryInterval_Bounds(t *testing.T) {
	addr := getBindAddr()
	m := HostMemberlist(addr.String(), t, func(c *Config) {
		c.ProbeInterval = 100 * time.Millisecond
		c.ProbeTimeout = 40 * time.Millisecond
		c.MinProbeTimeout = 10 * time.Millisecond
		c.FailureProbeSpeedup = 4
	})
	t.Cleanup(func() { _ = m.Shutdown() })

	state := &nodeState{health: &probeHealth{}}

	// With no RTT history the adaptive timeout equals the base timeout, so
	// the retry interval must be widened to leave room for a full probe.
	got := m.probeRetryInterval(state, m.config.ProbeInterval)
	minRetry := m.config.ProbeTimeout + m.config.ProbeTimeout/2
	if got < minRetry {
		t.Fatalf("expected retry interval >= %v, got %v", minRetry, got)
	}
	if got > m.config.ProbeInterval {
		t.Fatalf("expected retry interval <= probe interval %v, got %v", m.config.ProbeInterval, got)
	}

	// Once the node demonstrates a fast, stable RTT the retry cadence can
	// shrink toward ProbeInterval / FailureProbeSpeedup.
	for range 6 {
		state.health.observeRTT(time.Millisecond)
	}
	fast := m.probeRetryInterval(state, m.config.ProbeInterval)
	if fast >= got {
		t.Fatalf("expected fast retry %v to be shorter than cold retry %v", fast, got)
	}
	if fast < m.probeTimeout(state) {
		t.Fatalf("expected fast retry %v to leave room for timeout %v", fast, m.probeTimeout(state))
	}
}

func TestProbeRetryQueue_DedupAndPrune(t *testing.T) {
	addr := getBindAddr()
	m := HostMemberlist(addr.String(), t, nil)
	t.Cleanup(func() { _ = m.Shutdown() })

	a := alive{Node: "node-a", Addr: []byte(addr), Port: uint16(m.config.BindPort), Incarnation: 1}
	m.aliveNode(&a, nil, true)

	// A retry scheduled in the future is not due yet.
	m.scheduleProbeRetry("node-a", time.Hour)
	if _, ok := m.takeDueProbeRetry(); ok {
		t.Fatal("did not expect retry to be due yet")
	}

	// Re-scheduling replaces the pending entry rather than duplicating it.
	m.scheduleProbeRetry("node-a", time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	name, ok := m.takeDueProbeRetry()
	if !ok || name != "node-a" {
		t.Fatalf("expected due retry for node-a, got %q ok=%v", name, ok)
	}
	if len(m.probeRetries) != 0 {
		t.Fatalf("expected retry to be dequeued, remaining %v", m.probeRetries)
	}

	// Entries for nodes that are gone are pruned rather than returned.
	m.scheduleProbeRetry("ghost", time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	if _, ok := m.takeDueProbeRetry(); ok {
		t.Fatal("did not expect a retry for an unknown node")
	}
}
