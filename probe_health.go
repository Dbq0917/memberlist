// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"sync"
	"time"
)

// rttAlpha is the smoothing factor used for the exponentially weighted
// moving averages of the round-trip time and its variance. It is the same
// value the TCP stack uses for its RTT estimator.
const rttAlpha = 0.125

// rttBeta is the fraction of the RTT standard deviation added as safety
// margin. TCP uses 4, which empirically keeps the false-retransmission rate
// well under one percent, so we reuse the same conservative margin.
const rttBeta = 4.0

// probeHealth tracks the per-node probe history used to adapt the probe
// timeout to observed network conditions and to distinguish a hard failure
// from an isolated lost packet.
//
// All methods are safe for concurrent use. A probeHealth instance is shared
// by pointer from the node's nodeState, so its state survives the shallow
// node copy made at the start of each probe.
type probeHealth struct {
	mu sync.Mutex

	// srtt is the smoothed round-trip time estimate.
	srtt time.Duration

	// rttvar is the smoothed mean deviation of the round-trip time.
	rttvar time.Duration

	// samples is the number of RTT samples observed so far. Until enough
	// samples have been collected the configured base timeout is used.
	samples int

	// failures is the number of consecutive probes that did not produce a
	// direct ack. It is reset to zero on every successful probe.
	failures int
}

// observeRTT feeds a successful round-trip measurement into the estimator
// and clears the consecutive failure count.
func (h *probeHealth) observeRTT(rtt time.Duration) {
	if rtt <= 0 {
		rtt = time.Microsecond
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	h.samples++
	h.failures = 0

	if h.samples == 1 {
		h.srtt = rtt
		h.rttvar = rtt / 2
		return
	}

	// Jacobson/Karels RTT estimation, mirroring the TCP implementation.
	delta := rtt - h.srtt
	if delta < 0 {
		h.rttvar = time.Duration((1-rttAlpha)*float64(h.rttvar) + rttAlpha*float64(-delta))
	} else {
		h.rttvar = time.Duration((1-rttAlpha)*float64(h.rttvar) + rttAlpha*float64(delta))
	}
	h.srtt = time.Duration((1-rttAlpha)*float64(h.srtt) + rttAlpha*float64(rtt))
}

// recordFailure increments the consecutive failure count and returns the new
// value.
func (h *probeHealth) recordFailure() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.failures++
	return h.failures
}

// consecutiveFailures returns the current consecutive failure count.
func (h *probeHealth) consecutiveFailures() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.failures
}

// resetFailures clears the consecutive failure count without touching the
// RTT estimates.
func (h *probeHealth) resetFailures() {
	h.mu.Lock()
	h.failures = 0
	h.mu.Unlock()
}

// adaptiveTimeout returns the timeout to wait for a direct ack. Once a few
// samples are available it tracks the observed RTT plus a variance-based
// safety margin so that ordinary jitter does not look like a failure, while
// a genuinely fast network converges much quicker than the static base
// timeout. The result is always clamped to [minTimeout, baseTimeout]: the
// lower bound protects against an unrealistically tight estimate and the
// upper bound keeps the adaptive timeout from ever being more permissive
// than the configured value.
func (h *probeHealth) adaptiveTimeout(baseTimeout, minTimeout time.Duration, warmupSamples int) time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.samples < warmupSamples || h.srtt <= 0 {
		return baseTimeout
	}

	timeout := h.srtt + time.Duration(rttBeta*float64(h.rttvar))
	if timeout < minTimeout {
		timeout = minTimeout
	}
	if timeout > baseTimeout {
		timeout = baseTimeout
	}
	return timeout
}

// probeHealthFor returns the probeHealth attached to a node, creating and
// attaching one on first use. It must be called while holding the nodeLock.
func (m *Memberlist) probeHealthFor(state *nodeState) *probeHealth {
	if state.health == nil {
		state.health = &probeHealth{}
	}
	return state.health
}

// probeTimeout returns the direct UDP ack timeout to use for the next probe
// of the given node.
func (m *Memberlist) probeTimeout(state *nodeState) time.Duration {
	base := m.config.ProbeTimeout
	minTimeout := m.config.MinProbeTimeout
	if minTimeout <= 0 || minTimeout > base {
		minTimeout = base
	}
	if state.health == nil {
		return base
	}
	return state.health.adaptiveTimeout(base, minTimeout, m.config.RTTWarmupSamples)
}

// probeRetryInterval returns the delay before a node that just failed a
// probe is retried. A failed node is re-probed on a tighter cadence than the
// regular round-robin so hard failures converge quickly, while the delay is
// never shorter than the ack timeout (so a probe has time to complete) and
// never longer than the current awareness-scaled probe interval.
func (m *Memberlist) probeRetryInterval(state *nodeState, probeInterval time.Duration) time.Duration {
	retry := m.config.ProbeInterval / time.Duration(m.config.FailureProbeSpeedup)

	// Allow at least one full adaptive timeout plus a small grace period.
	if minRetry := m.probeTimeout(state) + m.config.ProbeTimeout/2; retry < minRetry {
		retry = minRetry
	}
	if retry > probeInterval {
		retry = probeInterval
	}
	return retry
}
