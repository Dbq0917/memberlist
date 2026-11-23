// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fastAdaptiveConfig returns a LAN-derived config with short, deterministic
// probing timings suitable for failure-detection timing tests.
func fastAdaptiveConfig(t *testing.T, name string) *Config {
	t.Helper()
	c := testConfig(t)
	c.Name = name
	c.ProbeInterval = 50 * time.Millisecond
	c.ProbeTimeout = 25 * time.Millisecond
	c.MinProbeTimeout = 5 * time.Millisecond
	c.RTTWarmupSamples = 2
	c.ProbeFailureThreshold = 2
	c.FailureProbeSpeedup = 5
	c.IndirectChecks = 1
	c.SuspicionMult = 3
	c.SuspicionMaxTimeoutMult = 2
	c.GossipInterval = 20 * time.Millisecond
	c.PushPullInterval = 0
	return c
}

// TestMemberlist_AdaptiveProbe_HardShutdownConvergence verifies the core
// requirement: once a node hard-stops (power loss style, no graceful leave),
// the surviving member removes it from the online list substantially faster
// than the old static interval plus suspicion window would allow.
func TestMemberlist_AdaptiveProbe_HardShutdownConvergence(t *testing.T) {
	c1 := fastAdaptiveConfig(t, "node-a")
	m1, err := Create(c1)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m1.Shutdown() })

	c2 := fastAdaptiveConfig(t, "node-b")
	c2.BindPort = m1.config.BindPort
	m2, err := Create(c2)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m2.Shutdown() })

	joined, err := m2.Join([]string{m1.config.Name + "/" + m1.config.BindAddr})
	require.NoError(t, err)
	require.Equal(t, 1, joined)

	waitForCondition(t, func() (bool, string) {
		return m1.NumMembers() == 2, fmt.Sprintf("members %d", m1.NumMembers())
	})

	// Let m1 collect RTT samples against the healthy m2 so the adaptive
	// timeout and accelerated retry path are fully warmed up.
	time.Sleep(200 * time.Millisecond)

	// Hard stop without Leave: equivalent to losing power.
	start := time.Now()
	require.NoError(t, m2.Shutdown())

	waitForCondition(t, func() (bool, string) {
		dead := m1.getNodeState(c2.Name) == StateDead
		return dead, fmt.Sprintf("state %v", m1.getNodeState(c2.Name))
	})
	elapsed := time.Since(start)
	t.Logf("hard shutdown detected in %v", elapsed)

	// Upper bound: two accelerated failure probes plus the short suspicion
	// timer. This is deliberately far below the legacy ~40s window and below
	// what plain ProbeInterval-based probing would need for two failures.
	bound := 750 * time.Millisecond
	require.Less(t, elapsed, bound, "failure detection took too long: %v", elapsed)
}

// TestMemberlist_AdaptiveProbe_ToleratesTransientLoss verifies that an
// isolated dropped probe does not get a healthy node marked suspect: the
// failure threshold must require consecutive failures, and a subsequent
// successful probe must clear the streak.
func TestMemberlist_AdaptiveProbe_ToleratesTransientLoss(t *testing.T) {
	addr1 := getBindAddr()
	addr2 := getBindAddr()

	m1 := HostMemberlist(addr1.String(), t, func(c *Config) {
		c.ProbeInterval = 20 * time.Millisecond
		c.ProbeTimeout = 5 * time.Millisecond
		c.ProbeFailureThreshold = 2
		c.FailureProbeSpeedup = 4
	})
	t.Cleanup(func() { _ = m1.Shutdown() })

	m2 := HostMemberlist(addr2.String(), t, func(c *Config) {
		c.BindPort = m1.config.BindPort
	})
	t.Cleanup(func() { _ = m2.Shutdown() })

	a1 := alive{Node: addr1.String(), Addr: []byte(addr1), Port: uint16(m1.config.BindPort), Incarnation: 1, Vsn: m1.config.BuildVsnArray()}
	a2 := alive{Node: addr2.String(), Addr: []byte(addr2), Port: uint16(m2.config.BindPort), Incarnation: 1, Vsn: m2.config.BuildVsnArray()}
	m1.aliveNode(&a1, nil, true)
	m1.aliveNode(&a2, nil, false)
	m2.aliveNode(&a2, nil, true)

	n := m1.nodeMap[addr2.String()]

	// First, a healthy probe succeeds and builds an RTT sample.
	m1.probeNode(n)
	require.Equal(t, StateAlive, n.State)
	require.NotNil(t, n.health)
	require.Equal(t, 0, n.health.consecutiveFailures())

	// Simulate one isolated lost probe by pointing at an unroutable address
	// that will never answer. This must only register the first failure.
	ghost := getBindAddr()
	ghostState := &nodeState{
		Node:   n.Node,
		health: n.health,
	}
	ghostState.Addr = ghost
	ghostState.Name = addr2.String()
	m1.probeNode(ghostState)
	require.Equal(t, StateAlive, n.State, "single lost probe must not mark the node suspect")
	require.Equal(t, 1, n.health.consecutiveFailures())

	// The failed probe must have queued an accelerated retry rather than
	// escalating straight to suspicion.
	m1.probeRetryLock.Lock()
	_, queued := m1.probeRetries[addr2.String()]
	m1.probeRetryLock.Unlock()
	require.True(t, queued, "expected an accelerated retry to be scheduled")

	// A subsequent successful probe clears the streak and cancels the retry.
	m1.probeNode(n)
	require.Equal(t, StateAlive, n.State)
	require.Equal(t, 0, n.health.consecutiveFailures())

	m1.probeRetryLock.Lock()
	_, queued = m1.probeRetries[addr2.String()]
	m1.probeRetryLock.Unlock()
	require.False(t, queued, "expected retry to be cancelled after recovery")
}

// TestMemberlist_AdaptiveProbe_ConsecutiveFailuresEscalate proves the
// transient-loss tolerance does not delay real outages: once the threshold
// of consecutive failures is reached, the node is declared suspect.
func TestMemberlist_AdaptiveProbe_ConsecutiveFailuresEscalate(t *testing.T) {
	addr1 := getBindAddr()
	ghost := getBindAddr()

	m1 := HostMemberlist(addr1.String(), t, func(c *Config) {
		c.ProbeInterval = 20 * time.Millisecond
		c.ProbeTimeout = 5 * time.Millisecond
		c.ProbeFailureThreshold = 3
		c.FailureProbeSpeedup = 4
	})
	t.Cleanup(func() { _ = m1.Shutdown() })

	a1 := alive{Node: addr1.String(), Addr: []byte(addr1), Port: uint16(m1.config.BindPort), Incarnation: 1, Vsn: m1.config.BuildVsnArray()}
	m1.aliveNode(&a1, nil, true)

	// A node that never actually runs, so every probe times out.
	ag := alive{Node: "ghost", Addr: []byte(ghost), Port: uint16(m1.config.BindPort), Incarnation: 1, Vsn: m1.config.BuildVsnArray()}
	m1.aliveNode(&ag, nil, false)
	n := m1.nodeMap["ghost"]

	m1.probeNode(n)
	require.Equal(t, StateAlive, n.State)
	m1.probeNode(n)
	require.Equal(t, StateAlive, n.State, "node must stay alive below the threshold")
	m1.probeNode(n)
	require.Equal(t, StateSuspect, n.State, "node must be suspect once the threshold is hit")
}
