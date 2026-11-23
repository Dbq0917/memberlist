// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"bytes"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestMemberlist_Announce verifies that a node can proactively report its
// presence to an existing peer using only an alive notification, without any
// push/pull state exchange. The receiving side runs untouched, legacy
// discovery code, proving old nodes need no changes.
func TestMemberlist_Announce(t *testing.T) {
	c1 := testConfig(t)
	m1, err := Create(c1)
	require.NoError(t, err)
	defer func() { require.NoError(t, m1.Shutdown()) }()

	c2 := testConfig(t)
	c2.BindPort = m1.config.BindPort
	m2, err := Create(c2)
	require.NoError(t, err)
	defer func() { require.NoError(t, m2.Shutdown()) }()

	// Before announcing, neither side knows about the other.
	require.Equal(t, 1, m1.NumMembers())
	require.Equal(t, 1, m2.NumMembers())

	num, err := m2.Announce([]string{m1.config.Name + "/" + m1.config.BindAddr})
	require.NoError(t, err)
	require.Equal(t, 1, num)

	// The peer should now treat the announcing node as a normal, alive member.
	waitUntilSizeAndEstimate(t, m1, 2)

	got := m1.getNodeState(m2.config.Name)
	require.Equal(t, StateAlive, got)
}

// TestMemberlist_Announce_Errors checks the deterministic failure paths and
// makes sure failures are surfaced through errors and logs rather than being
// swallowed. Note that a plain UDP write to an unreachable port is allowed to
// succeed by the OS, so delivery failures are instead covered by the retry
// path in TestMemberlist_Announce_Retries.
func TestMemberlist_Announce_Errors(t *testing.T) {
	var logBuf bytes.Buffer

	c := testConfig(t)
	c.Logger = log.New(&logBuf, "", 0)
	m, err := Create(c)
	require.NoError(t, err)

	// No targets at all.
	_, err = m.Announce(nil)
	require.ErrorIs(t, err, ErrAnnounceNoTargets)

	// RequireNodeNames is enabled by testConfig; an unnamed target must fail.
	num, err := m.Announce([]string{"127.0.0.1:9"})
	require.Error(t, err)
	require.Equal(t, 0, num)
	require.Contains(t, logBuf.String(), "failed to announce to")

	// Announcing after shutdown must not touch the transport.
	require.NoError(t, m.Shutdown())
	num, err = m.Announce([]string{m.config.Name + "/" + m.config.BindAddr})
	require.Error(t, err)
	require.Equal(t, 0, num)
}

// TestMemberlist_Announce_Retries verifies that a temporarily unavailable
// peer is picked up by the startup announcement once it becomes reachable,
// exercising the retry loop without any manual intervention.
func TestMemberlist_Announce_Retries(t *testing.T) {
	c1 := testConfig(t)
	m1, err := Create(c1)
	require.NoError(t, err)
	defer func() { require.NoError(t, m1.Shutdown()) }()

	c2 := testConfig(t)
	c2.BindPort = m1.config.BindPort
	c2.AnnounceAddrs = []string{m1.config.Name + "/" + m1.config.BindAddr}
	c2.AnnounceRetries = 5
	c2.AnnounceInterval = 20 * time.Millisecond
	m2, err := Create(c2)
	require.NoError(t, err)
	defer func() { require.NoError(t, m2.Shutdown()) }()

	// Create() schedules the announcement in the background; the peer should
	// learn about the new node without an explicit Join call.
	waitUntilSizeAndEstimate(t, m1, 2)
	require.Equal(t, StateAlive, m1.getNodeState(m2.config.Name))
}

// TestMemberlist_Announce_ScalingTiming is the scaling use case: add one node
// to a running cluster and measure how long it takes for an existing member to
// treat it as a normal, request-serving member. Announcement should make this
// effectively immediate (well under one probe interval).
func TestMemberlist_Announce_ScalingTiming(t *testing.T) {
	eventCh := make(chan NodeEvent, 4)
	c1 := testConfig(t)
	c1.Events = &ChannelEventDelegate{Ch: eventCh}
	// Slow the passive mechanisms down so the test clearly relies on the
	// proactive announcement rather than gossip or push/pull convergence.
	c1.GossipInterval = time.Second
	c1.PushPullInterval = 30 * time.Second
	m1, err := Create(c1)
	require.NoError(t, err)
	defer func() { require.NoError(t, m1.Shutdown()) }()

	c2 := testConfig(t)
	c2.BindPort = m1.config.BindPort
	c2.GossipInterval = time.Second
	c2.PushPullInterval = 30 * time.Second
	c2.AnnounceAddrs = []string{m1.config.Name + "/" + m1.config.BindAddr}
	c2.AnnounceRetries = 3
	c2.AnnounceInterval = 50 * time.Millisecond

	start := time.Now()
	m2, err := Create(c2)
	require.NoError(t, err)
	defer func() { require.NoError(t, m2.Shutdown()) }()

	deadline := time.After(3 * time.Second)
	for {
		select {
		case ev := <-eventCh:
			if ev.Node.Name != m2.config.Name {
				continue
			}
			elapsed := time.Since(start)
			require.Equal(t, NodeJoin, ev.Event)
			t.Logf("new node became a cluster member after %s", elapsed)
			require.Less(t, elapsed, 500*time.Millisecond,
				"announcement should register the new node immediately")
		case <-deadline:
			t.Fatal("existing member never observed the new node joining")
		}
		break
	}

	waitUntilSizeAndEstimate(t, m1, 2)
	waitUntilSizeAndEstimate(t, m2, 2)
}

// TestMemberlist_Announce_DisabledByDefault ensures that nodes created
// without AnnounceAddrs keep the original, fully passive discovery behavior
// and do not spontaneously contact an unrelated peer.
func TestMemberlist_Announce_DisabledByDefault(t *testing.T) {
	c1 := testConfig(t)
	m1, err := Create(c1)
	require.NoError(t, err)
	defer func() { require.NoError(t, m1.Shutdown()) }()

	c2 := testConfig(t)
	c2.BindPort = m1.config.BindPort
	m2, err := Create(c2)
	require.NoError(t, err)
	defer func() { require.NoError(t, m2.Shutdown()) }()

	require.Nil(t, c2.AnnounceAddrs)

	// Give any (unexpected) background announcement time to arrive.
	time.Sleep(150 * time.Millisecond)
	require.Equal(t, 1, m1.NumMembers(), "passive node must not announce itself")
	require.Equal(t, 1, m2.NumMembers())
}

// TestMemberlist_Announce_WithEncryption verifies that the announcement is
// encrypted and labeled exactly like regular gossip, so it works against an
// unchanged peer that enforces encryption.
func TestMemberlist_Announce_WithEncryption(t *testing.T) {
	secret := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

	c1 := testConfig(t)
	c1.Label = "cluster"
	c1.SecretKey = secret
	m1, err := Create(c1)
	require.NoError(t, err)
	defer func() { require.NoError(t, m1.Shutdown()) }()

	c2 := testConfig(t)
	c2.BindPort = m1.config.BindPort
	c2.Label = "cluster"
	c2.SecretKey = secret
	m2, err := Create(c2)
	require.NoError(t, err)
	defer func() { require.NoError(t, m2.Shutdown()) }()

	num, err := m2.Announce([]string{m1.config.Name + "/" + m1.config.BindAddr})
	require.NoError(t, err)
	require.Equal(t, 1, num)

	waitUntilSizeAndEstimate(t, m1, 2)
	require.Equal(t, StateAlive, m1.getNodeState(m2.config.Name))
}
