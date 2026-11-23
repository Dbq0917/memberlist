// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/stretchr/testify/require"
)

func testIncrementalMemberlist(t *testing.T) *Memberlist {
	t.Helper()
	c := testConfig(t)
	c.ProtocolVersion = incrementalProtocolVersion
	c.IncrementalPushPull = true
	c.PushPullInterval = time.Hour
	c.GossipInterval = time.Hour
	c.ProbeInterval = time.Hour
	m, err := newMemberlist(c)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, m.Shutdown()) })
	return m
}

func testAliveNode(name string, port uint16, incarnation uint32) alive {
	return alive{
		Incarnation: incarnation,
		Node:        name,
		Addr:        net.ParseIP("127.0.0.1"),
		Port:        port,
	}
}

func TestStateChangeLogCursorAndRetention(t *testing.T) {
	log := newStateChangeLog(2)
	log.append(pushNodeState{Name: "node-1"})
	second := log.append(pushNodeState{Name: "node-2"})
	third := log.append(pushNodeState{Name: "node-3"})

	firstSeq, nextSeq := log.bounds()
	require.Equal(t, second.Seq, firstSeq)
	require.Equal(t, third.Seq, nextSeq)

	changes, ok := log.changesAfter(0)
	require.False(t, ok)
	require.Nil(t, changes)

	changes, ok = log.changesAfter(1)
	require.True(t, ok)
	require.Len(t, changes, 2)

	changes, ok = log.changesAfter(2)
	require.True(t, ok)
	require.Len(t, changes, 1)
	require.Equal(t, "node-3", changes[0].Node.Name)
}

func TestIncrementalPushPullSendsOnlyChangedState(t *testing.T) {
	m1 := testIncrementalMemberlist(t)
	m2 := testIncrementalMemberlist(t)

	m1.aliveNode(&alive{
		Incarnation: 1,
		Node:        m1.config.Name,
		Addr:        net.ParseIP(m1.config.BindAddr),
		Port:        uint16(m1.config.BindPort),
		Vsn:         m1.config.BuildVsnArray(),
	}, nil, true)
	m2.aliveNode(&alive{
		Incarnation: 1,
		Node:        m2.config.Name,
		Addr:        net.ParseIP(m2.config.BindAddr),
		Port:        uint16(m2.config.BindPort),
		Vsn:         m2.config.BuildVsnArray(),
	}, nil, true)

	m1.aliveNode(&alive{
		Incarnation: 1,
		Node:        m2.config.Name,
		Addr:        net.ParseIP(m2.config.BindAddr),
		Port:        uint16(m2.config.BindPort),
		Vsn:         m2.config.BuildVsnArray(),
	}, nil, false)

	require.NoError(t, m1.pushPullNode(m2.LocalNode().FullAddress(), false))
	require.Equal(t, uint64(1), m1.syncCursor(m2.config.Name))

	changed := testAliveNode("changed-node", 9000, 1)
	changed.Vsn = m2.config.BuildVsnArray()
	m2.aliveNode(&changed, nil, false)

	require.NoError(t, m1.pushPullNode(m2.LocalNode().FullAddress(), false))
	require.Equal(t, uint64(2), m1.syncCursor(m2.config.Name))

	state, ok := m1.nodeMap["changed-node"]
	require.True(t, ok)
	require.Equal(t, StateAlive, state.State)
	require.Equal(t, uint32(1), state.Incarnation)

	_, next := m2.stateChanges.bounds()
	require.Equal(t, uint64(2), next)
}

func TestIncrementalPushPullGapFallsBackToFullState(t *testing.T) {
	m1 := testIncrementalMemberlist(t)
	m2 := testIncrementalMemberlist(t)

	m1.aliveNode(&alive{
		Incarnation: 1,
		Node:        m1.config.Name,
		Addr:        net.ParseIP(m1.config.BindAddr),
		Port:        uint16(m1.config.BindPort),
		Vsn:         m1.config.BuildVsnArray(),
	}, nil, true)
	m2.aliveNode(&alive{
		Incarnation: 1,
		Node:        m2.config.Name,
		Addr:        net.ParseIP(m2.config.BindAddr),
		Port:        uint16(m2.config.BindPort),
		Vsn:         m2.config.BuildVsnArray(),
	}, nil, true)

	require.NoError(t, m1.pushPullNode(m2.LocalNode().FullAddress(), true))

	nodeA := testAliveNode("node-a", 9001, 1)
	nodeB := testAliveNode("node-b", 9002, 1)
	nodeC := testAliveNode("node-c", 9003, 1)
	nodeA.Vsn = m2.config.BuildVsnArray()
	nodeB.Vsn = m2.config.BuildVsnArray()
	nodeC.Vsn = m2.config.BuildVsnArray()
	m2.aliveNode(&nodeA, nil, false)
	m2.aliveNode(&nodeB, nil, false)
	m2.aliveNode(&nodeC, nil, false)

	m2.stateChanges.mu.Lock()
	m2.stateChanges.changes = m2.stateChanges.changes[2:]
	m2.stateChanges.mu.Unlock()

	require.NoError(t, m1.pushPullNode(m2.LocalNode().FullAddress(), false))

	require.Contains(t, m1.nodeMap, "node-a")
	require.Contains(t, m1.nodeMap, "node-b")
	require.Contains(t, m1.nodeMap, "node-c")
}

func TestIncrementalSyncDataVolumeAt200Nodes(t *testing.T) {
	const nodeCount = 200

	m := testIncrementalMemberlist(t)
	for idx := range nodeCount {
		node := testAliveNode("node-"+string(rune('a'+idx%26))+string(rune('a'+idx/26)), uint16(9000+idx), uint32(idx+1))
		node.Vsn = m.config.BuildVsnArray()
		m.aliveNode(&node, nil, false)
	}

	fullChanges := make([]stateChange, len(m.nodes))
	for idx, node := range m.nodes {
		fullChanges[idx] = stateChange{Node: pushNodeStateFromNode(node)}
	}
	fullClient, fullServer := net.Pipe()
	go func() {
		_ = m.sendDeltaState(fullServer, deltaSyncHeader{FullSync: true}, fullChanges, "")
		_ = fullServer.Close()
	}()
	fullBuf := make([]byte, 64*1024)
	fullSize, err := fullClient.Read(fullBuf)
	require.NoError(t, err)
	require.NoError(t, fullClient.Close())

	_, nextSeq := m.stateChanges.bounds()
	deltaClient, deltaServer := net.Pipe()
	go func() {
		_ = m.sendDeltaState(deltaServer, deltaSyncHeader{
			Cursor:      nextSeq,
			LogFirstSeq: nextSeq,
			LogNextSeq:  nextSeq,
		}, nil, "")
		_ = deltaServer.Close()
	}()
	deltaBuf := make([]byte, 4096)
	deltaSize, err := deltaClient.Read(deltaBuf)
	require.NoError(t, err)
	require.NoError(t, deltaClient.Close())

	require.Equal(t, nodeCount, len(m.nodes))
	require.Less(t, deltaSize, fullSize/10)
	t.Logf("200-node sync bytes: full=%d no-change-delta=%d", fullSize, deltaSize)
}

func TestIncrementalPushPullFallsBackForOlderProtocol(t *testing.T) {
	c1 := testConfig(t)
	c1.ProtocolVersion = incrementalProtocolVersion
	c1.IncrementalPushPull = true
	m1, err := newMemberlist(c1)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, m1.Shutdown()) })

	c2 := testConfig(t)
	c2.ProtocolVersion = ProtocolVersion2Compatible
	c2.IncrementalPushPull = true
	m2, err := newMemberlist(c2)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, m2.Shutdown()) })

	m1.aliveNode(&alive{
		Incarnation: 1,
		Node:        m1.config.Name,
		Addr:        net.ParseIP(m1.config.BindAddr),
		Port:        uint16(m1.config.BindPort),
		Vsn:         m1.config.BuildVsnArray(),
	}, nil, true)
	m2.aliveNode(&alive{
		Incarnation: 1,
		Node:        m2.config.Name,
		Addr:        net.ParseIP(m2.config.BindAddr),
		Port:        uint16(m2.config.BindPort),
		Vsn:         m2.config.BuildVsnArray(),
	}, nil, true)
	m1.aliveNode(&alive{
		Incarnation: 1,
		Node:        m2.config.Name,
		Addr:        net.ParseIP(m2.config.BindAddr),
		Port:        uint16(m2.config.BindPort),
		Vsn:         m2.config.BuildVsnArray(),
	}, nil, false)

	require.False(t, m1.canIncrementalSync(m2.LocalNode().FullAddress()))
	require.NoError(t, m1.pushPullNode(m2.LocalNode().FullAddress(), false))
	require.Zero(t, m1.syncCursor(m2.config.Name))
	require.Contains(t, m1.nodeMap, m2.config.Name)
}

func TestReadDeltaPayloadRejectsCursorOverflow(t *testing.T) {
	m := testIncrementalMemberlist(t)
	header := deltaSyncHeader{Changes: 1, Cursor: ^uint64(0)}
	dec := codec.NewDecoder(bytes.NewReader(nil), &codec.MsgpackHandle{})

	_, err := m.readDeltaPayload(header, bytes.NewReader(nil), dec)
	require.ErrorContains(t, err, "delta cursor overflow")
}
