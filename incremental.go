// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	metrics "github.com/hashicorp/go-metrics"
	"github.com/hashicorp/go-msgpack/v2/codec"
)

const incrementalProtocolVersion uint8 = 6

type stateChangeLog struct {
	mu      sync.RWMutex
	nextSeq uint64
	changes []stateChange
	retain  int
}

type deltaSyncResult struct {
	header      deltaSyncHeader
	remoteNodes []pushNodeState
	userState   []byte
}

func newStateChangeLog(retain int) *stateChangeLog {
	if retain <= 0 {
		retain = 4096
	}
	return &stateChangeLog{retain: retain}
}

func (l *stateChangeLog) append(node pushNodeState) stateChange {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.nextSeq++
	change := stateChange{Seq: l.nextSeq, Node: node}
	l.changes = append(l.changes, change)
	if len(l.changes) > l.retain {
		drop := len(l.changes) - l.retain
		l.changes = append([]stateChange(nil), l.changes[drop:]...)
	}
	return change
}

func (l *stateChangeLog) bounds() (firstSeq, nextSeq uint64) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if len(l.changes) == 0 {
		return 0, l.nextSeq
	}
	return l.changes[0].Seq, l.nextSeq
}

func (m *Memberlist) syncCursor(peer string) uint64 {
	m.syncCursorLock.RLock()
	defer m.syncCursorLock.RUnlock()
	return m.syncCursors[peer]
}

func (m *Memberlist) setSyncCursor(peer string, cursor uint64) {
	m.syncCursorLock.Lock()
	defer m.syncCursorLock.Unlock()
	if cursor > m.syncCursors[peer] {
		m.syncCursors[peer] = cursor
	}
}

func (m *Memberlist) canIncrementalSync(a Address) bool {
	if !m.config.IncrementalPushPull || m.ProtocolVersion() < incrementalProtocolVersion {
		return false
	}
	m.nodeLock.RLock()
	defer m.nodeLock.RUnlock()
	state, ok := m.nodeMap[a.Name]
	return ok && state.State == StateAlive && state.PCur >= incrementalProtocolVersion
}

func (m *Memberlist) sendAndReceiveDeltaState(a Address, join bool) (*deltaSyncResult, error) {
	conn, err := m.transport.DialAddressTimeout(a, m.config.TCPTimeout)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()

	if err := conn.SetDeadline(time.Now().Add(m.config.TCPTimeout)); err != nil {
		return nil, err
	}

	cursor := m.syncCursor(a.Name)
	request := deltaSyncHeader{Join: join, Cursor: cursor, LogNextSeq: cursor}
	requestBuf := bytes.NewBuffer(nil)
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(requestBuf, &hd)
	if _, err := requestBuf.Write([]byte{byte(deltaSyncMsg)}); err != nil {
		return nil, err
	}
	if err := enc.Encode(&request); err != nil {
		return nil, err
	}
	if err := m.rawSendMsgStream(conn, requestBuf.Bytes(), m.config.Label); err != nil {
		return nil, err
	}

	if err := conn.SetDeadline(time.Now().Add(m.config.TCPTimeout)); err != nil {
		return nil, err
	}
	msgType, bufConn, dec, err := m.readStream(conn, m.config.Label)
	if err != nil {
		return nil, err
	}
	if msgType == errMsg {
		var resp errResp
		if err := dec.Decode(&resp); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("remote error: %v", resp.Error)
	}
	if msgType != deltaSyncMsg {
		return nil, fmt.Errorf("received invalid msgType (%d), expected deltaSyncMsg (%d) %s", msgType, deltaSyncMsg, LogConn(conn))
	}
	result, err := m.readDeltaState(bufConn, dec)
	if err != nil {
		return nil, err
	}
	m.setSyncCursor(a.Name, result.header.LogNextSeq)
	return result, nil
}

func (m *Memberlist) sendDeltaState(conn net.Conn, header deltaSyncHeader, changes []stateChange, streamLabel string) error {
	var userData []byte
	if m.config.Delegate != nil {
		userData = m.config.Delegate.LocalState(header.Join)
	}

	var localNodes []pushNodeState
	if header.FullSync {
		header.Nodes = len(changes)
		header.Changes = 0
		localNodes = make([]pushNodeState, len(changes))
		for idx, change := range changes {
			localNodes[idx] = change.Node
		}
	} else {
		header.Changes = len(changes)
	}
	header.UserStateLen = len(userData)
	recordCount := header.Changes + header.Nodes
	fullSync := float32(0)
	if header.FullSync {
		fullSync = 1
	}

	bufConn := bytes.NewBuffer(nil)
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(bufConn, &hd)
	if _, err := bufConn.Write([]byte{byte(deltaSyncMsg)}); err != nil {
		return err
	}
	if err := enc.Encode(&header); err != nil {
		return err
	}
	if header.FullSync {
		for idx := range localNodes {
			if err := enc.Encode(&localNodes[idx]); err != nil {
				return err
			}
		}
	} else {
		for idx := range changes {
			if err := enc.Encode(&changes[idx]); err != nil {
				return err
			}
		}
	}
	if userData != nil {
		if _, err := bufConn.Write(userData); err != nil {
			return err
		}
	}

	metrics.AddSampleWithLabels([]string{"memberlist", "delta", "size", "local"}, float32(bufConn.Len()), m.metricLabels)
	metrics.SetGaugeWithLabels([]string{"memberlist", "delta", "records", "local"}, float32(recordCount), m.metricLabels)
	metrics.SetGaugeWithLabels([]string{"memberlist", "delta", "full", "local"}, fullSync, m.metricLabels)
	return m.rawSendMsgStream(conn, bufConn.Bytes(), streamLabel)
}

func (m *Memberlist) readDeltaState(bufConn io.Reader, dec *codec.Decoder) (*deltaSyncResult, error) {
	var header deltaSyncHeader
	if err := dec.Decode(&header); err != nil {
		return nil, err
	}
	return m.readDeltaPayload(header, bufConn, dec)
}

func (m *Memberlist) readDeltaPayload(header deltaSyncHeader, bufConn io.Reader, dec *codec.Decoder) (*deltaSyncResult, error) {
	if err := m.validateDeltaHeader(header); err != nil {
		return nil, err
	}

	remoteNodes := make([]pushNodeState, header.Nodes)
	if header.FullSync {
		for idx := range remoteNodes {
			if err := dec.Decode(&remoteNodes[idx]); err != nil {
				return nil, err
			}
		}
	} else {
		var expected uint64
		if header.Changes > 0 {
			expected = header.Cursor + 1
		}
		for range header.Changes {
			var change stateChange
			if err := dec.Decode(&change); err != nil {
				return nil, err
			}
			if change.Seq != expected {
				return nil, fmt.Errorf("delta sequence gap: expected (%d), got (%d)", expected, change.Seq)
			}
			expected++
			remoteNodes = append(remoteNodes, change.Node)
		}
		if header.Changes > 0 && expected-1 != header.LogNextSeq {
			return nil, fmt.Errorf("delta sequence exceeds log: latest (%d), next (%d)", expected-1, header.LogNextSeq)
		}
	}

	var userBuf []byte
	if header.UserStateLen > 0 {
		userBuf = make([]byte, header.UserStateLen)
		readBytes, err := io.ReadAtLeast(bufConn, userBuf, header.UserStateLen)
		if err == nil && readBytes != header.UserStateLen {
			err = fmt.Errorf("failed to read full user state (%d / %d)", readBytes, header.UserStateLen)
		}
		if err != nil {
			return nil, err
		}
	}

	for idx := range remoteNodes {
		if m.ProtocolVersion() < 2 || remoteNodes[idx].Port == 0 {
			remoteNodes[idx].Port = uint16(m.config.BindPort)
		}
	}

	recordCount := header.Changes + header.Nodes
	fullSync := float32(0)
	if header.FullSync {
		fullSync = 1
	}
	metrics.SetGaugeWithLabels([]string{"memberlist", "delta", "records", "remote"}, float32(recordCount), m.metricLabels)
	metrics.SetGaugeWithLabels([]string{"memberlist", "delta", "full", "remote"}, fullSync, m.metricLabels)
	return &deltaSyncResult{header: header, remoteNodes: remoteNodes, userState: userBuf}, nil
}

func (l *stateChangeLog) changesAfter(cursor uint64) ([]stateChange, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if len(l.changes) == 0 {
		return nil, cursor == l.nextSeq
	}
	if cursor > l.nextSeq {
		return nil, false
	}
	if cursor+1 < l.changes[0].Seq {
		return nil, false
	}

	start := 0
	if cursor >= l.changes[0].Seq {
		start = int(cursor - l.changes[0].Seq + 1)
	}
	return append([]stateChange(nil), l.changes[start:]...), true
}

func pushNodeStateFromNode(n *nodeState) pushNodeState {
	return pushNodeState{
		Name:        n.Name,
		Addr:        append(net.IP(nil), n.Addr...),
		Port:        n.Port,
		Meta:        append([]byte(nil), n.Meta...),
		Incarnation: n.Incarnation,
		State:       n.State,
		Vsn:         append([]uint8(nil), n.PMin, n.PMax, n.PCur, n.DMin, n.DMax, n.DCur),
	}
}

func (m *Memberlist) appendStateChangeLocked(n *nodeState) {
	if m.stateChanges == nil {
		return
	}
	m.stateChanges.append(pushNodeStateFromNode(n))
}

func (m *Memberlist) validateDeltaHeader(header deltaSyncHeader) error {
	if header.Changes > 0 && header.Cursor == ^uint64(0) {
		return fmt.Errorf("delta cursor overflow")
	}
	if header.Changes < 0 || header.Changes > maxPushStateNodes {
		return fmt.Errorf("number of changes in header (%d) exceeds limit", header.Changes)
	}
	if header.Nodes < 0 || header.Nodes > maxPushStateNodes {
		return fmt.Errorf("number of nodes in header (%d) exceeds limit", header.Nodes)
	}
	if header.UserStateLen < 0 || header.UserStateLen > maxPushStateBytes {
		return fmt.Errorf("user state length (%d) exceeds limit", header.UserStateLen)
	}
	return nil
}
