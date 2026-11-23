// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemberlistBroadcast_Invalidates(t *testing.T) {
	m1 := &memberlistBroadcast{"test", nil, nil}
	m2 := &memberlistBroadcast{"foo", nil, nil}

	if m1.Invalidates(m2) || m2.Invalidates(m1) {
		t.Fatalf("unexpected invalidation")
	}

	if !m1.Invalidates(m1) {
		t.Fatalf("expected invalidation")
	}
}

func TestMemberlistBroadcast_Message(t *testing.T) {
	m1 := &memberlistBroadcast{"test", []byte("test"), nil}
	msg := m1.Message()
	if !reflect.DeepEqual(msg, []byte("test")) {
		t.Fatalf("messages do not match")
	}
}

func TestMemberlistBroadcast_Fingerprint(t *testing.T) {
	encoded := func(t *testing.T, msgType messageType, msg any) *memberlistBroadcast {
		t.Helper()
		buf, err := encode(msgType, msg, false)
		require.NoError(t, err)
		return &memberlistBroadcast{"test", buf.Bytes(), nil}
	}

	alive1 := encoded(t, aliveMsg, &alive{
		Incarnation: 1,
		Node:        "node",
		Addr:        []byte{127, 0, 0, 1},
		Port:        7946,
		Meta:        []byte("meta"),
		Vsn:         []uint8{1, 5, 5, 1, 5, 5},
	})
	alive1Dup := encoded(t, aliveMsg, &alive{
		Incarnation: 1,
		Node:        "node",
		Addr:        []byte{127, 0, 0, 1},
		Port:        7946,
		Meta:        []byte("meta"),
		Vsn:         []uint8{1, 5, 5, 1, 5, 5},
	})
	alive2 := encoded(t, aliveMsg, &alive{
		Incarnation: 2,
		Node:        "node",
		Addr:        []byte{127, 0, 0, 1},
		Port:        7946,
		Meta:        []byte("meta"),
		Vsn:         []uint8{1, 5, 5, 1, 5, 5},
	})
	aliveMetaChanged := encoded(t, aliveMsg, &alive{
		Incarnation: 1,
		Node:        "node",
		Addr:        []byte{127, 0, 0, 1},
		Port:        7946,
		Meta:        []byte("other"),
		Vsn:         []uint8{1, 5, 5, 1, 5, 5},
	})

	suspectA := encoded(t, suspectMsg, &suspect{Incarnation: 1, Node: "node", From: "a"})
	suspectB := encoded(t, suspectMsg, &suspect{Incarnation: 1, Node: "node", From: "b"})
	suspect2 := encoded(t, suspectMsg, &suspect{Incarnation: 2, Node: "node", From: "a"})

	failedA := encoded(t, deadMsg, &dead{Incarnation: 2, Node: "node", From: "a"})
	failedB := encoded(t, deadMsg, &dead{Incarnation: 2, Node: "node", From: "b"})
	left := encoded(t, deadMsg, &dead{Incarnation: 2, Node: "node", From: "node"})

	require.Equal(t, alive1.Fingerprint(), alive1Dup.Fingerprint(),
		"the same alive event must have the same fingerprint")
	require.NotEqual(t, alive1.Fingerprint(), alive2.Fingerprint(),
		"a newer incarnation is a real change")
	require.NotEqual(t, alive1.Fingerprint(), aliveMetaChanged.Fingerprint(),
		"changed metadata is a real change")

	require.Equal(t, suspectA.Fingerprint(), suspectB.Fingerprint(),
		"the detecting node is not part of a suspect event")
	require.NotEqual(t, suspectA.Fingerprint(), suspect2.Fingerprint(),
		"a newer suspect incarnation is a real change")

	require.Equal(t, failedA.Fingerprint(), failedB.Fingerprint(),
		"the detecting node is not part of a failure event")
	require.NotEqual(t, failedA.Fingerprint(), left.Fingerprint(),
		"leaving and failing are different membership changes")
	require.NotEqual(t, alive1.Fingerprint(), suspectA.Fingerprint(),
		"state transitions at the same incarnation are distinct")
}
