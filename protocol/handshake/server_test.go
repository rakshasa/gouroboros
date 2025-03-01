// Copyright 2023 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package handshake_test

import (
	"fmt"
	"testing"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/internal/test/ouroboros_mock"
	"github.com/blinklabs-io/gouroboros/protocol"
	"github.com/blinklabs-io/gouroboros/protocol/handshake"
)

func TestServerBasicHandshake(t *testing.T) {
	mockConn := ouroboros_mock.NewConnection(
		ouroboros_mock.ProtocolRoleServer,
		[]ouroboros_mock.ConversationEntry{
			// MsgProposeVersions from mock client
			{
				Type:       ouroboros_mock.EntryTypeOutput,
				ProtocolId: handshake.ProtocolId,
				OutputMessages: []protocol.Message{
					handshake.NewMsgProposeVersions(
						protocol.ProtocolVersionMap{
							(10 + protocol.ProtocolVersionNtCOffset): protocol.VersionDataNtC9to14(ouroboros_mock.MockNetworkMagic),
							(11 + protocol.ProtocolVersionNtCOffset): protocol.VersionDataNtC9to14(ouroboros_mock.MockNetworkMagic),
							(12 + protocol.ProtocolVersionNtCOffset): protocol.VersionDataNtC9to14(ouroboros_mock.MockNetworkMagic),
						},
					),
				},
			},
			// MsgAcceptVersion from server
			{
				Type:            ouroboros_mock.EntryTypeInput,
				IsResponse:      true,
				ProtocolId:      handshake.ProtocolId,
				MsgFromCborFunc: handshake.NewMsgFromCbor,
				InputMessage: handshake.NewMsgAcceptVersion(
					(12 + protocol.ProtocolVersionNtCOffset),
					protocol.VersionDataNtC9to14(ouroboros_mock.MockNetworkMagic),
				),
			},
		},
	)
	oConn, err := ouroboros.New(
		ouroboros.WithConnection(mockConn),
		ouroboros.WithNetworkMagic(ouroboros_mock.MockNetworkMagic),
		ouroboros.WithServer(true),
	)
	if err != nil {
		t.Fatalf("unexpected error when creating Ouroboros object: %s", err)
	}
	// Async error handler
	go func() {
		err, ok := <-oConn.ErrorChan()
		if !ok {
			return
		}
		// We can't call t.Fatalf() from a different Goroutine, so we panic instead
		panic(fmt.Sprintf("unexpected Ouroboros error: %s", err))
	}()
	// Close Ouroboros connection
	if err := oConn.Close(); err != nil {
		t.Fatalf("unexpected error when closing Ouroboros object: %s", err)
	}
}

func TestServerHandshakeRefuseVersionMismatch(t *testing.T) {
	expectedErr := fmt.Errorf("handshake failed: refused due to version mismatch")
	mockConn := ouroboros_mock.NewConnection(
		ouroboros_mock.ProtocolRoleServer,
		[]ouroboros_mock.ConversationEntry{
			// MsgProposeVersions from mock client
			{
				Type:       ouroboros_mock.EntryTypeOutput,
				ProtocolId: handshake.ProtocolId,
				OutputMessages: []protocol.Message{
					handshake.NewMsgProposeVersions(
						protocol.ProtocolVersionMap{
							(100 + protocol.ProtocolVersionNtCOffset): protocol.VersionDataNtC9to14(ouroboros_mock.MockNetworkMagic),
							(101 + protocol.ProtocolVersionNtCOffset): protocol.VersionDataNtC9to14(ouroboros_mock.MockNetworkMagic),
							(102 + protocol.ProtocolVersionNtCOffset): protocol.VersionDataNtC9to14(ouroboros_mock.MockNetworkMagic),
						},
					),
				},
			},
			// MsgRefuse from server
			{
				Type:             ouroboros_mock.EntryTypeInput,
				IsResponse:       true,
				ProtocolId:       handshake.ProtocolId,
				MsgFromCborFunc:  handshake.NewMsgFromCbor,
				InputMessageType: handshake.MessageTypeRefuse,
				InputMessage: handshake.NewMsgRefuse(
					[]any{
						handshake.RefuseReasonVersionMismatch,
						protocol.GetProtocolVersionsNtC(),
					},
				),
			},
		},
	)
	oConn, err := ouroboros.New(
		ouroboros.WithConnection(mockConn),
		ouroboros.WithNetworkMagic(ouroboros_mock.MockNetworkMagic),
		ouroboros.WithServer(true),
	)
	if err != nil {
		if err.Error() != expectedErr.Error() {
			t.Fatalf("unexpected error when creating Ouroboros object: %s", err)
		}
	} else {
		oConn.Close()
		t.Fatalf("did not receive expected error")
	}
}
