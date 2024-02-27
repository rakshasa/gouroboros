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

package chainsync

import (
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/protocol"
	"github.com/blinklabs-io/gouroboros/protocol/common"
)

// Client implements the ChainSync client
type Client struct {
	*protocol.Protocol
	config                *Config
	busyMutex             sync.Mutex
	intersectResultChan   chan error
	readyForNextBlockChan chan bool
	wantCurrentTip        bool
	currentTipChan        chan Tip
	wantFirstBlock        bool
	firstBlockChan        chan common.Point
	wantIntersectPoint    bool
	intersectPointChan    chan common.Point

	// New fields
	handleMessageChan chan<- clientMessage
	gracefulStopChan  chan chan<- error

	requestCurrentTipChan chan clientRequestCurrentTip
	wantCurrentTipChan    chan clientWantCurrentTip
}

type clientMessage struct {
	message   protocol.Message
	errorChan chan<- error
}

type clientRequestCurrentTip struct {
	resultChan chan<- Tip
	errorChan  chan<- error
}

type clientWantCurrentTip struct {
	resultChan chan<- Tip
	errorChan  chan<- error
}

// NewClient returns a new ChainSync client object
func NewClient(protoOptions protocol.ProtocolOptions, cfg *Config) *Client {
	// Use node-to-client protocol ID
	ProtocolId := ProtocolIdNtC
	msgFromCborFunc := NewMsgFromCborNtC
	if protoOptions.Mode == protocol.ProtocolModeNodeToNode {
		// Use node-to-node protocol ID
		ProtocolId = ProtocolIdNtN
		msgFromCborFunc = NewMsgFromCborNtN
	}
	if cfg == nil {
		tmpCfg := NewConfig()
		cfg = &tmpCfg
	}

	c := &Client{
		config:                cfg,
		intersectResultChan:   make(chan error),
		readyForNextBlockChan: make(chan bool),
		currentTipChan:        make(chan Tip),
		firstBlockChan:        make(chan common.Point),
		intersectPointChan:    make(chan common.Point),

		gracefulStopChan:      make(chan chan<- error),
		requestCurrentTipChan: make(chan clientRequestCurrentTip, 10),
		wantCurrentTipChan:    make(chan clientWantCurrentTip, 10),
	}
	// Update state map with timeouts
	stateMap := StateMap.Copy()
	if entry, ok := stateMap[stateIntersect]; ok {
		entry.Timeout = c.config.IntersectTimeout
		stateMap[stateIntersect] = entry
	}
	for _, state := range []protocol.State{stateCanAwait, stateMustReply} {
		if entry, ok := stateMap[state]; ok {
			entry.Timeout = c.config.BlockTimeout
			stateMap[state] = entry
		}
	}
	// Configure underlying Protocol
	protoConfig := protocol.ProtocolConfig{
		Name:                ProtocolName,
		ProtocolId:          ProtocolId,
		Muxer:               protoOptions.Muxer,
		ErrorChan:           protoOptions.ErrorChan,
		Mode:                protoOptions.Mode,
		Role:                protocol.ProtocolRoleClient,
		MessageHandlerFunc:  c.messageHandler,
		MessageFromCborFunc: msgFromCborFunc,
		StateMap:            stateMap,
		InitialState:        stateIdle,
	}
	c.Protocol = protocol.New(protoConfig)

	handleMessageChan := make(chan clientMessage)
	c.handleMessageChan = handleMessageChan

	go c.clientLoop(handleMessageChan)

	return c
}

// messageHandler handles incoming messages from the protocol. It is called from the underlying
// protocol and is blocking.
func (c *Client) messageHandler(msg protocol.Message) error {
	errorChan := make(chan error, 1)
	c.handleMessageChan <- clientMessage{message: msg, errorChan: errorChan}
	return <-errorChan
}

// Stop transitions the protocol to the Done state. No more protocol operations will be possible afterward
func (c *Client) Stop() error {
	errorChan := make(chan error, 1)

	select {
	case c.gracefulStopChan <- errorChan:
		return <-errorChan
	default:
		return nil
	}
}

// GetCurrentTip returns the current chain tip
func (c *Client) GetCurrentTip() (*Tip, error) {
	c.busyMutex.Lock()
	defer c.busyMutex.Unlock()
	c.wantCurrentTip = true
	msg := NewMsgFindIntersect([]common.Point{})
	if err := c.SendMessage(msg); err != nil {
		return nil, err
	}
	tip, ok := <-c.currentTipChan
	if !ok {
		return nil, protocol.ProtocolShuttingDownError
	}
	// Clear out intersect result channel to prevent blocking
	_, ok = <-c.intersectResultChan
	if !ok {
		return nil, protocol.ProtocolShuttingDownError
	}
	c.wantCurrentTip = false
	return &tip, nil
}

// GetAvailableBlockRange returns the start and end of the range of available blocks given the provided intersect
// point(s). Empty start/end points will be returned if there are no additional blocks available.
func (c *Client) GetAvailableBlockRange(
	intersectPoints []common.Point,
) (common.Point, common.Point, error) {
	c.busyMutex.Lock()
	defer c.busyMutex.Unlock()
	var start, end common.Point
	// Find our chain intersection
	c.wantCurrentTip = true
	c.wantIntersectPoint = true
	msgFindIntersect := NewMsgFindIntersect(intersectPoints)
	if err := c.SendMessage(msgFindIntersect); err != nil {
		return start, end, err
	}
	gotIntersectResult := false
	for {
		select {
		case <-c.DoneChan():
			return start, end, protocol.ProtocolShuttingDownError
		case tip := <-c.currentTipChan:
			end = tip.Point
			c.wantCurrentTip = false
		case point := <-c.intersectPointChan:
			start = point
			c.wantIntersectPoint = false
		case err := <-c.intersectResultChan:
			if err != nil {
				return start, end, err
			}
			gotIntersectResult = true
		}
		if !c.wantIntersectPoint && !c.wantCurrentTip && gotIntersectResult {
			break
		}
	}
	// If we're already at the chain tip, return an empty range
	if start.Slot >= end.Slot {
		return common.Point{}, common.Point{}, nil
	}
	// Request the next block to get the first block after the intersect point. This should result in a rollback
	c.wantCurrentTip = true
	c.wantFirstBlock = true
	msgRequestNext := NewMsgRequestNext()
	if err := c.SendMessage(msgRequestNext); err != nil {
		return start, end, err
	}
	for {
		select {
		case <-c.DoneChan():
			return start, end, protocol.ProtocolShuttingDownError
		case tip := <-c.currentTipChan:
			end = tip.Point
			c.wantCurrentTip = false
		case point := <-c.firstBlockChan:
			start = point
			c.wantFirstBlock = false
		case <-c.readyForNextBlockChan:
			// Request the next block
			msg := NewMsgRequestNext()
			if err := c.SendMessage(msg); err != nil {
				return start, end, err
			}
		}
		if !c.wantFirstBlock && !c.wantCurrentTip {
			break
		}
	}
	// If we're already at the chain tip, return an empty range
	if start.Slot >= end.Slot {
		return common.Point{}, common.Point{}, nil
	}
	return start, end, nil
}

// Sync begins a chain-sync operation using the provided intersect point(s). Incoming blocks will be delivered
// via the RollForward callback function specified in the protocol config
func (c *Client) Sync(intersectPoints []common.Point) error {
	c.busyMutex.Lock()
	defer c.busyMutex.Unlock()
	// Use origin if no intersect points were specified
	if len(intersectPoints) == 0 {
		intersectPoints = []common.Point{common.NewPointOrigin()}
	}
	msg := NewMsgFindIntersect(intersectPoints)
	if err := c.SendMessage(msg); err != nil {
		return err
	}
	if err, ok := <-c.intersectResultChan; !ok {
		return protocol.ProtocolShuttingDownError
	} else if err != nil {
		return err
	}
	// Pipeline the initial block requests to speed things up a bit
	// Using a value higher than 10 seems to cause problems with NtN
	for i := 0; i <= c.config.PipelineLimit; i++ {
		msg := NewMsgRequestNext()
		if err := c.SendMessage(msg); err != nil {
			return err
		}
	}
	go c.syncLoop()
	return nil
}

func (c *Client) syncLoop() {
	for {
		// Wait for a block to be received
		if ready, ok := <-c.readyForNextBlockChan; !ok {
			// Channel is closed, which means we're shutting down
			return
		} else if !ready {
			// Sync was cancelled
			return
		}
		c.busyMutex.Lock()
		// Request the next block
		// In practice we already have multiple block requests pipelined
		// and this just adds another one to the pile
		msg := NewMsgRequestNext()
		if err := c.SendMessage(msg); err != nil {
			c.SendError(err)
			return
		}
		c.busyMutex.Unlock()
	}
}

// clientLoop is the main loop for the client.
//
// We're rewriting the client to use a single loop to handle all incoming messages, this will use golang channels
//
// Seems the public functions use busyMutex to protect the client from concurrent access
//
// However the message handlers are not protected by busyMutex, so we need to figure out what
// variables are shared between the two

func (c *Client) clientLoop(handleMessageChan <-chan clientMessage) {
	messageHandlerDoneChan := make(chan struct{})
	go c.messageHandlerLoop(handleMessageChan, messageHandlerDoneChan)

	doneMessageSent := false

	// Likely we need to separate out the requests from the client loop?

	for {
		select {
		case ch := <-c.gracefulStopChan:
			// Consider the need for graceful shutdown handling, e.g. waiting for all messages to be
			// processed vs. interrupting the mesage handler immediately
			//
			// The interrupting variant is implemented here, but the graceful variant would be in
			// the meessageHandlerLoop

			// Stop() as previous implemented was a graceful shutdown (mutex amongst public methods), so change to that?

			if !doneMessageSent {
				doneMessageSent = true

				msg := NewMsgDone()
				ch <- c.SendMessage(msg)
			}

		case <-messageHandlerDoneChan:
			// We need to keep reading from public method channels to drain them, so we can't return here(?)

			close(c.intersectResultChan)
			close(c.readyForNextBlockChan)
			close(c.currentTipChan)
			close(c.firstBlockChan)
			close(c.intersectPointChan)
			return

		case req := <-c.requestCurrentTipChan:
			c.requestCurrentTip(req)
		}
	}
}

// fix the shutdown sequence, clientLoop should wait for messageHandlerLoop

func (c *Client) messageHandlerLoop(handleMessageChan <-chan clientMessage, doneChan chan<- struct{}) {
	for {
		select {
		case <-c.Protocol.DoneChan():
			// TODO: Handle this more gracefully, check if we should clean up anything else
			close(doneChan)

			// Handle any remaining messages so they don't block, and let the GC clean up this
			// loop. This is strictly not necessary, however future changes might cause issues.
			for {
				// TODO: Drain all want channels. (?)

				select {
				case msg := <-handleMessageChan:
					msg.errorChan <- protocol.ProtocolShuttingDownError
				case want := <-c.wantCurrentTipChan:
					want.errorChan <- protocol.ProtocolShuttingDownError
				}
			}

		case msg := <-handleMessageChan:
			// TODO: Replace the 'want' variables with channel arrays.
			// consider passing them as a struct

			msg.errorChan <- func() error {
				switch msg.message.Type() {
				case MessageTypeAwaitReply:
					return c.handleAwaitReply()
				case MessageTypeRollForward:
					return c.handleRollForward(msg.message)
				case MessageTypeRollBackward:
					return c.handleRollBackward(msg.message)
				case MessageTypeIntersectFound:
					return c.handleIntersectFound(msg.message)
				case MessageTypeIntersectNotFound:
					return c.handleIntersectNotFound(msg.message)
				default:
					return fmt.Errorf(
						"%s: received unexpected message type %d",
						ProtocolName,
						msg.message.Type(),
					)
				}
			}()
		}
	}
}

func (c *Client) requestCurrentTip(req clientRequestCurrentTip) {
	c.busyMutex.Lock()
	defer c.busyMutex.Unlock()

	c.wantCurrentTip = true
	msg := NewMsgFindIntersect([]common.Point{})
	if err := c.SendMessage(msg); err != nil {
		req.errorChan <- err
		return
	}
	tip, ok := <-c.currentTipChan
	if !ok {
		req.errorChan <- protocol.ProtocolShuttingDownError
		return
	}
	// Clear out intersect result channel to prevent blocking
	_, ok = <-c.intersectResultChan
	if !ok {
		req.errorChan <- protocol.ProtocolShuttingDownError
		return
	}
	c.wantCurrentTip = false
	req.resultChan <- tip
}

func (c *Client) handleAwaitReply() error {
	return nil
}

func (c *Client) handleRollForward(msgGeneric protocol.Message) error {
	if (c.config == nil || c.config.RollForwardFunc == nil) && !c.wantFirstBlock {
		return fmt.Errorf(
			"received chain-sync RollForward message but no callback function is defined",
		)
	}
	var callbackErr error
	if c.Mode() == protocol.ProtocolModeNodeToNode {
		msg := msgGeneric.(*MsgRollForwardNtN)
		var blockHeader ledger.BlockHeader
		var blockType uint
		blockEra := msg.WrappedHeader.Era
		switch blockEra {
		case ledger.BlockHeaderTypeByron:
			blockType = msg.WrappedHeader.ByronType()
			var err error
			blockHeader, err = ledger.NewBlockHeaderFromCbor(
				blockType,
				msg.WrappedHeader.HeaderCbor(),
			)
			if err != nil {
				return err
			}
		default:
			// Map block header type to block type
			blockType = ledger.BlockHeaderToBlockTypeMap[blockEra]
			var err error
			blockHeader, err = ledger.NewBlockHeaderFromCbor(
				blockType,
				msg.WrappedHeader.HeaderCbor(),
			)
			if err != nil {
				return err
			}
		}
		if c.wantFirstBlock {
			blockHash, err := hex.DecodeString(blockHeader.Hash())
			if err != nil {
				return err
			}
			point := common.NewPoint(blockHeader.SlotNumber(), blockHash)
			c.firstBlockChan <- point
			return nil
		}
		// Call the user callback function
		callbackErr = c.config.RollForwardFunc(blockType, blockHeader, msg.Tip)
	} else {
		msg := msgGeneric.(*MsgRollForwardNtC)
		blk, err := ledger.NewBlockFromCbor(msg.BlockType(), msg.BlockCbor())
		if err != nil {
			return err
		}
		if c.wantFirstBlock {
			blockHash, err := hex.DecodeString(blk.Hash())
			if err != nil {
				return err
			}
			point := common.NewPoint(blk.SlotNumber(), blockHash)
			c.firstBlockChan <- point
			return nil
		}
		// Call the user callback function
		callbackErr = c.config.RollForwardFunc(msg.BlockType(), blk, msg.Tip)
	}
	if callbackErr != nil {
		if callbackErr == StopSyncProcessError {
			// Signal that we're cancelling the sync
			c.readyForNextBlockChan <- false
			return nil
		} else {
			return callbackErr
		}
	}
	// Signal that we're ready for the next block
	c.readyForNextBlockChan <- true
	return nil
}

func (c *Client) handleRollBackward(msg protocol.Message) error {
	msgRollBackward := msg.(*MsgRollBackward)
	if c.wantCurrentTip {
		c.currentTipChan <- msgRollBackward.Tip
	}
	if !c.wantFirstBlock {
		if c.config.RollBackwardFunc == nil {
			return fmt.Errorf(
				"received chain-sync RollBackward message but no callback function is defined",
			)
		}
		// Call the user callback function
		if callbackErr := c.config.RollBackwardFunc(msgRollBackward.Point, msgRollBackward.Tip); callbackErr != nil {
			if callbackErr == StopSyncProcessError {
				// Signal that we're cancelling the sync
				c.readyForNextBlockChan <- false
				return nil
			} else {
				return callbackErr
			}
		}
	}
	// Signal that we're ready for the next block
	c.readyForNextBlockChan <- true
	return nil
}

func (c *Client) handleIntersectFound(msg protocol.Message) error {
	msgIntersectFound := msg.(*MsgIntersectFound)
	if c.wantCurrentTip {
		c.currentTipChan <- msgIntersectFound.Tip
	}
	if c.wantIntersectPoint {
		c.intersectPointChan <- msgIntersectFound.Point
	}
	c.intersectResultChan <- nil
	return nil
}

func (c *Client) handleIntersectNotFound(msgGeneric protocol.Message) error {
	if c.wantCurrentTip {
		msgIntersectNotFound := msgGeneric.(*MsgIntersectNotFound)
		c.currentTipChan <- msgIntersectNotFound.Tip
	}
	c.intersectResultChan <- IntersectNotFoundError
	return nil
}
