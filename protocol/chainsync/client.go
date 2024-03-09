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
	readyForNextBlockChan chan bool
	wantFirstBlock        bool
	firstBlockChan        chan common.Point

	// New fields
	handleMessageChan       chan<- clientMessage
	gracefulStopChan        chan chan<- error
	immediateStopChan       chan chan<- error
	wantIntersectResultChan chan chan<- clientIntersectResult
	wantRollbackChan        chan chan<- clientRollback
}

type clientMessage struct {
	message   protocol.Message
	errorChan chan<- error
}

type clientIntersectResult struct {
	tip   Tip
	point common.Point
	error error
}

type clientRollback struct {
	tip Tip
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
		readyForNextBlockChan: make(chan bool),
		firstBlockChan:        make(chan common.Point),

		gracefulStopChan:        make(chan chan<- error, 10),
		immediateStopChan:       make(chan chan<- error, 10),
		wantIntersectResultChan: make(chan chan<- clientIntersectResult, 10),
		wantRollbackChan:        make(chan chan<- clientRollback, 10),
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

// Stop gracefully transitions the protocol to the Done state. No more protocol operations will be
// possible afterward
func (c *Client) Stop() error {
	errorChan := make(chan error, 1)
	c.gracefulStopChan <- errorChan
	return c.waitForError(errorChan)
}

// Close immediately transitions the protocol to the Done state. No more protocol operations will be
// possible afterward
func (c *Client) Close() error {
	errorChan := make(chan error, 1)
	c.immediateStopChan <- errorChan
	return c.waitForError(errorChan)
}

// GetCurrentTip returns the current chain tip
func (c *Client) GetCurrentTip() (*Tip, error) {
	c.busyMutex.Lock()
	defer c.busyMutex.Unlock()

	result := c.requestFindIntersect([]common.Point{})
	if result.error != nil && result.error != IntersectNotFoundError {
		return nil, result.error
	}

	return &result.tip, nil
}

// GetAvailableBlockRange returns the start and end of the range of available blocks given the provided intersect
// point(s). Empty start/end points will be returned if there are no additional blocks available.
func (c *Client) GetAvailableBlockRange(
	intersectPoints []common.Point,
) (common.Point, common.Point, error) {
	c.busyMutex.Lock()
	defer c.busyMutex.Unlock()
	var start, end common.Point

	fmt.Printf("GetAvailableBlockRange: waiting for intersect result for: %+v\n", intersectPoints)

	result := c.requestFindIntersect(intersectPoints)
	if result.error != nil {
		return common.Point{}, common.Point{}, result.error
	}
	start = result.point
	end = result.tip.Point

	// If we're already at the chain tip, return an empty range
	if start.Slot >= end.Slot {
		return common.Point{}, common.Point{}, nil
	}

	fmt.Printf("GetAvailableBlockRange: start=%v, end=%v\n", start, end)

	// Request the next block to get the first block after the intersect point. This should result
	// in a rollback.
	//
	// TODO: Verify that the rollback always happends, if not review the code here.
	rollbackChan := c.wantRollback()
	c.wantFirstBlock = true
	msgRequestNext := NewMsgRequestNext()
	if err := c.SendMessage(msgRequestNext); err != nil {
		return start, end, err
	}

	for {
		select {
		case <-c.Protocol.DoneChan():
			return start, end, protocol.ProtocolShuttingDownError
		case rollback := <-rollbackChan:
			end = rollback.tip.Point
			rollbackChan = nil
		case point := <-c.firstBlockChan:
			start = point
			c.wantFirstBlock = false
		case <-c.readyForNextBlockChan:
			// TODO: This doesn't check for true/false, verify if it should?

			// Request the next block
			msg := NewMsgRequestNext()
			if err := c.SendMessage(msg); err != nil {
				return start, end, err
			}
		}
		if !c.wantFirstBlock && rollbackChan == nil {
			break
		}
	}

	fmt.Println("GetAvailableBlockRange: done")

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

	fmt.Printf("Sync: intersectPoints=%v\n", func() string {
		var s string
		for _, p := range intersectPoints {
			s += fmt.Sprintf("%v ", p.Slot)
		}
		return s
	}())

	intersectResultChan := c.wantIntersectResult()
	msg := NewMsgFindIntersect(intersectPoints)
	if err := c.SendMessage(msg); err != nil {
		return err
	}

	select {
	case <-c.Protocol.DoneChan():
		return protocol.ProtocolShuttingDownError
	case result := <-intersectResultChan:
		if result.error != nil {
			return result.error
		}
	}

	fmt.Println("Sync: fill pipeline")

	// Pipeline the initial block requests to speed things up a bit
	// Using a value higher than 10 seems to cause problems with NtN
	for i := 0; i <= c.config.PipelineLimit; i++ {
		msg := NewMsgRequestNext()
		if err := c.SendMessage(msg); err != nil {
			return err
		}
	}

	fmt.Println("Sync: startin sync loop")

	go c.syncLoop()
	return nil
}

func (c *Client) waitForError(ch <-chan error) error {
	select {
	case <-c.Protocol.DoneChan():
		return protocol.ProtocolShuttingDownError
	case err := <-ch:
		return err
	}
}

func (c *Client) wantIntersectResult() <-chan clientIntersectResult {
	ch := make(chan clientIntersectResult, 1)
	c.wantIntersectResultChan <- ch
	return ch
}

func (c *Client) wantRollback() <-chan clientRollback {
	ch := make(chan clientRollback, 1)
	c.wantRollbackChan <- ch
	return ch
}

func (c *Client) requestFindIntersect(intersectPoints []common.Point) clientIntersectResult {
	// TODO: Retry on rollback.
	resultChan := c.wantIntersectResult()

	msg := NewMsgFindIntersect(intersectPoints)
	if err := c.SendMessage(msg); err != nil {
		fmt.Printf("requestFindIntersect: error sending message: %v\n", err)
		return clientIntersectResult{error: err}
	}

	select {
	case <-c.Protocol.DoneChan():
		return clientIntersectResult{error: protocol.ProtocolShuttingDownError}
	case result := <-resultChan:
		fmt.Printf("requestFindIntersect: received intersect: %+v - %+v - %v\n", result.tip, result.point, result.error)
		return result
	}
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
// TODO: Seems the public functions use busyMutex to protect the client from concurrent access
// TODO: However the message handlers are not protected by busyMutex, so we need to figure out what
// variables are shared between the two
func (c *Client) clientLoop(handleMessageChan <-chan clientMessage) {
	messageHandlerDoneChan := make(chan struct{})
	go c.messageHandlerLoop(handleMessageChan, messageHandlerDoneChan)

	defer func() {
		// We should avoid closing channels, instead they should check done channel.
		close(c.readyForNextBlockChan)
		close(c.firstBlockChan)

		// Handle any remaining messages in the channels so they don't block, and let the GC
		// clean up this loop.
		for {
			select {
			case msg := <-c.immediateStopChan:
				msg <- nil
			}
		}
	}()

	doneMessageSent := false

	// Likely we need to separate out the requests from the client loop(?)
	for {
		select {
		case <-c.Protocol.DoneChan():
			return

		case <-messageHandlerDoneChan:
			return

		case ch := <-c.immediateStopChan:
			// Consider the need for graceful shutdown handling, e.g. waiting for all messages to be
			// processed vs. interrupting the message handler immediately
			//
			// The interrupting variant is implemented here, but the graceful variant would be in
			// the meessageHandlerLoop
			//
			// Stop() as previous implemented was a graceful shutdown (busymutex), so we need to
			// review the desired shutdown behaviour.
			if !doneMessageSent {
				// TODO: Add a timeout.
				doneMessageSent = true
				msg := NewMsgDone()
				ch <- c.SendMessage(msg)
			}
		}
	}
}

func (c *Client) messageHandlerLoop(handleMessageChan <-chan clientMessage, messageHandlerDoneChan chan<- struct{}) {
	defer func() {
		close(messageHandlerDoneChan)

		// Handle any remaining messages in the channels so they don't block, and let the GC
		// clean up this loop.
		for {
			select {
			case msg := <-handleMessageChan:
				msg.errorChan <- protocol.ProtocolShuttingDownError
			case <-c.wantIntersectResultChan:
				// The reader of the result channel must also watch protocol.DoneChan() to avoid a
				// deadlock. We do not close the channels as that complicates select statements with
				// multiple channels.
			}
		}
	}()

	for {
		select {
		case <-c.Protocol.DoneChan():
			return

		case ch := <-c.gracefulStopChan:
			err := c.Close()
			ch <- err

			// Reconsider the error handling here if the current behavior is changed.
			if err == nil || err == protocol.ProtocolShuttingDownError {
				return
			}

		case msg := <-handleMessageChan:
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

// messageHandler handles incoming messages from the protocol. It is called from the underlying
// protocol and is blocking.
func (c *Client) messageHandler(msg protocol.Message) error {
	errorChan := make(chan error, 1)
	c.handleMessageChan <- clientMessage{message: msg, errorChan: errorChan}
	return c.waitForError(errorChan)
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

	// TODO: Seems suspect, does RollBackward cancel intersect requests? It seem
	// we should instead send an error and let the requester decide what to do.
	//
	// E.g. getting the current tip should retry with a new empty intersection request.
	//
	// Consider emptying the intersection request channel and sending an error them.

	// TODO: Figure out if the previous code was reusing the currentTipChan for multiple purposes,
	// and use only explicit channels for each purpose.
	for len(c.wantIntersectResultChan) > 0 {
		select {
		case ch := <-c.wantIntersectResultChan:
			ch <- clientIntersectResult{tip: msgRollBackward.Tip} // err: protocol.ProtocolRollbackError}
		default:
		}
	}

	select {
	case ch := <-c.wantRollbackChan:
		ch <- clientRollback{tip: msgRollBackward.Tip}
	default:
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

loop:
	for {
		select {
		case ch := <-c.wantIntersectResultChan:
			ch <- clientIntersectResult{tip: msgIntersectFound.Tip, point: msgIntersectFound.Point}
		default:
			break loop
		}
	}

	return nil
}

func (c *Client) handleIntersectNotFound(msgGeneric protocol.Message) error {
	msgIntersectNotFound := msgGeneric.(*MsgIntersectNotFound)

loop:
	for len(c.wantIntersectResultChan) > 0 {
		select {
		case ch := <-c.wantIntersectResultChan:
			ch <- clientIntersectResult{tip: msgIntersectNotFound.Tip, error: IntersectNotFoundError}
		default:
			break loop
		}
	}

	return nil
}
