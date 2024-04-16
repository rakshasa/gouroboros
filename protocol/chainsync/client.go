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

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/protocol"
	"github.com/blinklabs-io/gouroboros/protocol/common"
)

// Client implements the ChainSync client
type Client struct {
	*protocol.Protocol
	config                *Config
	readyForNextBlockChan chan bool

	handleMessageChan chan<- clientMessage
	gracefulStopChan  chan chan<- error
	startSyncingChan  chan clientSyncingRequest

	requestFindIntersectChan          chan clientFindIntersectRequest
	requestGetAvailableBlockRangeChan chan clientGetAvailableBlockRangeRequest
	requestNextBlockChan              chan struct{}
	requestStartSyncingChan           chan clientSyncingRequest
	requestStopSyncingChan            chan struct{}
	wantCurrentTipChan                chan chan<- clientCurrentTip
	wantFirstBlockChan                chan chan<- clientFirstBlock
	wantIntersectFoundChan            chan chan<- clientIntersectFound
	wantRollbackChan                  chan chan<- clientRollback
}

type clientMessage struct {
	message   protocol.Message
	errorChan chan<- error
}

type clientSyncingRequest struct {
	intersectPoints []common.Point
	resultChan      chan<- error
}

type clientFindIntersectRequest struct {
	intersectPoints []common.Point
	resultChan      chan<- clientIntersectFound
}

type clientGetAvailableBlockRangeRequest struct {
	intersectPoints []common.Point
	resultChan      chan<- clientGetAvailableBlockRangeResult
}

type clientGetAvailableBlockRangeResult struct {
	start common.Point
	end   common.Point
	error error
}

type clientCurrentTip struct {
	tip Tip
}

type clientFirstBlock struct {
	point common.Point
	error error
}

type clientIntersectFound struct {
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
	// TODO: Storing the config as a pointer is unsafe.
	if cfg == nil {
		tmpCfg := NewConfig()
		cfg = &tmpCfg
	}

	c := &Client{
		config:                cfg,
		readyForNextBlockChan: make(chan bool),
		gracefulStopChan:      make(chan chan<- error, 10),
		startSyncingChan:      make(chan clientSyncingRequest),

		requestFindIntersectChan:          make(chan clientFindIntersectRequest),
		requestGetAvailableBlockRangeChan: make(chan clientGetAvailableBlockRangeRequest),
		requestNextBlockChan:              make(chan struct{}),
		requestStartSyncingChan:           make(chan clientSyncingRequest),
		requestStopSyncingChan:            make(chan struct{}),

		// TODO: We should only have a buffer size of 1 here, and review the protocol to make sure
		// it always responds to messages. If it doesn't, we should add a timeout to the channels
		// and error handling in case the node misbehaves.
		wantCurrentTipChan:     make(chan chan<- clientCurrentTip),
		wantFirstBlockChan:     make(chan chan<- clientFirstBlock, 1),
		wantIntersectFoundChan: make(chan chan<- clientIntersectFound, 1),
		wantRollbackChan:       make(chan chan<- clientRollback, 1),
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

// GetCurrentTip returns the current chain tip
func (c *Client) GetCurrentTip() (*Tip, error) {
	currentTipChan := make(chan clientCurrentTip, 1)
	resultChan := make(chan clientIntersectFound, 1)
	request := clientFindIntersectRequest{
		intersectPoints: []common.Point{},
		resultChan:      resultChan,
	}

	select {
	case <-c.Protocol.DoneChan():
		return nil, protocol.ProtocolShuttingDownError
	case c.wantCurrentTipChan <- currentTipChan:
		result := <-currentTipChan
		return &result.tip, nil
	case c.requestFindIntersectChan <- request:
	}

	select {
	case <-c.Protocol.DoneChan():
		return nil, protocol.ProtocolShuttingDownError
	case result := <-resultChan:
		if result.error != nil && result.error != IntersectNotFoundError {
			return nil, result.error
		}
		return &result.tip, nil
	}
}

// GetAvailableBlockRange returns the start and end of the range of available blocks given the provided intersect
// point(s). Empty start/end points will be returned if there are no additional blocks available.
func (c *Client) GetAvailableBlockRange(
	intersectPoints []common.Point,
) (common.Point, common.Point, error) {
	resultChan := make(chan clientGetAvailableBlockRangeResult, 1)
	request := clientGetAvailableBlockRangeRequest{
		intersectPoints: intersectPoints,
		resultChan:      resultChan,
	}

	fmt.Printf("GetAvailableBlockRange: %v\n", intersectPoints)

	select {
	case <-c.Protocol.DoneChan():
		return common.Point{}, common.Point{}, protocol.ProtocolShuttingDownError
	case c.requestGetAvailableBlockRangeChan <- request:
	}

	fmt.Printf("GetAvailableBlockRange: waiting for result\n")

	select {
	case <-c.Protocol.DoneChan():
		return common.Point{}, common.Point{}, protocol.ProtocolShuttingDownError
	case result := <-resultChan:
		fmt.Printf("GetAvailableBlockRange: result: %v, %v, %v\n", result.start, result.end, result.error)
		return result.start, result.end, result.error
	}
}

// Sync begins a chain-sync operation using the provided intersect point(s). Incoming blocks will be delivered
// via the RollForward callback function specified in the protocol config
func (c *Client) Sync(intersectPoints []common.Point) error {
	resultChan := make(chan error, 1)
	request := clientSyncingRequest{
		intersectPoints: intersectPoints,
		resultChan:      resultChan,
	}

	select {
	case <-c.Protocol.DoneChan():
		return protocol.ProtocolShuttingDownError
	case c.startSyncingChan <- request:
	}

	return c.waitForError(resultChan)
}

func (c *Client) requestFindIntersect(intersectPoints []common.Point) clientIntersectFound {
	resultChan, cancel := c.wantIntersectFound()

	msg := NewMsgFindIntersect(intersectPoints)
	if err := c.SendMessage(msg); err != nil {
		fmt.Printf("requestFindIntersect: error sending message: %v\n", err)
		cancel()
		return clientIntersectFound{error: err}
	}

	select {
	case <-c.Protocol.DoneChan():
		return clientIntersectFound{error: protocol.ProtocolShuttingDownError}
	case result := <-resultChan:
		fmt.Printf("requestFindIntersect: received intersect: %+v --- %+v --- %+v --- %v\n", intersectPoints, result.tip, result.point, result.error)
		return result
	}
}

func (c *Client) requestGetAvailableBlockRange(
	intersectPoints []common.Point,
) clientGetAvailableBlockRangeResult {
	fmt.Printf("requestGetAvailableBlockRange: waiting for intersect result for: %+v\n", intersectPoints)

	result := c.requestFindIntersect(intersectPoints)
	if result.error != nil {
		return clientGetAvailableBlockRangeResult{error: result.error}
	}
	start := result.point
	end := result.tip.Point

	// If we're already at the chain tip, return an empty range
	if start.Slot >= end.Slot {
		return clientGetAvailableBlockRangeResult{}
	}

	fmt.Printf("requestGetAvailableBlockRange: start=%v, end=%v\n", start, end)

	// Request the next block to get the first block after the intersect point. This should result
	// in a rollback.
	//
	// TODO: Verify that the rollback always happends, if not review the code here.
	rollbackChan, cancelRollback := c.wantRollback()
	firstBlockChan, cancelFirstBlock := c.wantFirstBlock()
	defer func() {
		if rollbackChan != nil {
			cancelRollback()
		}
		if firstBlockChan != nil {
			cancelFirstBlock()
		}
	}()

	// TODO: Recommended behavior on error should be to send an empty range.

	fmt.Printf("requestGetAvailableBlockRange: requesting next block\n")

	msgRequestNext := NewMsgRequestNext()
	if err := c.SendMessage(msgRequestNext); err != nil {
		return clientGetAvailableBlockRangeResult{start: start, end: end, error: err}
	}

	fmt.Printf("requestGetAvailableBlockRange: waiting for rollback\n")

	for {
		select {
		case <-c.Protocol.DoneChan():
			return clientGetAvailableBlockRangeResult{start: start, end: end, error: protocol.ProtocolShuttingDownError}
		case rollback := <-rollbackChan:
			rollbackChan = nil
			end = rollback.tip.Point

			fmt.Printf("requestGetAvailableBlockRange: rollback received: %v\n", rollback)

		case firstBlock := <-firstBlockChan:
			firstBlockChan = nil

			fmt.Printf("requestGetAvailableBlockRange: first block received: %v\n", firstBlock)

			if firstBlock.error != nil {
				return clientGetAvailableBlockRangeResult{
					start: start,
					end:   end,
					error: fmt.Errorf("failed to get first block: %w", firstBlock.error),
				}
			}
			start = firstBlock.point
		case <-c.readyForNextBlockChan:
			// TODO: This doesn't check for true/false, verify if it should?

			fmt.Printf("requestGetAvailableBlockRange: ready for next block received\n")

			// Request the next block
			msg := NewMsgRequestNext()
			if err := c.SendMessage(msg); err != nil {
				return clientGetAvailableBlockRangeResult{start: start, end: end, error: err}
			}
		}
		if firstBlockChan == nil && rollbackChan == nil {
			break
		}
	}

	fmt.Println("GetAvailableBlockRange: done")

	// If we're already at the chain tip, return an empty range
	if start.Slot >= end.Slot {
		return clientGetAvailableBlockRangeResult{}
	}

	return clientGetAvailableBlockRangeResult{start: start, end: end}
}

func (c *Client) requestSync(intersectPoints []common.Point) error {
	// TODO: Check if we're already syncing, if so return an error or cancel the current sync
	// operation. Use a channel for this.

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

	intersectResultChan, cancel := c.wantIntersectFound()
	msg := NewMsgFindIntersect(intersectPoints)
	if err := c.SendMessage(msg); err != nil {
		cancel()
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

	// fmt.Println("Sync: fill pipeline")

	// Pipeline the initial block requests to speed things up a bit
	// Using a value higher than 10 seems to cause problems with NtN
	// for i := 0; i <= c.config.PipelineLimit; i++ {
	// 	msg := NewMsgRequestNext()
	// 	if err := c.SendMessage(msg); err != nil {
	// 		return err
	// 	}
	// }

	fmt.Println("Sync: startin sync loop")

	return nil
}

func (c *Client) sendCurrentTip(tip Tip) {
	for {
		select {
		case ch := <-c.wantCurrentTipChan:
			fmt.Printf("sendCurrentTip: %v\n", tip)
			ch <- clientCurrentTip{tip: tip}
		default:
			return
		}
	}
}

func (c *Client) waitForError(ch <-chan error) error {
	select {
	case <-c.Protocol.DoneChan():
		return protocol.ProtocolShuttingDownError
	case err := <-ch:
		return err
	}
}

// want* returns a channel that will receive the result of the next intersect request,
// and a function that can be used to clear the channel if sending the request message fails.
//
// TODO: This is a bit of a hack, and should be replaced with a request loop that manages the
// request state and channel buffers. This only works correctly when using busyMutex.
func (c *Client) wantFirstBlock() (<-chan clientFirstBlock, func()) {
	ch := make(chan clientFirstBlock, 1)
	c.wantFirstBlockChan <- ch
	return ch, func() {
		select {
		case <-c.wantFirstBlockChan:
		default:
		}
	}
}

func (c *Client) wantIntersectFound() (<-chan clientIntersectFound, func()) {
	ch := make(chan clientIntersectFound, 1)
	c.wantIntersectFoundChan <- ch
	return ch, func() {
		select {
		case <-c.wantIntersectFoundChan:
		default:
		}
	}
}

// wantRollback returns a channel that will receive the result of the next intersect request, and a
// function that can be used to clear the channel if sending the request message fails.
func (c *Client) wantRollback() (<-chan clientRollback, func()) {
	ch := make(chan clientRollback, 1)
	c.wantRollbackChan <- ch
	return ch, func() {
		select {
		case <-c.wantRollbackChan:
		default:
		}
	}
}

// clientLoop is the main loop for the client.
//
// TODO: Future changes should move all request handling to a request (or client) loop, and wait
// channel management will be handled there. The request loop will be responsible for keeping track
// of if we're in a sync loop and optimizing the current tip requests.
func (c *Client) clientLoop(handleMessageChan <-chan clientMessage) {
	defer func() {
		// TODO: We should avoid closing channels, instead they should check done channel.
		close(c.readyForNextBlockChan)

		for {
			// TODO: Review what channels should be emptied.
			select {
			case ch := <-c.startSyncingChan:
				// TODO: Return error.
				ch.resultChan <- nil
			}
		}
	}()

	messageHandlerDoneChan := make(chan struct{})
	go c.messageHandlerLoop(handleMessageChan, messageHandlerDoneChan)
	requestHandlerDoneChan := make(chan struct{})
	go c.requestHandlerLoop(requestHandlerDoneChan)

	isSyncing := false
	syncPipelineCount := 0
	syncPipelineLimit := c.config.PipelineLimit
	// TODO: Clean these up, we should have a unified (or per-requester) handling of readyForNextBlockChan.
	syncReadyForNextBlockChan := chan bool(nil)
	syncRequestNextBlockChan := chan struct{}(nil)

	// TESTING
	syncPipelineLimit = 10

	// TODO: Change NewClient to return errors on invalid configuration.
	if syncPipelineLimit < 1 {
		syncPipelineLimit = 1
	}

	for {
		select {
		case <-c.Protocol.DoneChan():
			return
		case <-messageHandlerDoneChan:
			// messageHandlerLoop is responsible for the underlying protocol connection being closed.
			return
		case <-requestHandlerDoneChan:
			return

		case request := <-c.startSyncingChan:
			if isSyncing {
				// Already syncing. This should be an error.
				request.resultChan <- nil
				return
			}

			resultChan := make(chan error, 1)
			newRequest := clientSyncingRequest{intersectPoints: request.intersectPoints, resultChan: resultChan}

			select {
			case <-c.Protocol.DoneChan():
				request.resultChan <- protocol.ProtocolShuttingDownError
				return
			case c.requestStartSyncingChan <- newRequest:
			}

			if err := c.waitForError(resultChan); err != nil {
				request.resultChan <- err
				return
			}

			isSyncing = true
			syncPipelineCount = 0
			syncReadyForNextBlockChan = c.readyForNextBlockChan
			syncRequestNextBlockChan = c.requestNextBlockChan

			request.resultChan <- nil

			// TODO: Fill pipeline with initial block requests here.
			// TODO: Consider setting readyForNextBlockChan to nil to keep old behavior.

		case ready := <-syncReadyForNextBlockChan:
			if !isSyncing {
				// We're not syncing, so just ignore the ready signal. This might need better handling.
				continue
			}
			if !ready {
				isSyncing = false
				syncPipelineCount = 0
				syncRequestNextBlockChan = nil
				syncReadyForNextBlockChan = nil

				select {
				case <-c.Protocol.DoneChan():
					return
				case c.requestStopSyncingChan <- struct{}{}:
				}

				continue
			}

			if syncPipelineCount != 0 {
				syncPipelineCount--
			}
			if syncPipelineCount < syncPipelineLimit {
				syncRequestNextBlockChan = c.requestNextBlockChan
			}

		case syncRequestNextBlockChan <- struct{}{}:
			fmt.Printf("syncRequestNextBlockChan: %v : %v\n", syncPipelineCount, c.config.PipelineLimit)

			syncPipelineCount++

			if syncPipelineCount >= syncPipelineLimit {
				syncRequestNextBlockChan = nil
			}
		}
	}
}

func (c *Client) requestHandlerLoop(requestHandlerDoneChan chan<- struct{}) {
	defer func() {
		close(requestHandlerDoneChan)

		for {
			// TODO: Review what channels should be emptied.
			select {
			case <-c.requestNextBlockChan:
			}
		}
	}()

	// TODO: Reduce the number of channels to one generic request channel.
	requestFindIntersectChan := c.requestFindIntersectChan
	requestGetAvailableBlockRangeChan := c.requestGetAvailableBlockRangeChan

	for {
		select {
		case <-c.Protocol.DoneChan():
			return

		case request := <-c.requestStartSyncingChan:
			err := c.requestSync(request.intersectPoints)
			request.resultChan <- err

			// TODO: Better error handling for requests? Some might not be fatal.
			if err != nil {
				return
			}

			requestFindIntersectChan = nil
			requestGetAvailableBlockRangeChan = nil

		case <-c.requestStopSyncingChan:
			requestFindIntersectChan = c.requestFindIntersectChan
			requestGetAvailableBlockRangeChan = c.requestGetAvailableBlockRangeChan

		case request := <-requestFindIntersectChan:
			result := c.requestFindIntersect(request.intersectPoints)
			request.resultChan <- result
			if result.error != nil {
				return
			}

		case request := <-requestGetAvailableBlockRangeChan:
			result := c.requestGetAvailableBlockRange(request.intersectPoints)
			request.resultChan <- result
			if result.error != nil {
				return
			}

		case <-c.requestNextBlockChan:
			fmt.Printf("requestNextBlockChan\n")

			msg := NewMsgRequestNext()
			if err := c.SendMessage(msg); err != nil {
				c.SendError(err)
				return
			}
		}
	}
}

// messageHandlerLoop is responsible for handling messages from the protocol connection.
func (c *Client) messageHandlerLoop(handleMessageChan <-chan clientMessage, messageHandlerDoneChan chan<- struct{}) {
	defer func() {
		close(messageHandlerDoneChan)

		// Read any remaining messages in the channels so they don't block, and let the GC clean up
		// this loop.
		for {
			select {
			case msg := <-handleMessageChan:
				msg.errorChan <- protocol.ProtocolShuttingDownError
			case <-c.gracefulStopChan:
			case <-c.wantIntersectFoundChan:
			case <-c.wantRollbackChan:
				// The reader of the result channels must also watch protocol.DoneChan() to avoid a
				// deadlock. We do not close them as that complicates select statements with
				// multiple channels.
			}
		}
	}()

	doneMessageSent := false

	for {
		select {
		case <-c.Protocol.DoneChan():
			return
		case ch := <-c.gracefulStopChan:
			if !doneMessageSent {
				// Not entirely correct, but good enough for now while we have to deal with busyMutex.

				// TODO: MOVE TO REQUEST HANDLER LOOP?
				// c.busyMutex.Lock()
				msg := NewMsgDone()
				err := c.SendMessage(msg)
				// c.busyMutex.Unlock()

				if err != nil && err != protocol.ProtocolShuttingDownError {
					ch <- err
					break
				}
				doneMessageSent = true
			}
			go func() {
				<-c.Protocol.DoneChan()
				ch <- nil
			}()

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
	firstBlockChan := func() chan<- clientFirstBlock {
		select {
		case ch := <-c.wantFirstBlockChan:
			return ch
		default:
			return nil
		}
	}()
	if firstBlockChan == nil && (c.config == nil || c.config.RollForwardFunc == nil) {
		return fmt.Errorf(
			"received chain-sync RollForward message but no callback function is defined",
		)
	}

	var callbackErr error
	if c.Mode() == protocol.ProtocolModeNodeToNode {
		msg := msgGeneric.(*MsgRollForwardNtN)
		c.sendCurrentTip(msg.Tip)

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
				if firstBlockChan != nil {
					firstBlockChan <- clientFirstBlock{error: err}
				}
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
				if firstBlockChan != nil {
					firstBlockChan <- clientFirstBlock{error: err}
				}
				return err
			}
		}

		if firstBlockChan != nil {
			blockHash, err := hex.DecodeString(blockHeader.Hash())
			if err != nil {
				firstBlockChan <- clientFirstBlock{error: err}
				return err
			}
			point := common.NewPoint(blockHeader.SlotNumber(), blockHash)
			firstBlockChan <- clientFirstBlock{point: point}
			return nil
		}

		// Call the user callback function
		callbackErr = c.config.RollForwardFunc(blockType, blockHeader, msg.Tip)
	} else {
		msg := msgGeneric.(*MsgRollForwardNtC)
		c.sendCurrentTip(msg.Tip)

		blk, err := ledger.NewBlockFromCbor(msg.BlockType(), msg.BlockCbor())
		if err != nil {
			if firstBlockChan != nil {
				firstBlockChan <- clientFirstBlock{error: err}
			}
			return err
		}

		if firstBlockChan != nil {
			blockHash, err := hex.DecodeString(blk.Hash())
			if err != nil {
				firstBlockChan <- clientFirstBlock{error: err}
				return err
			}
			point := common.NewPoint(blk.SlotNumber(), blockHash)
			firstBlockChan <- clientFirstBlock{point: point}
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

	fmt.Printf("handleRolling back to %v\n", msgRollBackward.Point)

	c.sendCurrentTip(msgRollBackward.Tip)

	select {
	case ch := <-c.wantRollbackChan:
		ch <- clientRollback{tip: msgRollBackward.Tip}
	default:
	}

	// firstBlockChan := func() chan<- clientFirstBlock {
	// 	select {
	// 	case ch := <-c.wantFirstBlockChan:
	// 		return ch
	// 	default:
	// 		return nil
	// 	}
	// }()

	if len(c.wantFirstBlockChan) == 0 {
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
	} else {
		// firstBlockChan <- clientFirstBlock{point: msgRollBackward.Point}
		fmt.Printf("handleRolling firstBlockchan\n")
	}

	// TODO: If the requester cancels, and this doesn't get flushed it causes an issue.
	c.readyForNextBlockChan <- true
	return nil
}

func (c *Client) handleIntersectFound(msg protocol.Message) error {
	msgIntersectFound := msg.(*MsgIntersectFound)

	fmt.Printf("handleIntersect found: %v\n", msgIntersectFound.Point)

	c.sendCurrentTip(msgIntersectFound.Tip)

	select {
	case ch := <-c.wantIntersectFoundChan:
		ch <- clientIntersectFound{tip: msgIntersectFound.Tip, point: msgIntersectFound.Point}
	default:
	}

	return nil
}

func (c *Client) handleIntersectNotFound(msgGeneric protocol.Message) error {
	msgIntersectNotFound := msgGeneric.(*MsgIntersectNotFound)

	fmt.Printf("handleIntersect not found")

	c.sendCurrentTip(msgIntersectNotFound.Tip)

	select {
	case ch := <-c.wantIntersectFoundChan:
		ch <- clientIntersectFound{tip: msgIntersectNotFound.Tip, error: IntersectNotFoundError}
	default:
	}

	return nil
}
