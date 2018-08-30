// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package kv

import (
	"context"
	"sync"
	"time"

	opentracing "github.com/opentracing/opentracing-go"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

const (
	turningCommitToRollbackMsg string = "Turning commit to rollback. All writes are part of old epochs."
)

// txnHeartbeat is a txnInterceptor in charge of the txn's heartbeat loop.
// The heartbeat loop is started upon the first write. txnHeartbeat is also in
// charge of prepending a BeginTransaction to the first write batch and possibly
// eliding EndTransaction requests on read-only transactions.
//
// txnHeartbeat should only be used for root transactions; leafs don't perform
// writes and don't need any of the functionality here.
type txnHeartbeat struct {
	log.AmbientContext

	wrapped           lockedSender
	gatekeeper        lockedSender
	clock             *hlc.Clock
	heartbeatInterval time.Duration
	metrics           *TxnMetrics

	// stopper is the TxnCoordSender's stopper. Used to stop the heartbeat loop
	// when quiescing.
	stopper *stop.Stopper

	// asyncAbortCallbackLocked is called when the heartbeat loop shuts itself
	// down because it has detected the transaction to be aborted. The intention
	// is to notify the TxnCoordSender to shut itself down.
	asyncAbortCallbackLocked func(context.Context)

	// mu contains state protected by the TxnCoordSender's mutex.
	mu struct {
		sync.Locker

		// txnEnd is closed when the transaction is aborted or committed, terminating
		// the heartbeat loop. Nil if the heartbeat loop is not running.
		txnEnd chan struct{}

		// txn is a reference to the TxnCoordSender's proto.
		txn *roachpb.Transaction

		// finalErr, if set, will be returned by all subsequent SendLocked() calls,
		// except rollbacks.
		finalErr *roachpb.Error

		// needBeginTxn dictates whether a BeginTxn request is to be prepended to a
		// write batch. It starts as set and then gets unset when the BeginTxn is
		// sent. It gets reset on epoch increment, as it's possible that the
		// retriable error was generated by the BeginTxn batch and the transaction
		// record has not been written.
		// We could be smarter about not resetting this if there's ever been a
		// successful BeginTxn (in which case we know that there is a txn record)
		// but as of May 2018 we don't do that. Note that the server accepts a
		// BeginTxn with a higher epoch if a transaction record already exists.
		needBeginTxn bool

		// everSentBeginTxn is set once a BeginTransactionRequest (out of possibly
		// many) was sent to the server. If a BeginTxn was ever sent, then an
		// EndTransaction needs to eventually be sent and cannot be elided.
		// Note that simply looking at txnEnd == nil to see if a heartbeat loop is
		// currently running is not always sufficient for deciding whether an
		// EndTransaction can be elided - we want to allow multiple rollback attempts
		// to be sent and the first one stops the heartbeat loop.
		everSentBeginTxn bool
	}
	// !!!
	leaf bool
}

// init initializes the txnHeartbeat. This method exists instead of a
// constructor because txnHeartbeats live in a pool in the TxnCoordSender.
func (h *txnHeartbeat) init(
	mu sync.Locker,
	txn *roachpb.Transaction,
	clock *hlc.Clock,
	heartbeatInterval time.Duration,
	gatekeeper lockedSender,
	metrics *TxnMetrics,
	stopper *stop.Stopper,
	asyncAbortCallbackLocked func(context.Context),
) {
	h.stopper = stopper
	h.clock = clock
	h.heartbeatInterval = heartbeatInterval
	h.metrics = metrics
	h.mu.Locker = mu
	h.mu.txn = txn
	h.mu.needBeginTxn = true
	h.gatekeeper = gatekeeper
	h.asyncAbortCallbackLocked = asyncAbortCallbackLocked
}

// SendLocked is part of the txnInteceptor interface.
func (h *txnHeartbeat) SendLocked(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	// If finalErr is set, we reject everything but rollbacks.
	if h.mu.finalErr != nil {
		singleRollback := ba.IsSingleEndTransactionRequest() &&
			!ba.Requests[0].GetInner().(*roachpb.EndTransactionRequest).Commit
		if !singleRollback {
			return nil, h.mu.finalErr
		}
	}

	firstWriteIdx, pErr := firstWriteIndex(&ba)
	if pErr != nil {
		return nil, pErr
	}
	haveTxnWrite := firstWriteIdx != -1
	et, haveEndTxn := ba.GetArg(roachpb.EndTransaction)
	var etReq *roachpb.EndTransactionRequest
	if haveEndTxn {
		etReq = et.(*roachpb.EndTransactionRequest)
	}

	needBeginTxn := haveTxnWrite && h.mu.needBeginTxn
	if needBeginTxn {
		h.mu.needBeginTxn = false
		h.mu.everSentBeginTxn = true
		// From now on, all requests need to be checked against the AbortCache on
		// the server side. We also conservatively update the current request,
		// although I'm not sure if that's necessary.
		h.mu.txn.Writing = true
		ba.Txn.Writing = true

		// Set txn key based on the key of the first transactional write if not
		// already set. If we're in a restart, make sure we keep the anchor key the
		// same.
		if len(h.mu.txn.Key) == 0 {
			anchor := ba.Requests[firstWriteIdx].GetInner().Header().Key
			h.mu.txn.Key = anchor
			// Put the anchor also in the ba's copy of the txn, since this batch was
			// prepared before we had an anchor.
			ba.Txn.Key = anchor
		}
		// Set the key in the begin transaction request to the txn's anchor key.
		bt := &roachpb.BeginTransactionRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: h.mu.txn.Key,
			},
		}

		// Inject the new request before the first write position, taking care to
		// avoid unnecessary allocations.
		oldRequests := ba.Requests
		ba.Requests = make([]roachpb.RequestUnion, len(ba.Requests)+1)
		copy(ba.Requests, oldRequests[:firstWriteIdx])
		ba.Requests[firstWriteIdx].MustSetInner(bt)
		copy(ba.Requests[firstWriteIdx+1:], oldRequests[firstWriteIdx:])

		// Start the heartbeat loop.
		// Note that we don't do it for 1PC txns: they only leave intents around on
		// retriable errors if the batch has been split between ranges. We consider
		// that unlikely enough so we prefer to not pay for a goroutine.
		//
		// Note that we don't start the heartbeat loop if the loop is already
		// running. That can happen because we send BeginTransaction again after
		// retriable errors.
		if h.mu.txnEnd == nil && !haveEndTxn {
			if err := h.startHeartbeatLoopLocked(ctx); err != nil {
				h.mu.finalErr = roachpb.NewError(err)
				return nil, h.mu.finalErr
			}
		}
	}

	// See if we can elide an EndTxn. We can elide it for read-only transactions.
	lastIndex := int32(len(ba.Requests) - 1)
	var elideEndTxn bool
	var commitTurnedToRollback bool
	if haveEndTxn {
		// Are we writing now or have we written in the past?
		elideEndTxn = !h.mu.everSentBeginTxn
		if elideEndTxn {
			ba.Requests = ba.Requests[:lastIndex]
		} else if etReq.Commit {
			// If all the writes were part of old epochs, we can turn the commit into
			// a rollback. Besides the rollback being potentially cheaper, this
			// transformation is important in situations where it's unclear if the txn
			// record exist: if it doesn't, then a commit would return a
			// TransactionStatusError where a rollback returns success.
			if h.mu.needBeginTxn {
				log.VEventf(ctx, 2, turningCommitToRollbackMsg)
				etReq.Commit = false
				commitTurnedToRollback = true
			}
		}
	}

	// Forward the request.
	// If we've elided the EndTxn and there's no other requests, we can't send an
	// empty batch.
	var br *roachpb.BatchResponse
	if len(ba.Requests) > 0 {
		br, pErr = h.wrapped.SendLocked(ctx, ba)
	} else {
		br = ba.CreateReply()
		txn := ba.Txn.Clone()
		br.Txn = &txn
	}

	// If we inserted a begin transaction request, remove it here.
	if needBeginTxn {
		if br != nil && br.Responses != nil {
			br.Responses = append(br.Responses[:firstWriteIdx], br.Responses[firstWriteIdx+1:]...)
		}
		lastIndex--
		// Handle case where inserted begin txn confused an indexed error.
		if pErr != nil && pErr.Index != nil {
			idx := pErr.Index.Index
			if idx == int32(firstWriteIdx) {
				// An error was encountered on begin txn; disallow the indexing.
				pErr.Index = nil
			} else if idx > int32(firstWriteIdx) {
				// An error was encountered after begin txn; decrement index.
				pErr.SetErrorIndex(idx - 1)
			}
		}
	}

	if pErr != nil {
		return nil, pErr
	}

	if elideEndTxn {
		// Check if the (read-only) txn was pushed above its timestamp.
		// Note that we compare the deadline to br.Txn.Timestamp, not
		// h.mu.txn.Timestamp; the last batch might have been the pushed one, so br
		// has the most up to date timestamp.
		if etReq.Deadline != nil && etReq.Deadline.Less(br.Txn.Timestamp) {
			return nil, roachpb.NewErrorWithTxn(roachpb.NewTransactionStatusError(
				"deadline exceeded before transaction finalization"), br.Txn)
		}
		// This normally happens on the server and sent back in response
		// headers, but this transaction was optimized away. The caller may
		// still inspect the transaction struct, so we manually update it
		// here to emulate a true transaction.
		var status roachpb.TransactionStatus
		if etReq.Commit {
			status = roachpb.COMMITTED
		} else {
			status = roachpb.ABORTED
		}
		if br.Txn == nil {
			txn := ba.Txn.Clone()
			br.Txn = &txn
		} else {
			clone := br.Txn.Clone()
			br.Txn = &clone
		}
		br.Txn.Status = status
		// Synthesize an EndTransactionResponse.
		resp := &roachpb.EndTransactionResponse{}
		resp.Txn = br.Txn
		br.Add(resp)
	} else if commitTurnedToRollback {
		// If we transformed a commit into a rollback, flip the status so that it
		// looks like a successful commit to the higher layers. In particular, the
		// SQL module looks at this status and wants it to be COMMITTED after a "1pc
		// planNode" runs.
		//
		// Note: if we sent an EndTransaction and got back a successful response, we
		// expect br.Txn to be filled.
		clone := br.Txn.Clone()
		br.Txn = &clone
		br.Txn.Status = roachpb.COMMITTED
	}

	return br, nil
}

// setWrapped is part of the txnInteceptor interface.
func (h *txnHeartbeat) setWrapped(wrapped lockedSender) {
	h.wrapped = wrapped
}

// populateMetaLocked is part of the txnInteceptor interface.
func (h *txnHeartbeat) populateMetaLocked(*roachpb.TxnCoordMeta) {}

// augmentMetaLocked is part of the txnInteceptor interface.
func (h *txnHeartbeat) augmentMetaLocked(roachpb.TxnCoordMeta) {}

// epochBumpedLocked is part of the txnInteceptor interface.
func (h *txnHeartbeat) epochBumpedLocked() {
	h.mu.needBeginTxn = true
}

// closeLocked is part of the txnInteceptor interface.
func (h *txnHeartbeat) closeLocked() {
	// If the heartbeat loop has already finished, there's nothing more to do.
	if h.mu.txnEnd == nil {
		return
	}
	close(h.mu.txnEnd)
	h.mu.txnEnd = nil
}

// startHeartbeatLoopLocked starts a heartbeat loop in a different goroutine.
func (h *txnHeartbeat) startHeartbeatLoopLocked(ctx context.Context) error {
	log.Infof(ctx, "!!! starting heartbeat for txn: %s", h.mu.txn)
	if h.mu.txnEnd != nil {
		log.Fatal(ctx, "attempting to start a second heartbeat loop ")
	}

	log.VEventf(ctx, 2, "coordinator spawns heartbeat loop")
	h.mu.txnEnd = make(chan struct{})

	// Create a new context so that the heartbeat loop doesn't inherit the
	// caller's cancelation.
	// We want the loop to run in a span linked to the current one, though, so we
	// put our span in the new context and expect RunAsyncTask to fork it
	// immediately.
	hbCtx := h.AnnotateCtx(context.Background())
	hbCtx = opentracing.ContextWithSpan(hbCtx, opentracing.SpanFromContext(ctx))

	return h.stopper.RunAsyncTask(
		hbCtx, "kv.TxnCoordSender: heartbeat loop", func(ctx context.Context) {
			h.heartbeatLoop(ctx)
		})
}

// heartbeatLoop periodically sends a HeartbeatTxn request to the transaction
// record, stopping in the event the transaction is aborted or committed after
// attempting to resolve the intents.
func (h *txnHeartbeat) heartbeatLoop(ctx context.Context) {
	var tickChan <-chan time.Time
	{
		ticker := time.NewTicker(h.heartbeatInterval)
		tickChan = ticker.C
		defer ticker.Stop()
	}

	var finalErr *roachpb.Error
	defer func() {
		h.mu.Lock()
		// Prevent future SendLocked() calls.
		if finalErr != nil {
			h.mu.finalErr = finalErr
		}
		if h.mu.txnEnd != nil {
			h.mu.txnEnd = nil
		}
		h.mu.Unlock()
	}()

	var closer <-chan struct{}
	{
		h.mu.Lock()
		closer = h.mu.txnEnd
		h.mu.Unlock()
		if closer == nil {
			return
		}
	}
	// Loop with ticker for periodic heartbeats.
	for {
		select {
		case <-tickChan:
			if !h.heartbeat(ctx) {
				// This error we're generating here should not be seen by clients. Since
				// the transaction is aborted, they should be rejected before they reach
				// this interceptor.
				finalErr = roachpb.NewErrorf("heartbeat failed fatally")
				return
			}
		case <-closer:
			// Transaction finished normally.
			finalErr = roachpb.NewErrorf("txnHeartbeat already closed")
			return
		case <-h.stopper.ShouldQuiesce():
			finalErr = roachpb.NewErrorf("node already quiescing")
			return
		}
	}
}

// heartbeat sends a HeartbeatTxnRequest to the txn record.
// Errors that carry update txn information (e.g. TransactionAbortedError) will
// update the txn. Other errors are swallowed.
// Returns true if heartbeating should continue, false if the transaction is no
// longer Pending and so there's no point in heartbeating further.
func (h *txnHeartbeat) heartbeat(ctx context.Context) bool {
	// Like with the TxnCoordSender, the locking here is peculiar. The lock is not
	// held continuously throughout this method: we acquire the lock here and
	// then, inside the wrapped.Send() call, the interceptor at the bottom of the
	// stack will unlock until it receives a response.
	h.mu.Lock()
	defer h.mu.Unlock()

	// If the txn is no longer pending, there's nothing for us to heartbeat.
	// This h.heartbeat() call could have raced with a response that updated the
	// status. That response is supposed to have closed the txnHeartbeat.
	if h.mu.txn.Status != roachpb.PENDING {
		if h.mu.txnEnd != nil {
			log.Fatalf(ctx, "txn status: %s, but heartbeat loop hasn't been signaled to stop", h.mu.txn.Status)
		}
		return false
	}

	// Clone the txn in order to put it in the heartbeat request.
	txn := h.mu.txn.Clone()

	if txn.Key == nil {
		log.Fatalf(ctx, "attempting to heartbeat txn without anchor key: %v", txn)
	}

	ba := roachpb.BatchRequest{}
	ba.Txn = &txn

	hb := &roachpb.HeartbeatTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txn.Key,
		},
		Now: h.clock.Now(),
	}
	ba.Add(hb)

	log.VEventf(ctx, 2, "heartbeat %s", h.mu.txn)
	br, pErr := h.gatekeeper.SendLocked(ctx, ba)

	var respTxn *roachpb.Transaction
	if pErr != nil {
		log.VEventf(ctx, 2, "heartbeat failed: %s", pErr)

		// If the heartbeat request arrived to find a missing transaction record
		// then we ignore the error. This is possible if the heartbeat loop was
		// started before a BeginTxn request succeeds because of ambiguity in the
		// first write request's response.
		if tse, ok := pErr.GetDetail().(*roachpb.TransactionStatusError); ok &&
			tse.Reason == roachpb.TransactionStatusError_REASON_TXN_NOT_FOUND {
			return true
		}
		log.Infof(ctx, "!!! heartbeat failed: %s", pErr)

		if pErr.GetTxn() != nil {
			// It is not expected for a 2.1 node to return an error with a transaction
			// in it. For one, heartbeats are not supposed to return
			// TransactionAbortedErrors.
			// TODO(andrei): Remove this in 2.2.
			respTxn = pErr.GetTxn()
		} else {
			return true
		}
	} else {
		respTxn = br.Responses[0].GetInner().(*roachpb.HeartbeatTxnResponse).Txn
	}

	// Update our txn. In particular, we need to make sure that the client will
	// notice when the txn has been aborted (in which case we'll give them an
	// error on their next request).
	h.mu.txn.Update(respTxn)
	if h.mu.txn.Status != roachpb.PENDING {
		if h.mu.txn.Status == roachpb.ABORTED {
			log.Infof(ctx, "!!! Heartbeat detected aborted txn %s. Cleaning up.", h.mu.txn)
			log.VEventf(ctx, 1, "Heartbeat detected aborted txn. Cleaning up.")
			h.abortTxnAsyncLocked(ctx)
		}
		return false
	}
	return true
}

// abortTxnAsyncLocked send an EndTransaction(commmit=false) asynchronously.
// The asyncAbortCallbackLocked callback is also called.
func (h *txnHeartbeat) abortTxnAsyncLocked(ctx context.Context) {
	// Stop the heartbeat loop if it is still running.
	if h.mu.txnEnd != nil {
		close(h.mu.txnEnd)
		h.mu.txnEnd = nil
	}

	if h.mu.txn.Status != roachpb.ABORTED {
		log.Fatalf(ctx, "abortTxnAsyncLocked called for non-aborted txn: %s", h.mu.txn)
	}
	h.asyncAbortCallbackLocked(ctx)
	txn := h.mu.txn.Clone()

	// NB: We use context.Background() here because we don't want a canceled
	// context to interrupt the aborting.
	ctx = h.AnnotateCtx(context.Background())

	// Construct a batch with an EndTransaction request.
	ba := roachpb.BatchRequest{}
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&roachpb.EndTransactionRequest{
		Commit: false,
		// Resolved intents should maintain an abort span entry to prevent
		// concurrent requests from failing to notice the transaction was aborted.
		Poison: true,
	})

	log.VEventf(ctx, 2, "async abort for txn: %s", txn)
	if err := h.stopper.RunAsyncTask(
		ctx, "txnHeartbeat: aborting txn", func(ctx context.Context) {
			// Send the abort request through the interceptor stack. This is important
			// because we need the txnIntentCollector to append intents to the
			// EndTransaction request.
			h.mu.Lock()
			defer h.mu.Unlock()
			_, pErr := h.wrapped.SendLocked(ctx, ba)
			if pErr != nil {
				log.VErrEventf(ctx, 1, "async abort failed for %s: %s ", txn, pErr)
			}
		},
	); err != nil {
		log.Warning(ctx, err)
	}
}

// firstWriteIndex returns the index of the first transactional write in the
// BatchRequest. Returns -1 if the batch has not intention to write. It also
// verifies that if an EndTransactionRequest is included, then it is the last
// request in the batch.
func firstWriteIndex(ba *roachpb.BatchRequest) (int, *roachpb.Error) {
	for i, ru := range ba.Requests {
		args := ru.GetInner()
		if i < len(ba.Requests)-1 /* if not last*/ {
			if _, ok := args.(*roachpb.EndTransactionRequest); ok {
				return -1, roachpb.NewErrorf("%s sent as non-terminal call", args.Method())
			}
		}
		if roachpb.IsTransactionWrite(args) {
			return i, nil
		}
	}
	return -1, nil
}
