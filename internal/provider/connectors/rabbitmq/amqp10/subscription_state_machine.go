// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"errors"
	"fmt"
	"sync"

	"github.com/sassoftware/arke/internal/util"
)

// at some point we'll have scoped loggers
var logger = util.Logger

type subscribeState int

const (
	subscribeStateNone subscribeState = 0
	subscribeStateIdle subscribeState = 1 << iota
	subscribeStateStarting
	subscribeStateRunning
	subscribeStateRecovering
	subscribeStateStopping
	subscribeStateStopped
)

func (s *subscribeState) String() string {
	switch *s {
	case subscribeStateIdle:
		return "idle"
	case subscribeStateStarting:
		return "starting"
	case subscribeStateRunning:
		return "running"
	case subscribeStateRecovering:
		return "recovering"
	case subscribeStateStopping:
		return "stopping"
	case subscribeStateStopped:
		return "stopped"
	default:
		return "unknown"
	}
}

var subscribeStateTransitions = map[subscribeState]subscribeState{
	subscribeStateIdle:       subscribeStateStarting | subscribeStateStopped,
	subscribeStateStarting:   subscribeStateRunning | subscribeStateStopped,
	subscribeStateRunning:    subscribeStateRecovering | subscribeStateStopping | subscribeStateStopped,
	subscribeStateRecovering: subscribeStateRunning | subscribeStateStopped | subscribeStateStopping,
	subscribeStateStopping:   subscribeStateStopped,
	subscribeStateStopped:    subscribeStateNone,
}

// isValidSubscribeState rejects the zero sentinel, combined state masks, and
// unknown bits. States are single-bit values so they can be ORed into allowed
// transition masks; state&(state-1) is zero only when exactly one bit is set.
// Requiring a transition-map entry also keeps only declared states valid,
// including Stopped, whose entry explicitly has no allowed transitions.
func isValidSubscribeState(state subscribeState) bool {
	if state == subscribeStateNone || state&(state-1) != 0 {
		return false
	}
	_, ok := subscribeStateTransitions[state]
	return ok
}

var ErrInvalidSubscribeStateTransition = errors.New("invalid subscribe state transition")
var ErrInvalidSubscribeState = errors.New("invalid subscribe state")

type subscribeStateTransitionError struct {
	err              error
	msg              string
	queue            string
	clientIdentifier string
}

func (s *subscribeStateTransitionError) Error() string {
	var errStr = fmt.Sprintf("client: %s, queue: %s, msg: %s, ", s.clientIdentifier, s.queue, s.msg)
	if s.err != nil {
		return fmt.Sprintf("%s, err: %v", errStr, s.err)
	}
	return errStr
}

// subscribeStateTransitionFunc defines a function signature for transition hooks.
type subscribeStateTransitionFunc func() (newState subscribeState, err *subscribeStateTransitionError)

type subscribeStateMachine struct {
	transitionMu         sync.Mutex
	mu                   *sync.Mutex
	cond                 *sync.Cond
	state                subscribeState
	transitionRegistryMu sync.RWMutex
	transitionRegistry   map[subscribeState]subscribeStateTransitionFunc
}

func newSubscribeStateMachine() *subscribeStateMachine {
	lock := new(sync.Mutex)
	return &subscribeStateMachine{
		mu:                 lock,
		cond:               sync.NewCond(lock),
		state:              subscribeStateIdle,
		transitionRegistry: make(map[subscribeState]subscribeStateTransitionFunc),
	}
}

func (sm *subscribeStateMachine) getState() subscribeState {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.state
}

func (sm *subscribeStateMachine) registerTransitionFunc(state subscribeState, fn subscribeStateTransitionFunc) {
	sm.transitionRegistryMu.Lock()
	defer sm.transitionRegistryMu.Unlock()
	sm.transitionRegistry[state] = fn
}

func (sm *subscribeStateMachine) waitForState(targetState subscribeState) error {
	if !isValidSubscribeState(targetState) {
		return ErrInvalidSubscribeState
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	currentState := sm.state
	for {
		if currentState == targetState {
			return nil
		}
		if currentState&(subscribeStateStopping|subscribeStateStopped) != 0 {
			return errors.New("subscribe state machine entered stopping or stopped state")
		}
		sm.cond.Wait()
		currentState = sm.state
	}
}

func (sm *subscribeStateMachine) transitionState(newState subscribeState) error {
	sm.transitionMu.Lock()
	defer sm.transitionMu.Unlock()
	return sm.transitionStateInner(newState, nil)
}

func (sm *subscribeStateMachine) transitionStateInner(newState subscribeState, transitionErr *subscribeStateTransitionError) error {
	// Looping rather than recursing keeps the stack flat for subscriptions that
	// cycle between Running and Recovering for their whole lifetime.
	for {
		sm.mu.Lock()
		currentState := sm.state
		logger.Debugf("transitioning subscribe state from %s to %s", currentState.String(), newState.String())

		if !isValidSubscribeState(currentState) || !isValidSubscribeState(newState) {
			sm.mu.Unlock()
			return ErrInvalidSubscribeStateTransition
		}
		if newState == currentState {
			sm.mu.Unlock()
			if transitionErr == nil {
				return nil
			}
			return transitionErr
		}
		if subscribeStateTransitions[currentState]&newState != newState {
			sm.mu.Unlock()
			return ErrInvalidSubscribeStateTransition
		}
		sm.state = newState
		// Waiters can target different states; wake all so each can check its target.
		sm.cond.Broadcast()
		sm.mu.Unlock()

		nextState, err := sm.callTransitionFunc(newState)
		if err != nil {
			logger.Debugf("subscribe state transition from %s to %s returned error: %v", newState.String(), nextState.String(), err)
			transitionErr = err
		}
		if !isValidSubscribeState(nextState) {
			return ErrInvalidSubscribeStateTransition
		}

		if subscribeStateTransitions[nextState] == subscribeStateNone {
			logger.Debugf("next state %s is terminal, exiting", nextState.String())
			sm.mu.Lock()
			sm.state = nextState
			sm.cond.Broadcast()
			sm.mu.Unlock()
			if transitionErr == nil {
				return nil
			}
			return transitionErr
		}
		newState = nextState
	}
}

func (sm *subscribeStateMachine) callTransitionFunc(state subscribeState) (subscribeState, *subscribeStateTransitionError) {
	// Hooks such as running() block for the life of the subscription, so the
	// registry lock must not be held while one executes.
	sm.transitionRegistryMu.RLock()
	fn, ok := sm.transitionRegistry[state]
	sm.transitionRegistryMu.RUnlock()
	if ok {
		nextState, err := fn()
		if err != nil && sm.getState() < subscribeStateRecovering {
			logger.Warn("%s [%s]", err.msg, err.queue)
		} else {
			if err != nil {
				logger.Debugf("subscribe state transition from %s to %s error: %s", state.String(), nextState.String(), err.msg)
			} else {
				logger.Debugf("subscribe state transition successful from %s to %s", state.String(), nextState.String())
			}
		}
		return nextState, err
	}
	// no function registered for this state, return current state and nil error
	logger.Debugf("no transition function registered for state %v", state)
	return sm.getState(), nil
}

func (sm *subscribeStateMachine) waitUntilStopped() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	logger.Debugf("waiting until subscribe state machine stops")
	for sm.state != subscribeStateStopped {
		// Recheck after each wakeup; only the terminal state completes shutdown.
		sm.cond.Wait()
	}
}
