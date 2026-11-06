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

var subscribeStateTranstions = map[subscribeState]subscribeState{
	subscribeStateIdle:       subscribeStateStarting | subscribeStateStopped,
	subscribeStateStarting:   subscribeStateRunning | subscribeStateStopped,
	subscribeStateRunning:    subscribeStateRecovering | subscribeStateStopping | subscribeStateStopped,
	subscribeStateRecovering: subscribeStateRunning | subscribeStateStopped | subscribeStateStopping,
	subscribeStateStopping:   subscribeStateStopped,
}

var ErrInvalidSubscribeStateTransition = errors.New("invalid subscribe state transition")

type subscribeStateTransitionError struct {
	err    error
	msg    string
	queue  string
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
	mu                   *sync.Mutex
	cond                 *sync.Cond
	state                subscribeState
	overrideState        *subscribeState
	error                *subscribeStateTransitionError
	transitionRegistryMu sync.RWMutex
	transitionRegistry   map[subscribeState]subscribeStateTransitionFunc
	startupDone          chan struct{}
	started              bool
}

func newSubscribeStateMachine() *subscribeStateMachine {
	lock := new(sync.Mutex)
	return &subscribeStateMachine{
		mu:                 lock,
		cond:               sync.NewCond(lock),
		state:              subscribeStateIdle,
		transitionRegistry: make(map[subscribeState]subscribeStateTransitionFunc),
		startupDone:        make(chan struct{}),
	}
}

func (sm *subscribeStateMachine) getState() subscribeState {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.state
}

func (sm *subscribeStateMachine) interruptState(newState subscribeState) error {
	currentState := sm.getState()
	if newState == currentState {
		return nil
	}
	if allowed, ok := subscribeStateTranstions[currentState]; ok {
		if allowed&newState == 0 {
			return ErrInvalidSubscribeStateTransition
		}
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.overrideState = &newState
	return nil
}

func (sm *subscribeStateMachine) registerTransitionFunc(state subscribeState, fn subscribeStateTransitionFunc) {
	sm.transitionRegistryMu.Lock()
	defer sm.transitionRegistryMu.Unlock()
	sm.transitionRegistry[state] = fn
}

func (sm *subscribeStateMachine) waitForState(targetState subscribeState) error {
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
	currentState := sm.getState()
	if newState == currentState {
		return nil
	}
	logger.Debugf("transitioning subscribe state from %v to %v", currentState, newState)
	if allowed, ok := subscribeStateTranstions[currentState]; ok {
		if allowed&newState == 0 {
			return ErrInvalidSubscribeStateTransition
		}
	}
	sm.mu.Lock()
	sm.state = newState
	sm.cond.Signal()
	sm.mu.Unlock()
	nextState, err := sm.callTransitionFunc(newState)
	logger.Debugf("subscribe state transition from %v to %v returned next state %v with error %v", newState, nextState, nextState, err)

	sm.mu.Lock()
	if sm.overrideState != nil {
		logger.Debugf("overriding subscribe state to %v instead of %v", *sm.overrideState, newState)
		sm.state = *sm.overrideState
		sm.overrideState = nil
	}
	sm.mu.Unlock()
	if subscribeStateTranstions[nextState] == 0 {
		logger.Debugf("next state %v is terminal, exiting", nextState)
		sm.mu.Lock()
		sm.state = nextState
		sm.cond.Broadcast()
		sm.mu.Unlock()
		return nil
	}
	return sm.transitionState(nextState)
}

func (sm *subscribeStateMachine) callTransitionFunc(state subscribeState) (subscribeState, *subscribeStateTransitionError) {
	sm.transitionRegistryMu.RLock()
	defer sm.transitionRegistryMu.RUnlock()
	if fn, ok := sm.transitionRegistry[state]; ok {
		nextState, err := fn()
		sm.error = err
		if err != nil && sm.getState() < subscribeStateRecovering {
			logger.Warn("%s [%s]", err.msg, err.queue)
		} else {
			if err != nil {
				logger.Debugf("subscribe state transition from %v to %v error: %s", state, nextState, err.msg)
			} else {
				logger.Debugf("subscribe state transition successful from %v to %v", state, nextState)
			}
		}
		return nextState, err
	}
	// no function registered for this state, return current state and nil error
	logger.Debugf("no transition function registered for state %v", state)
	return sm.getState(), nil
}

func (cm *subscribeStateMachine) waitUntilStopped() {
	cm.mu.Lock()
	started := cm.started
	defer cm.mu.Unlock()
	if !started {
		return
	}
	logger.Debugf("waiting until subscribe state machine stops")
	<-cm.startupDone
}
