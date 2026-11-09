// Copyright © 2026, SAS Institute Inc., Cary, NC, USA. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_subscribeStateString(t *testing.T) {
	tests := []struct {
		name  string
		state subscribeState
		want  string
	}{
		{name: "idle", state: subscribeStateIdle, want: "idle"},
		{name: "starting", state: subscribeStateStarting, want: "starting"},
		{name: "running", state: subscribeStateRunning, want: "running"},
		{name: "recovering", state: subscribeStateRecovering, want: "recovering"},
		{name: "stopping", state: subscribeStateStopping, want: "stopping"},
		{name: "stopped", state: subscribeStateStopped, want: "stopped"},
		{name: "unknown", state: 1 << 10, want: "unknown"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := test.state
			assert.Equal(t, test.want, state.String())
		})
	}
}

func Test_newSubscribeStateMachine(t *testing.T) {
	sm := newSubscribeStateMachine()

	assert.Equal(t, subscribeStateIdle, sm.getState())
	assert.NotNil(t, sm.mu)
	assert.NotNil(t, sm.cond)
	assert.Empty(t, sm.transitionRegistry)
}

func Test_subscribeStateMachineTransitionState(t *testing.T) {
	t.Run("same state is a no-op", func(t *testing.T) {
		sm := newSubscribeStateMachine()

		require.NoError(t, sm.transitionState(subscribeStateIdle))
		assert.Equal(t, subscribeStateIdle, sm.getState())
	})

	t.Run("allowed transitions", func(t *testing.T) {
		tests := []struct {
			from subscribeState
			to   subscribeState
		}{
			{from: subscribeStateIdle, to: subscribeStateStarting},
			{from: subscribeStateIdle, to: subscribeStateStopped},
			{from: subscribeStateStarting, to: subscribeStateRunning},
			{from: subscribeStateStarting, to: subscribeStateStopped},
			{from: subscribeStateRunning, to: subscribeStateRecovering},
			{from: subscribeStateRunning, to: subscribeStateStopping},
			{from: subscribeStateRunning, to: subscribeStateStopped},
			{from: subscribeStateRecovering, to: subscribeStateRunning},
			{from: subscribeStateRecovering, to: subscribeStateStopped},
			{from: subscribeStateRecovering, to: subscribeStateStopping},
			{from: subscribeStateStopping, to: subscribeStateStopped},
		}

		for _, test := range tests {
			sm := newSubscribeStateMachine()
			sm.state = test.from

			require.NoError(t, sm.transitionState(test.to), "%v -> %v", test.from, test.to)
			assert.Equal(t, test.to, sm.getState())
		}
	})

	t.Run("invalid transitions leave the current state unchanged", func(t *testing.T) {
		tests := []struct {
			from subscribeState
			to   subscribeState
		}{
			{from: subscribeStateIdle, to: subscribeStateRunning},
			{from: subscribeStateStarting, to: subscribeStateRecovering},
			{from: subscribeStateRunning, to: subscribeStateStarting},
			{from: subscribeStateRecovering, to: subscribeStateStarting},
			{from: subscribeStateStopping, to: subscribeStateRunning},
			{from: subscribeStateStopped, to: subscribeStateRunning},
			{from: subscribeStateNone, to: subscribeStateRunning},
			{from: subscribeStateIdle, to: subscribeStateIdle | subscribeStateStarting},
			{from: subscribeStateIdle | subscribeStateRunning, to: subscribeStateStopping},
		}

		for _, test := range tests {
			sm := newSubscribeStateMachine()
			sm.state = test.from

			err := sm.transitionState(test.to)

			assert.ErrorIs(t, err, ErrInvalidSubscribeStateTransition, "%v -> %v", test.from, test.to)
			assert.Equal(t, test.from, sm.getState())
		}
	})
}

func Test_subscribeStateMachineTransitionHooks(t *testing.T) {
	sm := newSubscribeStateMachine()
	var calls []subscribeState
	sm.registerTransitionFunc(subscribeStateStarting, func() (subscribeState, *subscribeStateTransitionError) {
		calls = append(calls, subscribeStateStarting)
		return subscribeStateRunning, nil
	})
	sm.registerTransitionFunc(subscribeStateRunning, func() (subscribeState, *subscribeStateTransitionError) {
		calls = append(calls, subscribeStateRunning)
		return subscribeStateStopping, nil
	})
	sm.registerTransitionFunc(subscribeStateStopping, func() (subscribeState, *subscribeStateTransitionError) {
		calls = append(calls, subscribeStateStopping)
		return subscribeStateStopped, nil
	})

	require.NoError(t, sm.transitionState(subscribeStateStarting))

	assert.Equal(t, []subscribeState{
		subscribeStateStarting,
		subscribeStateRunning,
		subscribeStateStopping,
	}, calls)
	assert.Equal(t, subscribeStateStopped, sm.getState())
}

func Test_subscribeStateMachineCallTransitionFunc(t *testing.T) {
	t.Run("registered hook returns next state and error", func(t *testing.T) {
		sm := newSubscribeStateMachine()
		transitionErr := &subscribeStateTransitionError{
			err:   errors.New("test transition failed"),
			msg:   "transition failed",
			queue: "queue",
		}
		sm.registerTransitionFunc(subscribeStateStarting, func() (subscribeState, *subscribeStateTransitionError) {
			return subscribeStateRecovering, transitionErr
		})

		nextState, err := sm.callTransitionFunc(subscribeStateStarting)

		assert.Equal(t, subscribeStateRecovering, nextState)
		assert.Same(t, transitionErr, err)
	})

	t.Run("missing hook returns current state", func(t *testing.T) {
		sm := newSubscribeStateMachine()

		nextState, err := sm.callTransitionFunc(subscribeStateIdle)

		assert.Equal(t, subscribeStateIdle, nextState)
		assert.Nil(t, err)
	})
}

func Test_subscribeStateMachineReturnsHookErrorAfterRecovery(t *testing.T) {
	sm := newSubscribeStateMachine()
	sm.state = subscribeStateStarting
	transitionErr := &subscribeStateTransitionError{
		err:   errors.New("consumer failed"),
		msg:   "consumer failed",
		queue: "queue",
	}
	recoveryHookCalled := false
	sm.registerTransitionFunc(subscribeStateRunning, func() (subscribeState, *subscribeStateTransitionError) {
		return subscribeStateRecovering, transitionErr
	})
	sm.registerTransitionFunc(subscribeStateRecovering, func() (subscribeState, *subscribeStateTransitionError) {
		recoveryHookCalled = true
		return subscribeStateStopped, nil
	})

	err := sm.transitionState(subscribeStateRunning)

	assert.Same(t, transitionErr, err)
	assert.True(t, recoveryHookCalled)
	assert.Equal(t, subscribeStateStopped, sm.getState())
}

func Test_subscribeStateMachineRejectsInvalidHookState(t *testing.T) {
	sm := newSubscribeStateMachine()
	sm.registerTransitionFunc(subscribeStateStarting, func() (subscribeState, *subscribeStateTransitionError) {
		return subscribeStateNone, nil
	})

	err := sm.transitionState(subscribeStateStarting)

	assert.ErrorIs(t, err, ErrInvalidSubscribeStateTransition)
	assert.Equal(t, subscribeStateStarting, sm.getState())
}

func Test_subscribeStateMachineInterruptState(t *testing.T) {
	t.Run("same state is a no-op", func(t *testing.T) {
		sm := newSubscribeStateMachine()

		require.NoError(t, sm.interruptState(subscribeStateIdle))
		assert.Nil(t, sm.overrideState)
		assert.Equal(t, subscribeStateIdle, sm.getState())
	})

	t.Run("valid interrupt records an override", func(t *testing.T) {
		sm := newSubscribeStateMachine()

		require.NoError(t, sm.interruptState(subscribeStateStarting))
		require.NotNil(t, sm.overrideState)
		assert.Equal(t, subscribeStateStarting, *sm.overrideState)
		assert.Equal(t, subscribeStateIdle, sm.getState())
	})

	t.Run("invalid interrupt is rejected", func(t *testing.T) {
		tests := []struct {
			current subscribeState
			next    subscribeState
		}{
			{current: subscribeStateIdle, next: subscribeStateRunning},
			{current: subscribeStateStopped, next: subscribeStateRunning},
			{current: 1 << 10, next: subscribeStateRunning},
			{current: subscribeStateIdle, next: subscribeStateIdle | subscribeStateStarting},
			{current: subscribeStateIdle | subscribeStateRunning, next: subscribeStateStopping},
		}
		for _, test := range tests {
			sm := newSubscribeStateMachine()
			sm.state = test.current

			err := sm.interruptState(test.next)

			assert.ErrorIs(t, err, ErrInvalidSubscribeStateTransition)
			assert.Nil(t, sm.overrideState)
		}
	})
}

func Test_subscribeStateMachineSerializesConcurrentTransitions(t *testing.T) {
	sm := newSubscribeStateMachine()
	sm.state = subscribeStateRunning

	recoveringHookEntered := make(chan struct{})
	releaseRecoveringHook := make(chan struct{})
	var releaseOnce sync.Once
	releaseHook := func() { releaseOnce.Do(func() { close(releaseRecoveringHook) }) }
	t.Cleanup(releaseHook)
	stoppingHookEntered := make(chan struct{}, 1)
	sm.registerTransitionFunc(subscribeStateRecovering, func() (subscribeState, *subscribeStateTransitionError) {
		close(recoveringHookEntered)
		<-releaseRecoveringHook
		return subscribeStateStopped, nil
	})
	sm.registerTransitionFunc(subscribeStateStopping, func() (subscribeState, *subscribeStateTransitionError) {
		stoppingHookEntered <- struct{}{}
		return subscribeStateStopped, nil
	})

	firstResult := make(chan error, 1)
	go func() { firstResult <- sm.transitionState(subscribeStateRecovering) }()
	select {
	case <-recoveringHookEntered:
	case <-time.After(time.Second):
		t.Fatal("recovering hook did not start")
	}

	secondStarted := make(chan struct{})
	secondResult := make(chan error, 1)
	go func() {
		close(secondStarted)
		secondResult <- sm.transitionState(subscribeStateStopping)
	}()
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("second transition did not start")
	}
	select {
	case <-stoppingHookEntered:
		t.Fatal("stopping hook ran while recovering hook was active")
	case <-time.After(25 * time.Millisecond):
	}

	releaseHook()
	select {
	case err := <-firstResult:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("recovering transition did not finish")
	}
	select {
	case err := <-secondResult:
		assert.ErrorIs(t, err, ErrInvalidSubscribeStateTransition)
	case <-time.After(time.Second):
		t.Fatal("second transition did not finish after the first")
	}
	assert.Equal(t, subscribeStateStopped, sm.getState())
}

func Test_subscribeStateMachineInterruptOverridesHook(t *testing.T) {
	sm := newSubscribeStateMachine()
	runningHookCalled := false
	sm.registerTransitionFunc(subscribeStateStarting, func() (subscribeState, *subscribeStateTransitionError) {
		if err := sm.interruptState(subscribeStateStopped); err != nil {
			return subscribeStateStopped, &subscribeStateTransitionError{err: err}
		}
		return subscribeStateRunning, nil
	})
	sm.registerTransitionFunc(subscribeStateRunning, func() (subscribeState, *subscribeStateTransitionError) {
		runningHookCalled = true
		return subscribeStateStopped, nil
	})

	err := sm.transitionState(subscribeStateStarting)
	require.NoError(t, err, "transition to starting state failed: %v", err)

	assert.Equal(t, subscribeStateStopped, sm.getState(), "expected state to be %s", subscribeStateStopped)
	assert.Nil(t, sm.overrideState)
	assert.False(t, runningHookCalled, "hook for the overridden state should not be called")
}

func Test_subscribeStateMachineWaitForState(t *testing.T) {
	t.Run("rejects invalid targets", func(t *testing.T) {
		for _, targetState := range []subscribeState{
			subscribeStateNone,
			subscribeStateIdle | subscribeStateStarting,
			1 << 10,
		} {
			sm := newSubscribeStateMachine()
			assert.ErrorIs(t, sm.waitForState(targetState), ErrInvalidSubscribeState)
		}
	})

	t.Run("returns when target state is reached", func(t *testing.T) {
		sm := newSubscribeStateMachine()
		sm.registerTransitionFunc(subscribeStateStarting, func() (subscribeState, *subscribeStateTransitionError) {
			return subscribeStateRunning, nil
		})
		result := make(chan error, 1)
		go func() {
			result <- sm.waitForState(subscribeStateRunning)
		}()

		require.NoError(t, sm.transitionState(subscribeStateStarting))
		select {
		case err := <-result:
			assert.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("waitForState did not return after reaching its target")
		}
	})

	t.Run("returns an error when stopping starts before target", func(t *testing.T) {
		for _, terminalState := range []subscribeState{subscribeStateStopping, subscribeStateStopped} {
			sm := newSubscribeStateMachine()
			sm.mu.Lock()
			sm.state = terminalState
			sm.mu.Unlock()

			err := sm.waitForState(subscribeStateRunning)

			assert.Error(t, err)
		}
	})
}

func Test_subscribeStateMachineWaitForStateBroadcastsOnStop(t *testing.T) {
	sm := newSubscribeStateMachine()
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			results <- sm.waitForState(subscribeStateRunning)
		}()
	}

	require.NoError(t, sm.transitionState(subscribeStateStopped))
	for i := 0; i < 2; i++ {
		select {
		case err := <-results:
			assert.Error(t, err)
		case <-time.After(time.Second):
			t.Fatal("a waiter did not return after the machine stopped")
		}
	}
}

func Test_subscribeStateMachineWaitUntilStopped(t *testing.T) {
	sm := newSubscribeStateMachine()
	sm.mu.Lock()
	sm.state = subscribeStateStarting
	sm.mu.Unlock()
	done := make(chan struct{})
	go func() {
		sm.waitUntilStopped()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("waitUntilStopped returned before the state machine stopped")
	case <-time.After(25 * time.Millisecond):
	}
	require.NoError(t, sm.transitionState(subscribeStateStopped))
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("waitUntilStopped did not return after the state machine stopped")
	}
}
