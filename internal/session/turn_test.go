package session

import (
	"testing"

	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
)

func td(typ string, create, interrupt bool) *config.TurnDetection {
	if typ == "" {
		return nil
	}
	t := &config.TurnDetection{Type: typ, CreateResponse: create, InterruptResponse: interrupt}
	t.ApplyDefaults()
	return t
}

type step struct {
	in   TurnInput
	want TurnDecision
}

func runSteps(t *testing.T, tm *TurnManager, steps []step) {
	t.Helper()
	for i, s := range steps {
		got := tm.Step(s.in)
		if got != s.want {
			t.Fatalf("step %d (%T): got %+v, want %+v", i, s.in, got, s.want)
		}
	}
}

func TestManualModeEmitsNothingAndFollowsClient(t *testing.T) {
	tm := newTurnManager(td("", false, false))
	if tm.vadEnabled() {
		t.Fatal("VAD must not run in manual mode")
	}
	runSteps(t, tm, []step{
		{vadSpeechStart{}, TurnDecision{}},
		{vadSpeechEnd{}, TurnDecision{}},
		{asrFinal{"hi."}, TurnDecision{}},
		{asrEndOfTurn{}, TurnDecision{}},
		{clientCommit{}, TurnDecision{Commit: true}},
		{clientCreateResponse{}, TurnDecision{Trigger: true}},
		{responseStarted{}, TurnDecision{}},
		{vadSpeechStart{}, TurnDecision{}}, // no interrupt either
		{responseEnded{}, TurnDecision{}},
	})
}

func TestServerVADModeTable(t *testing.T) {
	cases := []struct {
		name              string
		create, interrupt bool
		wantStart         TurnDecision
		wantEnd           TurnDecision
	}{
		{"create+interrupt", true, true,
			TurnDecision{EmitSpeechStarted: true, Interrupt: true},
			TurnDecision{EmitSpeechStopped: true, Commit: true, Trigger: true}},
		{"create only", true, false,
			TurnDecision{EmitSpeechStarted: true},
			TurnDecision{EmitSpeechStopped: true, Commit: true, Trigger: true}},
		{"interrupt only", false, true,
			TurnDecision{EmitSpeechStarted: true, Interrupt: true},
			TurnDecision{EmitSpeechStopped: true, Commit: true}},
		{"neither", false, false,
			TurnDecision{EmitSpeechStarted: true},
			TurnDecision{EmitSpeechStopped: true, Commit: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tm := newTurnManager(td(config.TurnDetectionServerVAD, tc.create, tc.interrupt))
			runSteps(t, tm, []step{
				{responseStarted{}, TurnDecision{}}, // a response is in progress (may be awaiting)
				{vadSpeechStart{}, tc.wantStart},
				{vadSpeechEnd{}, tc.wantEnd},
			})
			// Without an active response, speech start never interrupts.
			tm.Step(responseEnded{})
			if got := tm.Step(vadSpeechStart{}); got != (TurnDecision{EmitSpeechStarted: true}) {
				t.Fatalf("idle speech start: %+v", got)
			}
			// asr facts are ignored in server_vad.
			if got := tm.Step(asrFinal{"x."}); got != (TurnDecision{}) {
				t.Fatalf("asrFinal in server_vad: %+v", got)
			}
			if got := tm.Step(asrEndOfTurn{}); got != (TurnDecision{}) {
				t.Fatalf("asrEndOfTurn in server_vad: %+v", got)
			}
		})
	}
}

func TestServerVADClientCommitAndSpeechEndWithoutStart(t *testing.T) {
	tm := newTurnManager(td(config.TurnDetectionServerVAD, true, true))
	if got := tm.Step(vadSpeechEnd{}); got != (TurnDecision{}) {
		t.Fatalf("speech end without start: %+v", got)
	}
	runSteps(t, tm, []step{
		{vadSpeechStart{}, TurnDecision{EmitSpeechStarted: true}},
		{clientCommit{}, TurnDecision{Commit: true}},
		{vadSpeechEnd{}, TurnDecision{}}, // turn already committed by the client
	})
}

func TestSemanticVADDefersUntilEndOfTurn(t *testing.T) {
	for _, create := range []bool{true, false} {
		tm := newTurnManager(td(config.TurnDetectionSemanticVAD, create, true))
		// Provider EndOfTurn ends the turn.
		runSteps(t, tm, []step{
			{vadSpeechStart{}, TurnDecision{EmitSpeechStarted: true}},
			{vadSpeechEnd{}, TurnDecision{EmitSpeechStopped: true}},
			{asrEndOfTurn{}, TurnDecision{Commit: true, Trigger: create}},
			{asrEndOfTurn{}, TurnDecision{}}, // duplicate is ignored
		})
		// Final with terminal punctuation after speech end ends the turn now.
		runSteps(t, tm, []step{
			{vadSpeechStart{}, TurnDecision{EmitSpeechStarted: true}},
			{vadSpeechEnd{}, TurnDecision{EmitSpeechStopped: true}},
			{asrFinal{"How are you?"}, TurnDecision{Commit: true, Trigger: create}},
		})
		// Final without punctuation arms the eagerness timer; timer → end of turn.
		runSteps(t, tm, []step{
			{vadSpeechStart{}, TurnDecision{EmitSpeechStarted: true}},
			{vadSpeechEnd{}, TurnDecision{EmitSpeechStopped: true}},
			{asrFinal{"so I was thinking"}, TurnDecision{ArmEndOfTurnTimer: true}},
			{asrEndOfTurn{}, TurnDecision{Commit: true, Trigger: create}},
		})
		// Final arriving before speech end is remembered.
		runSteps(t, tm, []step{
			{vadSpeechStart{}, TurnDecision{EmitSpeechStarted: true}},
			{asrFinal{"Done."}, TurnDecision{}},
			{vadSpeechEnd{}, TurnDecision{EmitSpeechStopped: true, Commit: true, Trigger: create}},
		})
		// Speech resuming while waiting cancels the pending end of turn.
		runSteps(t, tm, []step{
			{vadSpeechStart{}, TurnDecision{EmitSpeechStarted: true}},
			{vadSpeechEnd{}, TurnDecision{EmitSpeechStopped: true}},
			{asrFinal{"and then"}, TurnDecision{ArmEndOfTurnTimer: true}},
			{vadSpeechStart{}, TurnDecision{EmitSpeechStarted: true}},
			{asrEndOfTurn{}, TurnDecision{}}, // stale timer firing is ignored
			{vadSpeechEnd{}, TurnDecision{EmitSpeechStopped: true}},
			{asrFinal{"and then some more。"}, TurnDecision{Commit: true, Trigger: create}},
		})
	}
}

func TestSemanticVADInterrupt(t *testing.T) {
	tm := newTurnManager(td(config.TurnDetectionSemanticVAD, true, true))
	tm.Step(responseStarted{})
	if got := tm.Step(vadSpeechStart{}); got != (TurnDecision{EmitSpeechStarted: true, Interrupt: true}) {
		t.Fatalf("got %+v", got)
	}
}

func TestEndsSentence(t *testing.T) {
	for text, want := range map[string]bool{
		"Hello.": true, "Hello!  ": true, "Really?\"": true, "你好。": true, "好吗？": true,
		"Hello": false, "3.14": false, "": false, "   ": false, "wait...": true,
	} {
		if got := endsSentence(text); got != want {
			t.Errorf("endsSentence(%q) = %v, want %v", text, got, want)
		}
	}
}
