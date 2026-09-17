package session

import (
	"strings"
	"unicode"

	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
)

// turnMode is the turn_detection mode in effect.
type turnMode int

const (
	modeManual turnMode = iota
	modeServerVAD
	modeSemanticVAD
)

// TurnInput is the sealed set of facts and commands fed to the TurnManager.
type TurnInput interface{ isTurnInput() }

type (
	vadSpeechStart       struct{}
	vadSpeechEnd         struct{}
	asrFinal             struct{ Text string }
	asrEndOfTurn         struct{}
	clientCommit         struct{}
	clientCreateResponse struct{}
	responseStarted      struct{}
	responseEnded        struct{}
)

func (vadSpeechStart) isTurnInput()       {}
func (vadSpeechEnd) isTurnInput()         {}
func (asrFinal) isTurnInput()             {}
func (asrEndOfTurn) isTurnInput()         {}
func (clientCommit) isTurnInput()         {}
func (clientCreateResponse) isTurnInput() {}
func (responseStarted) isTurnInput()      {}
func (responseEnded) isTurnInput()        {}

// TurnDecision is what the actor must do next. ArmEndOfTurnTimer asks the
// actor to start the semantic_vad eagerness timer, which feeds asrEndOfTurn
// when it fires. ArmFinalWaitTimer asks for the bounded wait used when speech
// ended with no Final to judge; it feeds asrEndOfTurn the same way, and a
// Final arriving first replaces it through judge. The two are never set
// together.
//
// DeferTrigger is semantic_vad's form of Trigger: the response is created
// only once the committed item's transcript is known, and not at all when
// that transcript is empty (non-speech noise). Trigger and DeferTrigger are
// never set together.
type TurnDecision struct {
	EmitSpeechStarted bool
	EmitSpeechStopped bool
	Interrupt         bool
	Commit            bool
	Trigger           bool
	DeferTrigger      bool
	ArmEndOfTurnTimer bool
	ArmFinalWaitTimer bool
}

// TurnManager is a pure state machine: no goroutines, no timers, no access
// to the conversation. The actor feeds it facts and executes its decisions.
type TurnManager struct {
	mode              turnMode
	createResponse    bool
	interruptResponse bool

	responseActive bool // from EvResponseCreated (including awaiting) to EvResponseDone
	speaking       bool
	pendingEOT     bool // semantic_vad: speech ended, waiting for end of turn
	finalPending   bool // semantic_vad: a Final arrived during speech, not yet consumed
	finalTerminal  bool // that Final ended with sentence punctuation
}

// newTurnManager derives the mode and switches from turn_detection; nil means
// manual mode.
func newTurnManager(td *config.TurnDetection) *TurnManager {
	tm := &TurnManager{}
	if td == nil {
		return tm
	}
	switch td.Type {
	case config.TurnDetectionServerVAD:
		tm.mode = modeServerVAD
	case config.TurnDetectionSemanticVAD:
		tm.mode = modeSemanticVAD
	}
	tm.createResponse = td.CreateResponse
	tm.interruptResponse = td.InterruptResponse
	return tm
}

// vadEnabled reports whether the acoustic VAD runs in this mode.
func (t *TurnManager) vadEnabled() bool { return t.mode != modeManual }

// Step applies one input and returns the decision.
func (t *TurnManager) Step(in TurnInput) TurnDecision {
	var d TurnDecision
	switch in := in.(type) {
	case responseStarted:
		t.responseActive = true
	case responseEnded:
		t.responseActive = false
	case clientCommit:
		d.Commit = true
		t.resetTurn()
	case clientCreateResponse:
		d.Trigger = true
	case vadSpeechStart:
		if t.mode == modeManual {
			return d
		}
		t.speaking = true
		t.pendingEOT = false
		t.finalPending = false
		d.EmitSpeechStarted = true
		d.Interrupt = t.interruptResponse && t.responseActive
	case vadSpeechEnd:
		if t.mode == modeManual || !t.speaking {
			return d
		}
		t.speaking = false
		d.EmitSpeechStopped = true
		switch t.mode {
		case modeServerVAD:
			t.endTurn(&d)
		case modeSemanticVAD:
			t.pendingEOT = true
			if t.finalPending {
				t.finalPending = false
				t.judge(t.finalTerminal, &d)
			} else {
				// Some recognizers emit no Final for a segment (a noise
				// burst). Without a bound the turn would wait forever and
				// the marked buffer segment would never be released.
				d.ArmFinalWaitTimer = true
			}
		}
	case asrFinal:
		if t.mode != modeSemanticVAD {
			return d
		}
		terminal := endsSentence(in.Text)
		if t.pendingEOT {
			t.judge(terminal, &d)
		} else if t.speaking {
			t.finalPending = true
			t.finalTerminal = terminal
		}
	case asrEndOfTurn:
		if t.mode == modeSemanticVAD && t.pendingEOT {
			t.endTurn(&d)
		}
	}
	return d
}

// judge implements the semantic_vad approximation after speech has ended:
// a Final ending in sentence punctuation ends the turn now, otherwise the
// eagerness timer is armed.
func (t *TurnManager) judge(terminal bool, d *TurnDecision) {
	if terminal {
		t.endTurn(d)
		return
	}
	d.ArmEndOfTurnTimer = true
}

func (t *TurnManager) endTurn(d *TurnDecision) {
	d.Commit = true
	if t.mode == modeSemanticVAD {
		d.DeferTrigger = t.createResponse
	} else {
		d.Trigger = t.createResponse
	}
	t.resetTurn()
}

func (t *TurnManager) resetTurn() {
	t.speaking = false
	t.pendingEOT = false
	t.finalPending = false
}

// endsSentence reports whether the trimmed text ends with sentence-ending
// punctuation (Latin or CJK), optionally followed by closing quotes.
func endsSentence(text string) bool {
	s := strings.TrimRightFunc(text, func(r rune) bool {
		return unicode.IsSpace(r) || isClosingQuote(r)
	})
	if s == "" {
		return false
	}
	last := []rune(s)
	return isTerminator(last[len(last)-1])
}
