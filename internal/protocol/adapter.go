package protocol

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/session"
)

// Poster is the slice of the session the adapter drives.
type Poster interface {
	Post(session.Command) error
	PostAudio([]byte) error
}

// Options configures an Adapter for one connection.
type Options struct {
	SessionID string
	Model     string // from ?model=; empty → "cascade"
	Defaults  config.SessionDefaults
	Session   Poster
}

// Adapter converts GA client events into session Commands and session Events
// into GA server events. Inbound and Outbound may be called from different
// goroutines; the mutex protects the id tables and the event counter. It
// never blocks on the session beyond Post's own back-pressure.
type Adapter struct {
	mu sync.Mutex

	sess      Poster
	sessionID string
	convID    string
	model     string
	cfg       config.SessionDefaults

	eventPrefix string
	eventSeq    uint64

	items    map[session.ItemRef]*itemState
	itemByID map[string]session.ItemRef
	resps    map[session.ResponseRef]*respState
	respByID map[string]session.ResponseRef
}

type itemState struct {
	id      string
	prev    *string
	audioMs int
}

type respState struct {
	id        string
	itemID    string
	textOnly  bool
	modal     []string
	voice     string
	maxTokens config.MaxOutputTokens
	metadata  map[string]string
}

// New builds an adapter; SessionID and the conversation id are generated
// when empty.
func New(opts Options) *Adapter {
	a := &Adapter{
		sess:        opts.Session,
		sessionID:   opts.SessionID,
		convID:      newID("conv_"),
		model:       opts.Model,
		cfg:         opts.Defaults.Clone(),
		eventPrefix: newID("")[:6],
		items:       map[session.ItemRef]*itemState{},
		itemByID:    map[string]session.ItemRef{},
		resps:       map[session.ResponseRef]*respState{},
		respByID:    map[string]session.ResponseRef{},
	}
	if a.sessionID == "" {
		a.sessionID = newID("sess_")
	}
	if a.model == "" {
		a.model = defaultModel
	}
	return a
}

// SessionID returns the wire session id.
func (a *Adapter) SessionID() string { return a.sessionID }

// Hello returns the frames sent right after the upgrade: session.created
// and conversation.created.
func (a *Adapter) Hello() [][]byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	created := sessionEvent{base: a.base("session.created"), Session: sessionObject(a.sessionID, a.model, a.cfg)}
	conv := conversationCreatedEvent{base: a.base("conversation.created")}
	conv.Conversation.ID = a.convID
	conv.Conversation.Object = objectConversation
	return [][]byte{marshal(created), marshal(conv)}
}

func (a *Adapter) base(typ string) base { return base{EventID: a.eventID(), Type: typ} }

func marshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic("protocol: marshal server event: " + err.Error())
	}
	return b
}

// ---- inbound -----------------------------------------------------------------

// Inbound validates one client frame, posts the resulting command and
// returns any error frames to send back. It never returns other frames.
func (a *Adapter) Inbound(msg []byte) [][]byte {
	env, werr := decodeEnvelope(msg)
	if werr != nil {
		return a.reject(env.eventID, werr)
	}
	if werr := a.dispatch(env); werr != nil {
		return a.reject(env.eventID, werr)
	}
	return nil
}

func (a *Adapter) reject(clientEventID string, werr *wireErr) [][]byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	return [][]byte{marshal(a.errorFrame(typeInvalidReq, werr.code, werr.message, werr.param, clientEventID))}
}

func (a *Adapter) errorFrame(typ, code, message, param, clientEventID string) errorEvent {
	e := errorEvent{base: a.base("error"), Error: wireError{Type: typ, Code: code, Message: message}}
	if param != "" {
		e.Error.Param = &param
	}
	if clientEventID != "" {
		e.Error.EventID = &clientEventID
	}
	return e
}

func (a *Adapter) dispatch(env envelope) *wireErr {
	meta := session.Meta{Tag: env.eventID}
	switch env.typ {
	case "session.update":
		u, err := decodeSessionUpdate(env)
		if err != nil {
			return err
		}
		if u.model != nil {
			a.mu.Lock()
			a.model = *u.model
			a.mu.Unlock()
		}
		a.post(session.CmdUpdateSession{Meta: meta, Patch: u.patch})
	case "input_audio_buffer.append":
		pcm, err := decodeAudioAppend(env)
		if err != nil {
			return err
		}
		_ = a.sess.PostAudio(pcm) // a full queue closes the session; nothing to report here
	case "input_audio_buffer.commit":
		if err := decodeBare(env); err != nil {
			return err
		}
		a.post(session.CmdCommitAudio{Meta: meta})
	case "input_audio_buffer.clear":
		if err := decodeBare(env); err != nil {
			return err
		}
		a.post(session.CmdClearAudio{Meta: meta})
	case "conversation.item.create":
		ic, err := decodeItemCreate(env)
		if err != nil {
			return err
		}
		cmd := session.CmdCreateItem{Meta: meta, Item: ic.spec}
		a.mu.Lock()
		if ic.spec.ClientID != "" {
			if _, dup := a.itemByID[ic.spec.ClientID]; dup {
				a.mu.Unlock()
				return invalidValue("item.id", fmt.Sprintf("Invalid value for 'item.id': '%s' already exists.", ic.spec.ClientID))
			}
		}
		if ic.hasPrev {
			if ic.previous == "root" {
				cmd.AtRoot = true
			} else {
				ref, ok := a.itemByID[ic.previous]
				if !ok {
					a.mu.Unlock()
					return invalidValue("previous_item_id", fmt.Sprintf("Item '%s' does not exist.", ic.previous))
				}
				cmd.PreviousItem = ref
			}
		}
		a.mu.Unlock()
		a.post(cmd)
	case "conversation.item.delete":
		id, err := decodeItemID(env)
		if err != nil {
			return err
		}
		ref, ok := a.lookupItem(id)
		if !ok {
			return invalidValue("item_id", fmt.Sprintf("Item '%s' does not exist.", id))
		}
		a.post(session.CmdDeleteItem{Meta: meta, ID: ref})
	case "conversation.item.truncate":
		tr, err := decodeItemTruncate(env)
		if err != nil {
			return err
		}
		ref, ok := a.lookupItem(tr.itemID)
		if !ok {
			return invalidValue("item_id", fmt.Sprintf("Item '%s' does not exist.", tr.itemID))
		}
		a.post(session.CmdTruncateItem{Meta: meta, ID: ref, ContentIndex: tr.contentIndex, AudioEndMs: tr.audioEndMs})
	case "conversation.item.retrieve":
		if _, err := decodeItemID(env); err != nil {
			return err
		}
		return invalidEvent("type", "Unsupported event: 'conversation.item.retrieve'. Cascade does not retain item audio.")
	case "response.create":
		ov, err := decodeResponseCreate(env)
		if err != nil {
			return err
		}
		a.post(session.CmdCreateResponse{Meta: meta, Overrides: ov})
	case "response.cancel":
		id, err := decodeResponseCancel(env)
		if err != nil {
			return err
		}
		cmd := session.CmdCancelResponse{Meta: meta}
		if id != "" {
			a.mu.Lock()
			ref, ok := a.respByID[id]
			a.mu.Unlock()
			if !ok {
				return invalidValue("response_id", fmt.Sprintf("Response '%s' does not exist.", id))
			}
			cmd.Resp = ref
		}
		a.post(cmd)
	case "output_audio_buffer.clear", "transcription_session.update":
		return invalidEvent("type", fmt.Sprintf("Unsupported event: '%s'.", env.typ))
	default:
		return invalidValue("type", fmt.Sprintf("Invalid value: '%s'. Supported values are: 'session.update', 'input_audio_buffer.append', 'input_audio_buffer.commit', 'input_audio_buffer.clear', 'conversation.item.create', 'conversation.item.delete', 'conversation.item.truncate', 'response.create', 'response.cancel'.", env.typ))
	}
	return nil
}

func (a *Adapter) post(cmd session.Command) {
	_ = a.sess.Post(cmd) // ErrClosed: the session is going away, nothing to report
}

func (a *Adapter) lookupItem(id string) (session.ItemRef, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	ref, ok := a.itemByID[id]
	return ref, ok
}

// ---- outbound ----------------------------------------------------------------

// Outbound projects one session event onto zero or more server frames.
func (a *Adapter) Outbound(ev session.Event) [][]byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch e := ev.(type) {
	case session.EvSessionUpdated:
		a.cfg = e.Config
		return frames(sessionEvent{base: a.base("session.updated"), Session: sessionObject(a.sessionID, a.model, a.cfg)})
	case session.EvError:
		typ, code := mapErrorCode(e)
		return frames(a.errorFrame(typ, code, e.Message, e.Param, e.Tag))
	case session.EvAudioBufferCommitted:
		st := a.item(e.Item)
		st.prev = a.optID(e.PreviousItem)
		return frames(committedEvent{base: a.base("input_audio_buffer.committed"), PreviousItemID: st.prev, ItemID: st.id})
	case session.EvAudioBufferCleared:
		return frames(struct{ base }{a.base("input_audio_buffer.cleared")})
	case session.EvSpeechStarted:
		return frames(speechStartedEvent{base: a.base("input_audio_buffer.speech_started"), AudioStartMs: e.AudioStartMs, ItemID: a.item(e.Item).id})
	case session.EvSpeechStopped:
		return frames(speechStoppedEvent{base: a.base("input_audio_buffer.speech_stopped"), AudioEndMs: e.AudioEndMs, ItemID: a.item(e.Item).id})
	case session.EvItemAdded:
		st := a.itemFor(e.Item)
		st.prev = a.optID(e.PreviousItem)
		st.audioMs = e.Item.AudioMs
		return frames(itemEvent{base: a.base("conversation.item.added"), PreviousItemID: st.prev, Item: itemObject(st.id, e.Item)})
	case session.EvItemDone:
		st := a.itemFor(e.Item)
		st.audioMs = e.Item.AudioMs
		return frames(itemEvent{base: a.base("conversation.item.done"), PreviousItemID: st.prev, Item: itemObject(st.id, e.Item)})
	case session.EvItemDeleted:
		st := a.item(e.ID)
		delete(a.items, e.ID)
		delete(a.itemByID, st.id)
		return frames(itemIDEvent{base: a.base("conversation.item.deleted"), ItemID: st.id})
	case session.EvItemTruncated:
		return frames(itemTruncatedEvent{base: a.base("conversation.item.truncated"), ItemID: a.item(e.ID).id, ContentIndex: e.ContentIndex, AudioEndMs: e.AudioEndMs})
	case session.EvInputTranscriptDelta:
		return frames(transcriptDeltaEvent{base: a.base("conversation.item.input_audio_transcription.delta"), ItemID: a.item(e.Item).id, Delta: e.Delta})
	case session.EvInputTranscriptDone:
		st := a.item(e.Item)
		return frames(transcriptCompletedEvent{
			base: a.base("conversation.item.input_audio_transcription.completed"), ItemID: st.id, Transcript: e.Text,
			Usage: transcriptUsage{Type: "duration", Seconds: float64(st.audioMs) / 1000},
		})
	case session.EvResponseCreated:
		st := &respState{id: newID("resp_"), modal: e.OutputModalities, voice: e.Voice, maxTokens: e.MaxOutputTokens, metadata: e.Metadata}
		st.textOnly = len(e.OutputModalities) == 1 && e.OutputModalities[0] == config.ModalityText
		a.resps[e.Resp] = st
		a.respByID[st.id] = e.Resp
		r := a.responseObject(st, session.ResponseInProgress, nil, nil, nil)
		return frames(responseEvent{base: a.base("response.created"), Response: r})
	case session.EvOutputItemAdded:
		rs := a.resps[e.Resp]
		st := a.itemFor(e.Item)
		st.prev = a.optID(e.PreviousItem)
		rs.itemID = st.id
		item := itemObject(st.id, e.Item)
		part := wireContentPart{Type: "audio", Transcript: ptr("")}
		if rs.textOnly {
			part = wireContentPart{Type: "text", Text: ptr("")}
		}
		return frames(
			outputItemEvent{base: a.base("response.output_item.added"), ResponseID: rs.id, Item: item},
			itemEvent{base: a.base("conversation.item.added"), PreviousItemID: st.prev, Item: item},
			contentPartEvent{base: a.base("response.content_part.added"), ResponseID: rs.id, ItemID: st.id, Part: part},
		)
	case session.EvOutputTextDelta:
		return frames(a.delta("response.output_text.delta", e.Resp, e.Item, e.Delta))
	case session.EvOutputAudioTranscriptDelta:
		return frames(a.delta("response.output_audio_transcript.delta", e.Resp, e.Item, e.Delta))
	case session.EvOutputAudioDelta:
		return frames(a.delta("response.output_audio.delta", e.Resp, e.Item, base64.StdEncoding.EncodeToString(e.PCM)))
	case session.EvOutputTextDone:
		return frames(outputDoneEvent{base: a.base("response.output_text.done"), ResponseID: a.resps[e.Resp].id, ItemID: a.item(e.Item).id, Text: ptr(e.Text)})
	case session.EvOutputAudioTranscriptDone:
		return frames(outputDoneEvent{base: a.base("response.output_audio_transcript.done"), ResponseID: a.resps[e.Resp].id, ItemID: a.item(e.Item).id, Transcript: ptr(e.Text)})
	case session.EvOutputAudioDone:
		return frames(outputDoneEvent{base: a.base("response.output_audio.done"), ResponseID: a.resps[e.Resp].id, ItemID: a.item(e.Item).id})
	case session.EvOutputItemDone:
		rs := a.resps[e.Resp]
		st := a.itemFor(e.Item)
		item := itemObject(st.id, e.Item)
		part := wireContentPart{Type: "audio", Transcript: ptr(e.Item.Text)}
		if rs.textOnly {
			part = wireContentPart{Type: "text", Text: ptr(e.Item.Text)}
		}
		return frames(
			contentPartEvent{base: a.base("response.content_part.done"), ResponseID: rs.id, ItemID: st.id, Part: part},
			outputItemEvent{base: a.base("response.output_item.done"), ResponseID: rs.id, Item: item},
			itemEvent{base: a.base("conversation.item.done"), PreviousItemID: st.prev, Item: item},
		)
	case session.EvResponseDone:
		rs := a.resps[e.Resp]
		var output []wireItem
		for _, it := range e.Output {
			output = append(output, itemObject(a.itemFor(it).id, it))
		}
		usage := wireUsage{TotalTokens: e.Usage.InputTokens + e.Usage.OutputTokens, InputTokens: e.Usage.InputTokens, OutputTokens: e.Usage.OutputTokens}
		usage.InputTokenDetails.TextTokens = e.Usage.InputTokens
		usage.OutputTokenDetails.TextTokens = e.Usage.OutputTokens
		r := a.responseObject(rs, e.Status, statusDetails(e), output, &usage)
		return frames(responseEvent{base: a.base("response.done"), Response: r})
	case session.EvSessionClosed:
		return nil
	}
	return nil
}

func frames(events ...any) [][]byte {
	out := make([][]byte, 0, len(events))
	for _, ev := range events {
		out = append(out, marshal(ev))
	}
	return out
}

func ptr[T any](v T) *T { return &v }

func (a *Adapter) delta(typ string, resp session.ResponseRef, item session.ItemRef, delta string) outputDeltaEvent {
	return outputDeltaEvent{base: a.base(typ), ResponseID: a.resps[resp].id, ItemID: a.item(item).id, Delta: delta}
}

// item returns the state for ref, allocating a server id on first sight.
func (a *Adapter) item(ref session.ItemRef) *itemState {
	if st, ok := a.items[ref]; ok {
		return st
	}
	st := &itemState{id: newID("item_")}
	a.items[ref] = st
	a.itemByID[st.id] = ref
	return st
}

// itemFor is item() honoring a client-supplied id.
func (a *Adapter) itemFor(it session.Item) *itemState {
	if st, ok := a.items[it.Ref]; ok {
		return st
	}
	if it.ClientID != "" {
		st := &itemState{id: it.ClientID}
		a.items[it.Ref] = st
		a.itemByID[st.id] = it.Ref
		return st
	}
	return a.item(it.Ref)
}

func (a *Adapter) optID(ref session.ItemRef) *string {
	if ref == 0 {
		return nil
	}
	id := a.item(ref).id
	return &id
}

func (a *Adapter) responseObject(rs *respState, status session.ResponseStatus, details *wireStatusDetails, output []wireItem, usage *wireUsage) wireResponse {
	if output == nil {
		output = []wireItem{}
	}
	r := wireResponse{
		ID: rs.id, Object: objectResponse, Status: string(status), StatusDetails: details, Output: output, Usage: usage,
		ConversationID: a.convID, OutputModalities: append([]string{}, rs.modal...),
		MaxOutputTokens: rs.maxTokens, Metadata: rs.metadata,
	}
	r.Audio.Output.Format = pcmFormat()
	r.Audio.Output.Voice = rs.voice
	return r
}

func statusDetails(e session.EvResponseDone) *wireStatusDetails {
	switch e.Status {
	case session.ResponseCancelled, session.ResponseIncomplete:
		return &wireStatusDetails{Type: string(e.Status), Reason: string(e.Reason)}
	case session.ResponseFailed:
		return &wireStatusDetails{Type: string(e.Status), Error: &wireStatusErr{Type: typeServerErr, Code: e.ErrCode}}
	}
	return nil
}

// mapErrorCode projects session error codes onto profile §8.
func mapErrorCode(e session.EvError) (typ, code string) {
	switch e.Code {
	case session.ErrCodeInvalidSession, session.ErrCodeInvalidItem, session.ErrCodeItemNotFound,
		session.ErrCodeItemNotTruncatable, session.ErrCodeInvalidOverrides,
		session.ErrCodeTruncateOutOfRange, session.ErrCodeVoiceLocked:
		return typeInvalidReq, codeInvalidValue
	case session.ErrCodeBufferEmpty, session.ErrCodeResponseInProgress:
		return typeInvalidReq, codeInvalidEvent
	case session.ErrCodeResponseCancelNotActive:
		if e.Param != "" {
			return typeInvalidReq, codeInvalidValue
		}
		return typeInvalidReq, codeInvalidEvent
	}
	return typeServerErr, e.Code
}
