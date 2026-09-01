package protocol

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/rasonyang/cascade-realtime-gateway/internal/audio"
	"github.com/rasonyang/cascade-realtime-gateway/internal/config"
	"github.com/rasonyang/cascade-realtime-gateway/internal/session"
)

// The only two protocol-validation codes (profile §8).
const (
	codeInvalidEvent = "invalid_event"
	codeInvalidValue = "invalid_value"
	typeInvalidReq   = "invalid_request_error"
	typeServerErr    = "server_error"
)

// wireErr is a rejected client event: profile §8 code, offending param and
// a human-readable message.
type wireErr struct {
	code    string
	param   string
	message string
}

func (e *wireErr) Error() string { return e.message }

func invalidEvent(param, msg string) *wireErr {
	return &wireErr{code: codeInvalidEvent, param: param, message: msg}
}

func invalidValue(param, msg string) *wireErr {
	return &wireErr{code: codeInvalidValue, param: param, message: msg}
}

func unknownParam(path string) *wireErr {
	return invalidValue(path, fmt.Sprintf("Unknown parameter: '%s'.", path))
}

func unsupportedParam(path string) *wireErr {
	return invalidValue(path, fmt.Sprintf("Unsupported parameter: '%s'. Cascade does not implement it.", path))
}

func missingParam(path string) *wireErr {
	return invalidValue(path, fmt.Sprintf("Missing required parameter: '%s'.", path))
}

func wrongType(path, want string) *wireErr {
	return invalidValue(path, fmt.Sprintf("Invalid type for '%s': expected %s.", path, want))
}

func unsupportedValue(path string, got any, supported string) *wireErr {
	return invalidValue(path, fmt.Sprintf("Unsupported value: '%v' for '%s'. Supported values are: %s.", got, path, supported))
}

// betaFields are top-level beta session fields; they get a pointed message.
var betaFields = map[string]bool{
	"modalities": true, "voice": true, "turn_detection": true, "input_audio_format": true,
	"output_audio_format": true, "input_audio_transcription": true, "temperature": true,
	"max_response_output_tokens": true, "input_audio_noise_reduction": true,
}

// obj is a JSON object being validated, with its dotted path for errors.
type obj struct {
	path string
	m    map[string]json.RawMessage
}

func parseObject(raw json.RawMessage, path string) (obj, *wireErr) {
	if isNull(raw) {
		return obj{}, wrongType(path, "object")
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return obj{}, wrongType(path, "object")
	}
	return obj{path: path, m: m}, nil
}

func isNull(raw json.RawMessage) bool { return strings.TrimSpace(string(raw)) == "null" }

func (o obj) pathOf(key string) string {
	if o.path == "" {
		return key
	}
	return o.path + "." + key
}

// allow rejects any key outside the given set.
func (o obj) allow(keys ...string) *wireErr {
	set := map[string]bool{}
	for _, k := range keys {
		set[k] = true
	}
	for k := range o.m {
		if !set[k] {
			return unknownParam(o.pathOf(k))
		}
	}
	return nil
}

func (o obj) has(key string) bool { _, ok := o.m[key]; return ok }

func (o obj) null(key string) bool {
	raw, ok := o.m[key]
	return ok && isNull(raw)
}

func (o obj) str(key string) (string, bool, *wireErr) {
	raw, ok := o.m[key]
	if !ok {
		return "", false, nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", true, wrongType(o.pathOf(key), "string")
	}
	return s, true, nil
}

func (o obj) float(key string) (float64, bool, *wireErr) {
	raw, ok := o.m[key]
	if !ok {
		return 0, false, nil
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err != nil {
		return 0, true, wrongType(o.pathOf(key), "number")
	}
	return f, true, nil
}

func (o obj) integer(key string) (int, bool, *wireErr) {
	f, ok, err := o.float(key)
	if err != nil || !ok {
		return 0, ok, err
	}
	if f != math.Trunc(f) {
		return 0, true, wrongType(o.pathOf(key), "integer")
	}
	return int(f), true, nil
}

func (o obj) boolean(key string) (bool, bool, *wireErr) {
	raw, ok := o.m[key]
	if !ok {
		return false, false, nil
	}
	var b bool
	if err := json.Unmarshal(raw, &b); err != nil {
		return false, true, wrongType(o.pathOf(key), "boolean")
	}
	return b, true, nil
}

func (o obj) object(key string) (obj, bool, *wireErr) {
	raw, ok := o.m[key]
	if !ok {
		return obj{}, false, nil
	}
	child, err := parseObject(raw, o.pathOf(key))
	return child, true, err
}

func (o obj) array(key string) ([]json.RawMessage, bool, *wireErr) {
	raw, ok := o.m[key]
	if !ok {
		return nil, false, nil
	}
	var arr []json.RawMessage
	if isNull(raw) {
		return nil, true, wrongType(o.pathOf(key), "array")
	}
	if err := json.Unmarshal(raw, &arr); err != nil {
		return nil, true, wrongType(o.pathOf(key), "array")
	}
	return arr, true, nil
}

func (o obj) strings(key string) ([]string, bool, *wireErr) {
	arr, ok, err := o.array(key)
	if err != nil || !ok {
		return nil, ok, err
	}
	out := make([]string, 0, len(arr))
	for i, raw := range arr {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, true, wrongType(fmt.Sprintf("%s[%d]", o.pathOf(key), i), "string")
		}
		out = append(out, s)
	}
	return out, true, nil
}

// ---- envelope ---------------------------------------------------------------

type envelope struct {
	typ     string
	eventID string
	body    obj
}

func decodeEnvelope(data []byte) (envelope, *wireErr) {
	body, err := parseObject(data, "")
	if err != nil {
		return envelope{}, invalidEvent("", "Invalid JSON: the client event must be a JSON object.")
	}
	env := envelope{body: body}
	if body.has("event_id") {
		id, _, err := body.str("event_id")
		if err != nil {
			return env, err
		}
		if len(id) > eventIDMaxLen {
			return env, invalidValue("event_id", fmt.Sprintf("Invalid 'event_id': must be at most %d characters.", eventIDMaxLen))
		}
		env.eventID = id
	}
	if !body.has("type") {
		return env, invalidEvent("", "The 'type' field is missing.")
	}
	typ, _, err := body.str("type")
	if err != nil {
		return env, invalidEvent("type", "Invalid type for 'type': expected string.")
	}
	env.typ = typ
	return env, nil
}

// ---- session.update ---------------------------------------------------------

type sessionUpdate struct {
	patch session.SessionPatch
	model *string
}

func decodeSessionUpdate(env envelope) (sessionUpdate, *wireErr) {
	var out sessionUpdate
	if err := env.body.allow("type", "event_id", "session"); err != nil {
		return out, err
	}
	sess, ok, err := env.body.object("session")
	if err != nil {
		return out, err
	}
	if !ok {
		return out, missingParam("session")
	}
	for k := range sess.m {
		if betaFields[k] {
			return out, invalidValue(sess.pathOf(k), fmt.Sprintf(
				"Unknown parameter: '%s'. This is a beta session field; the GA session shape is required (see 'output_modalities', 'audio.input', 'audio.output').", sess.pathOf(k)))
		}
	}
	if err := sess.allow("type", "model", "instructions", "output_modalities", "audio", "max_output_tokens",
		"tools", "tool_choice", "parallel_tool_calls", "reasoning", "prompt", "tracing", "truncation", "include"); err != nil {
		return out, err
	}
	typ, ok, err := sess.str("type")
	if err != nil {
		return out, err
	}
	if !ok {
		return out, missingParam(sess.pathOf("type"))
	}
	if typ != sessionTypeGA {
		return out, unsupportedValue(sess.pathOf("type"), typ, "['realtime']")
	}
	if m, ok, err := sess.str("model"); err != nil {
		return out, err
	} else if ok {
		out.model = &m
	}
	if v, ok, err := sess.str("instructions"); err != nil {
		return out, err
	} else if ok {
		out.patch.Instructions = &v
	}
	if v, ok, err := sess.strings("output_modalities"); err != nil {
		return out, err
	} else if ok {
		out.patch.OutputModalities = v
	}
	if sess.has("max_output_tokens") {
		v, err := decodeMaxTokens(sess, "max_output_tokens")
		if err != nil {
			return out, err
		}
		out.patch.MaxOutputTokens = &v
	}
	if aud, ok, err := sess.object("audio"); err != nil {
		return out, err
	} else if ok {
		if err := decodeAudio(aud, &out.patch); err != nil {
			return out, err
		}
	}
	if err := checkDefaultedFields(sess); err != nil {
		return out, err
	}
	return out, nil
}

// checkDefaultedFields accepts the profile §3 "ignored" values only.
func checkDefaultedFields(sess obj) *wireErr {
	if arr, ok, err := sess.array("tools"); err != nil {
		return err
	} else if ok && len(arr) != 0 {
		return unsupportedParam(sess.pathOf("tools"))
	}
	if v, ok, err := sess.str("tool_choice"); err != nil {
		return unsupportedParam(sess.pathOf("tool_choice"))
	} else if ok && v != "auto" && v != "none" {
		return unsupportedValue(sess.pathOf("tool_choice"), v, "['auto', 'none']")
	}
	if v, ok, err := sess.str("truncation"); err != nil {
		return unsupportedParam(sess.pathOf("truncation"))
	} else if ok && v != "auto" {
		return unsupportedValue(sess.pathOf("truncation"), v, "['auto']")
	}
	for _, k := range []string{"prompt", "tracing"} {
		if sess.has(k) && !sess.null(k) {
			return unsupportedParam(sess.pathOf(k))
		}
	}
	if sess.has("include") && !sess.null("include") {
		if arr, _, err := sess.array("include"); err != nil || len(arr) != 0 {
			return unsupportedParam(sess.pathOf("include"))
		}
	}
	for _, k := range []string{"parallel_tool_calls", "reasoning"} {
		if sess.has(k) {
			return unsupportedParam(sess.pathOf(k))
		}
	}
	return nil
}

func decodeMaxTokens(o obj, key string) (config.MaxOutputTokens, *wireErr) {
	raw := o.m[key]
	var v config.MaxOutputTokens
	if err := json.Unmarshal(raw, &v); err != nil {
		return v, invalidValue(o.pathOf(key), fmt.Sprintf("Invalid value for '%s': expected 'inf' or an integer.", o.pathOf(key)))
	}
	return v, nil
}

func decodeFormat(o obj) *wireErr {
	if err := o.allow("type", "rate"); err != nil {
		return err
	}
	if t, ok, err := o.str("type"); err != nil {
		return err
	} else if ok && t != config.AudioFormatPCM {
		return unsupportedValue(o.pathOf("type"), t, "['audio/pcm']")
	}
	if r, ok, err := o.integer("rate"); err != nil {
		return err
	} else if ok && r != config.AudioSampleRate {
		return unsupportedValue(o.pathOf("rate"), r, "[24000]")
	}
	return nil
}

func decodeAudio(aud obj, patch *session.SessionPatch) *wireErr {
	if err := aud.allow("input", "output"); err != nil {
		return err
	}
	if in, ok, err := aud.object("input"); err != nil {
		return err
	} else if ok {
		if err := in.allow("format", "transcription", "turn_detection", "noise_reduction"); err != nil {
			return err
		}
		if f, ok, err := in.object("format"); err != nil {
			return err
		} else if ok {
			if err := decodeFormat(f); err != nil {
				return err
			}
		}
		if in.has("transcription") {
			if in.null("transcription") {
				patch.Transcription = session.Nullable[config.Transcription]{Set: true}
			} else {
				tr, _, err := in.object("transcription")
				if err != nil {
					return err
				}
				if err := tr.allow("model", "language", "prompt"); err != nil {
					return err
				}
				var t config.Transcription
				for key, dst := range map[string]*string{"model": &t.Model, "language": &t.Language, "prompt": &t.Prompt} {
					if v, _, err := tr.str(key); err != nil {
						return err
					} else {
						*dst = v
					}
				}
				patch.Transcription = session.Nullable[config.Transcription]{Set: true, Value: &t}
			}
		}
		if in.has("turn_detection") {
			if in.null("turn_detection") {
				patch.TurnDetection = session.Nullable[config.TurnDetection]{Set: true}
			} else {
				td, _, err := in.object("turn_detection")
				if err != nil {
					return err
				}
				v, err := decodeTurnDetection(td)
				if err != nil {
					return err
				}
				patch.TurnDetection = session.Nullable[config.TurnDetection]{Set: true, Value: v}
			}
		}
		if in.has("noise_reduction") && !in.null("noise_reduction") {
			return unsupportedParam(in.pathOf("noise_reduction"))
		}
	}
	if out, ok, err := aud.object("output"); err != nil {
		return err
	} else if ok {
		if err := out.allow("format", "voice", "speed"); err != nil {
			return err
		}
		if f, ok, err := out.object("format"); err != nil {
			return err
		} else if ok {
			if err := decodeFormat(f); err != nil {
				return err
			}
		}
		if out.has("voice") {
			v, _, err := out.str("voice")
			if err != nil {
				return invalidValue(out.pathOf("voice"), "Unsupported value for 'voice': custom voice objects are not supported; provide a voice name string.")
			}
			patch.Voice = &v
		}
		if v, ok, err := out.float("speed"); err != nil {
			return err
		} else if ok {
			patch.Speed = &v
		}
	}
	return nil
}

func decodeTurnDetection(td obj) (*config.TurnDetection, *wireErr) {
	typ, ok, err := td.str("type")
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, missingParam(td.pathOf("type"))
	}
	out := &config.TurnDetection{Type: typ, CreateResponse: true, InterruptResponse: true}
	switch typ {
	case config.TurnDetectionServerVAD:
		if err := td.allow("type", "threshold", "prefix_padding_ms", "silence_duration_ms", "create_response", "interrupt_response", "idle_timeout_ms"); err != nil {
			return nil, err
		}
		if td.has("idle_timeout_ms") && !td.null("idle_timeout_ms") {
			return nil, unsupportedParam(td.pathOf("idle_timeout_ms"))
		}
		if v, ok, err := td.float("threshold"); err != nil {
			return nil, err
		} else if ok {
			out.Threshold = &v
		}
		if v, ok, err := td.integer("prefix_padding_ms"); err != nil {
			return nil, err
		} else if ok {
			out.PrefixPaddingMs = &v
		}
		if v, ok, err := td.integer("silence_duration_ms"); err != nil {
			return nil, err
		} else if ok {
			out.SilenceDurationMs = &v
		}
	case config.TurnDetectionSemanticVAD:
		if err := td.allow("type", "eagerness", "create_response", "interrupt_response"); err != nil {
			return nil, err
		}
		if v, ok, err := td.str("eagerness"); err != nil {
			return nil, err
		} else if ok {
			out.Eagerness = &v
		}
	default:
		return nil, unsupportedValue(td.pathOf("type"), typ, "['server_vad', 'semantic_vad']")
	}
	if v, ok, err := td.boolean("create_response"); err != nil {
		return nil, err
	} else if ok {
		out.CreateResponse = v
	}
	if v, ok, err := td.boolean("interrupt_response"); err != nil {
		return nil, err
	} else if ok {
		out.InterruptResponse = v
	}
	return out, nil
}

// ---- input_audio_buffer.* ---------------------------------------------------

func decodeAudioAppend(env envelope) ([]byte, *wireErr) {
	if err := env.body.allow("type", "event_id", "audio"); err != nil {
		return nil, err
	}
	s, ok, err := env.body.str("audio")
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, missingParam("audio")
	}
	pcm, derr := base64.StdEncoding.DecodeString(s)
	if derr != nil {
		return nil, invalidValue("audio", "Invalid value for 'audio': expected base64-encoded audio.")
	}
	if len(pcm)%audio.BytesPerSample != 0 {
		return nil, invalidValue("audio", "Invalid value for 'audio': audio/pcm requires whole 16-bit samples.")
	}
	return pcm, nil
}

func decodeBare(env envelope) *wireErr {
	return env.body.allow("type", "event_id")
}

// ---- conversation.item.* ----------------------------------------------------

type itemCreate struct {
	spec     session.ItemSpec
	previous string
	hasPrev  bool
}

func decodeItemCreate(env envelope) (itemCreate, *wireErr) {
	var out itemCreate
	if err := env.body.allow("type", "event_id", "item", "previous_item_id"); err != nil {
		return out, err
	}
	if p, ok, err := env.body.str("previous_item_id"); err != nil {
		return out, err
	} else if ok {
		out.previous, out.hasPrev = p, true
	}
	it, ok, err := env.body.object("item")
	if err != nil {
		return out, err
	}
	if !ok {
		return out, missingParam("item")
	}
	if err := it.allow("id", "object", "type", "role", "status", "content", "call_id", "name", "arguments", "output"); err != nil {
		return out, err
	}
	typ, ok, err := it.str("type")
	if err != nil {
		return out, err
	}
	if !ok {
		return out, missingParam(it.pathOf("type"))
	}
	if typ != "message" {
		return out, unsupportedValue(it.pathOf("type"), typ, "['message']")
	}
	for _, k := range []string{"call_id", "name", "arguments", "output"} {
		if it.has(k) {
			return out, unknownParam(it.pathOf(k))
		}
	}
	if id, ok, err := it.str("id"); err != nil {
		return out, err
	} else if ok {
		if id == "" {
			return out, invalidValue(it.pathOf("id"), "Invalid value for 'item.id': must not be empty.")
		}
		out.spec.ClientID = id
	}
	role, ok, err := it.str("role")
	if err != nil {
		return out, err
	}
	if !ok {
		return out, missingParam(it.pathOf("role"))
	}
	if role != "user" && role != "assistant" && role != "system" {
		return out, unsupportedValue(it.pathOf("role"), role, "['user', 'assistant', 'system']")
	}
	out.spec.Role = session.Role(role)
	content, ok, err := it.array("content")
	if err != nil {
		return out, err
	}
	if !ok || len(content) == 0 {
		return out, missingParam(it.pathOf("content"))
	}
	if len(content) > 1 {
		return out, invalidValue(it.pathOf("content"), "Unsupported value for 'item.content': exactly one content part is supported.")
	}
	part, perr := parseObject(content[0], it.pathOf("content")+"[0]")
	if perr != nil {
		return out, perr
	}
	if err := part.allow("type", "text", "audio", "transcript", "image_url", "detail", "id"); err != nil {
		return out, err
	}
	ptype, ok, err := part.str("type")
	if err != nil {
		return out, err
	}
	if !ok {
		return out, missingParam(part.pathOf("type"))
	}
	allowed := map[string]string{"user": "['input_text']", "system": "['input_text']", "assistant": "['output_text', 'text']"}[role]
	switch {
	case role == "assistant" && (ptype == "output_text" || ptype == "text"):
	case role != "assistant" && ptype == "input_text":
	default:
		return out, unsupportedValue(part.pathOf("type"), ptype, allowed)
	}
	for _, k := range []string{"audio", "transcript", "image_url", "detail", "id"} {
		if part.has(k) {
			return out, unknownParam(part.pathOf(k))
		}
	}
	text, ok, err := part.str("text")
	if err != nil {
		return out, err
	}
	if !ok {
		return out, missingParam(part.pathOf("text"))
	}
	out.spec.Text = text
	return out, nil
}

func decodeItemID(env envelope) (string, *wireErr) {
	if err := env.body.allow("type", "event_id", "item_id"); err != nil {
		return "", err
	}
	id, ok, err := env.body.str("item_id")
	if err != nil {
		return "", err
	}
	if !ok {
		return "", missingParam("item_id")
	}
	return id, nil
}

type itemTruncate struct {
	itemID       string
	contentIndex int
	audioEndMs   int
}

func decodeItemTruncate(env envelope) (itemTruncate, *wireErr) {
	var out itemTruncate
	if err := env.body.allow("type", "event_id", "item_id", "content_index", "audio_end_ms"); err != nil {
		return out, err
	}
	id, ok, err := env.body.str("item_id")
	if err != nil {
		return out, err
	}
	if !ok {
		return out, missingParam("item_id")
	}
	out.itemID = id
	ci, ok, err := env.body.integer("content_index")
	if err != nil {
		return out, err
	}
	if !ok {
		return out, missingParam("content_index")
	}
	out.contentIndex = ci
	ms, ok, err := env.body.integer("audio_end_ms")
	if err != nil {
		return out, err
	}
	if !ok {
		return out, missingParam("audio_end_ms")
	}
	out.audioEndMs = ms
	return out, nil
}

// ---- response.* -------------------------------------------------------------

func decodeResponseCreate(env envelope) (session.ResponseOverrides, *wireErr) {
	var ov session.ResponseOverrides
	if err := env.body.allow("type", "event_id", "response"); err != nil {
		return ov, err
	}
	r, ok, err := env.body.object("response")
	if err != nil {
		return ov, err
	}
	if !ok {
		return ov, nil
	}
	if err := r.allow("instructions", "output_modalities", "max_output_tokens", "audio", "conversation", "input",
		"metadata", "tools", "tool_choice", "parallel_tool_calls", "prompt", "reasoning"); err != nil {
		return ov, err
	}
	for _, k := range []string{"input", "tools", "tool_choice", "parallel_tool_calls", "prompt", "reasoning"} {
		if r.has(k) {
			return ov, unsupportedParam(r.pathOf(k))
		}
	}
	if v, ok, err := r.str("instructions"); err != nil {
		return ov, err
	} else if ok {
		ov.Instructions = &v
	}
	if v, ok, err := r.strings("output_modalities"); err != nil {
		return ov, err
	} else if ok {
		ov.OutputModalities = v
	}
	if r.has("max_output_tokens") {
		v, err := decodeMaxTokens(r, "max_output_tokens")
		if err != nil {
			return ov, err
		}
		ov.MaxOutputTokens = &v
	}
	if v, ok, err := r.str("conversation"); err != nil {
		return ov, err
	} else if ok && v != "auto" {
		return ov, unsupportedValue(r.pathOf("conversation"), v, "['auto']")
	}
	if aud, ok, err := r.object("audio"); err != nil {
		return ov, err
	} else if ok {
		if err := aud.allow("output"); err != nil {
			return ov, err
		}
		if out, ok, err := aud.object("output"); err != nil {
			return ov, err
		} else if ok {
			if err := out.allow("format", "voice"); err != nil {
				return ov, err
			}
			if f, ok, err := out.object("format"); err != nil {
				return ov, err
			} else if ok {
				if err := decodeFormat(f); err != nil {
					return ov, err
				}
			}
			if out.has("voice") {
				v, _, err := out.str("voice")
				if err != nil {
					return ov, invalidValue(out.pathOf("voice"), "Unsupported value for 'voice': custom voice objects are not supported; provide a voice name string.")
				}
				ov.Voice = &v
			}
		}
	}
	if r.has("metadata") && !r.null("metadata") {
		md, _, err := r.object("metadata")
		if err != nil {
			return ov, err
		}
		if len(md.m) > metadataMaxPairs {
			return ov, invalidValue(md.path, fmt.Sprintf("Invalid value for 'metadata': at most %d key-value pairs.", metadataMaxPairs))
		}
		ov.Metadata = map[string]string{}
		for k := range md.m {
			v, _, err := md.str(k)
			if err != nil {
				return ov, err
			}
			if len(k) > metadataKeyMaxLen || len(v) > metadataValMaxLen {
				return ov, invalidValue(md.pathOf(k), fmt.Sprintf("Invalid value for 'metadata': keys ≤ %d and values ≤ %d characters.", metadataKeyMaxLen, metadataValMaxLen))
			}
			ov.Metadata[k] = v
		}
	}
	return ov, nil
}

func decodeResponseCancel(env envelope) (string, *wireErr) {
	if err := env.body.allow("type", "event_id", "response_id"); err != nil {
		return "", err
	}
	id, _, err := env.body.str("response_id")
	if err != nil {
		return "", err
	}
	return id, nil
}
