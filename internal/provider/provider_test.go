package provider

import (
	"encoding/json"
	"errors"
	"testing"
)

type fake struct{ kind Kind }

func (f fake) Kind() Kind { return f.kind }

func TestRegistry(t *testing.T) {
	registry = map[Kind]map[string]Factory{} // isolate from -count=N reruns
	Register(KindLLM, "fake", func(apiKey string, opts json.RawMessage) (Provider, error) {
		if apiKey != "k" || string(opts) != `{"a":1}` {
			t.Fatalf("factory args = %q %s", apiKey, opts)
		}
		return fake{KindLLM}, nil
	})
	Register(KindTTS, "wrongkind", func(string, json.RawMessage) (Provider, error) { return fake{KindLLM}, nil })

	if !Known("llm", "fake") || Known("asr", "fake") || Known("llm", "nope") {
		t.Fatal("Known wrong")
	}
	p, err := New(KindLLM, "fake", "k", json.RawMessage(`{"a":1}`))
	if err != nil || p.Kind() != KindLLM {
		t.Fatalf("New: %v %v", p, err)
	}
	if _, err := New(KindLLM, "nope", "", nil); err == nil {
		t.Fatal("unknown provider must error")
	}
	if _, err := New(KindTTS, "wrongkind", "", nil); err == nil {
		t.Fatal("kind mismatch must error")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("duplicate registration must panic")
		}
	}()
	Register(KindLLM, "fake", nil)
}

func TestErrorWrapping(t *testing.T) {
	inner := errors.New("boom")
	err := &Error{Provider: "x", Kind: ErrTransient, Err: inner}
	if !errors.Is(err, inner) || err.Error() != "x: transient: boom" {
		t.Fatalf("got %q", err)
	}
}
