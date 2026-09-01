# sdkcheck — manual verification with the official OpenAI SDK

Drives a locally running Cascade with `openai-node`'s GA realtime client
(`OpenAIRealtimeWS`, no beta header). The SDK only speaks `wss:`, so a
self-signed TLS terminator sits in front of Cascade's plain `ws://`.

```sh
# from the repository root
go build -o hack/sdkcheck/cascade ./cmd/cascade
go build -o hack/sdkcheck/tlsproxy-bin ./hack/sdkcheck/tlsproxy
cd hack/sdkcheck && npm ci

REALTIME_API_KEY=sk-test ./cascade -config config.mock.json &   # mock providers on 127.0.0.1:18080
./tlsproxy-bin &                                                  # wss://127.0.0.1:18443 → ws://127.0.0.1:18080
REALTIME_API_KEY=sk-test npm run text     # text-only conversation
REALTIME_API_KEY=sk-test npm run audio    # audio response + cancel
```

Expected: `session.created` → … → `response.done status=completed`, socket closed with code 1000.
Only sources and configuration are committed; binaries, `node_modules/`, logs and pids are ignored.
