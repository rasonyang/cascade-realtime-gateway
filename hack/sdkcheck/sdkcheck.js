// Manual Phase 2/3 acceptance: text-only conversation through the official
// openai-node GA realtime client (OpenAIRealtimeWS), no beta header.
const { OpenAI } = require('openai');
const { OpenAIRealtimeWS } = require('openai/realtime/ws');

const client = new OpenAI({ apiKey: process.env.REALTIME_API_KEY, baseURL: 'https://127.0.0.1:18443/v1' });
const rt = new OpenAIRealtimeWS({ model: 'gpt-realtime', options: { rejectUnauthorized: false } }, client);
const seen = [];
let text = '';
const timer = setTimeout(() => { console.error('TIMEOUT', seen); process.exit(2); }, 5000);

rt.on('error', (e) => { console.error('ERROR', e.message, e.event ?? ''); process.exit(1); });
rt.on('event', (ev) => seen.push(ev.type));
rt.on('session.created', (ev) => {
  console.log('session.created id=%s model=%s type=%s', ev.session.id, ev.session.model, ev.session.type);
  rt.send({ type: 'session.update', session: { type: 'realtime', output_modalities: ['text'], instructions: 'Be terse.' } });
});
rt.on('session.updated', (ev) => {
  console.log('session.updated output_modalities=%j instructions=%j', ev.session.output_modalities, ev.session.instructions);
  rt.send({ type: 'conversation.item.create', item: { type: 'message', role: 'user', content: [{ type: 'input_text', text: 'Hi Cascade' }] } });
  rt.send({ type: 'response.create' });
});
rt.on('response.output_text.delta', (ev) => { text += ev.delta; });
rt.on('response.done', (ev) => {
  console.log('response.done status=%s text=%j usage=%j', ev.response.status, text, ev.response.usage);
  console.log('server events:', seen.join(' → '));
  clearTimeout(timer);
  rt.close();
});
rt.socket.on('close', (code, reason) => { console.log('socket closed code=%d reason=%s', code, reason); process.exit(text ? 0 : 3); });
