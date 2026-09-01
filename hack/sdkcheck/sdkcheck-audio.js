// Audio-mode turn through the official SDK, then a cancelled second turn.
const { OpenAI } = require('openai');
const { OpenAIRealtimeWS } = require('openai/realtime/ws');
const client = new OpenAI({ apiKey: process.env.REALTIME_API_KEY, baseURL: 'https://127.0.0.1:18443/v1' });
const rt = new OpenAIRealtimeWS({ model: 'gpt-realtime', options: { rejectUnauthorized: false } }, client);
let turn = 0, audio = 0;
const timer = setTimeout(() => { console.error('TIMEOUT'); process.exit(2); }, 10000);
rt.on('error', (e) => { console.error('ERROR', e.message); process.exit(1); });
rt.on('session.created', () => {
  rt.send({ type: 'conversation.item.create', item: { type: 'message', role: 'user', content: [{ type: 'input_text', text: 'Hi' }] } });
  rt.send({ type: 'response.create' });
});
rt.on('response.output_audio.delta', (ev) => { audio += Buffer.from(ev.delta, 'base64').length; if (turn === 1) rt.send({ type: 'response.cancel' }); });
rt.on('response.done', (ev) => {
  turn++;
  console.log(`response ${turn}: status=${ev.response.status} audio_bytes=${audio}`);
  audio = 0;
  if (turn === 1) { rt.send({ type: 'response.create' }); return; }
  clearTimeout(timer); rt.close();
});
rt.socket.on('close', () => process.exit(0));
