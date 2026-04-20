/**
 * Voice Preview Route — Gemini Native TTS
 * POST /api/voice-preview
 * Body: { voice: "Kore", text: "Assalam o Alaikum..." }
 * Returns: { audio: "<base64>", mimeType: "audio/mp3", model: "..." }
 */

import { Router } from 'express';

const router = Router();

/**
 * Create a WAV header for raw PCM data
 */
function createWavFromPcm(pcmBase64, sampleRate = 24000) {
  const pcmBuffer = Buffer.from(pcmBase64, 'base64');
  const numChannels = 1;
  const bitsPerSample = 16;
  const byteRate = sampleRate * numChannels * (bitsPerSample / 8);
  const blockAlign = numChannels * (bitsPerSample / 8);
  const dataSize = pcmBuffer.length;

  const header = Buffer.alloc(44);
  header.write('RIFF', 0);
  header.writeUInt32LE(36 + dataSize, 4);
  header.write('WAVE', 8);
  header.write('fmt ', 12);
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20); // PCM format
  header.writeUInt16LE(numChannels, 22);
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(byteRate, 28);
  header.writeUInt16LE(blockAlign, 32);
  header.writeUInt16LE(bitsPerSample, 34);
  header.write('data', 36);
  header.writeUInt32LE(dataSize, 40);

  return Buffer.concat([header, pcmBuffer]);
}

router.post('/voice-preview', async (req, res) => {
  try {
    const { voice, text } = req.body;

    if (!voice || !text) {
      return res.status(400).json({ error: 'voice and text are required' });
    }

    const geminiKey = process.env.GEMINI_API_KEY || process.env.API_KEY;
    if (!geminiKey) {
      return res.status(500).json({ error: 'GEMINI_API_KEY not configured in server/.env' });
    }

    // Try TTS-capable models in order (v1beta for stable, v1alpha for preview)
    const modelConfigs = [
      { model: 'gemini-2.5-flash-preview-tts', api: 'v1beta' },
      { model: 'gemini-2.0-flash', api: 'v1alpha' },
      { model: 'gemini-2.0-flash-lite', api: 'v1alpha' },
    ];

    let lastError = '';

    for (const { model, api } of modelConfigs) {
      const url = `https://generativelanguage.googleapis.com/${api}/models/${model}:generateContent?key=${geminiKey}`;

      const body = {
        contents: [{ parts: [{ text }] }],
        generationConfig: {
          responseModalities: ["AUDIO"],
          speechConfig: {
            voiceConfig: {
              prebuiltVoiceConfig: { voiceName: voice }
            }
          }
        }
      };

      console.log(`[VOICE-PREVIEW] Trying model: ${model}, voice: ${voice}`);

      try {
        const response = await fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body)
        });

        const data = await response.json();

        if (!response.ok) {
          lastError = data?.error?.message || JSON.stringify(data);
          console.log(`[VOICE-PREVIEW] Model ${model} failed: ${lastError}`);
          continue;
        }

        // Extract audio
        const audioPart = data.candidates?.[0]?.content?.parts?.find(p => p.inlineData);
        if (!audioPart?.inlineData?.data) {
          lastError = 'No audio in response';
          console.log(`[VOICE-PREVIEW] Model ${model}: no audio part`);
          continue;
        }

        const rawMime = audioPart.inlineData.mimeType || '';
        console.log(`[VOICE-PREVIEW] ✅ Success with ${model}, mime: ${rawMime}`);

        // If it's raw PCM (audio/L16), convert to WAV so browser can play it
        if (rawMime.includes('L16') || rawMime.includes('pcm') || rawMime.includes('raw')) {
          // Extract sample rate from mime type if present (e.g. "audio/L16;rate=24000")
          const rateMatch = rawMime.match(/rate=(\d+)/);
          const sampleRate = rateMatch ? parseInt(rateMatch[1]) : 24000;

          const wavBuffer = createWavFromPcm(audioPart.inlineData.data, sampleRate);
          console.log(`[VOICE-PREVIEW] Converted PCM to WAV (${sampleRate}Hz, ${wavBuffer.length} bytes)`);

          return res.json({
            audio: wavBuffer.toString('base64'),
            mimeType: 'audio/wav',
            model
          });
        }

        // For mp3/ogg/etc, return as-is
        return res.json({
          audio: audioPart.inlineData.data,
          mimeType: rawMime || 'audio/mp3',
          model
        });

      } catch (fetchErr) {
        lastError = fetchErr.message;
        console.log(`[VOICE-PREVIEW] Model ${model} fetch error: ${lastError}`);
        continue;
      }
    }

    return res.status(502).json({ error: 'All TTS models failed', details: lastError });

  } catch (err) {
    console.error('[VOICE-PREVIEW] Error:', err);
    return res.status(500).json({ error: err.message || 'Unknown error' });
  }
});

export default router;
