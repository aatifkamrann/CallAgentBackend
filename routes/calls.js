/**
 * Call Routes ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â Twilio Media Streams + Gemini Native Audio API
 * 
 * Architecture:
 * 1. POST /make-call ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ Twilio REST API with TwiML <Connect><Stream>
 * 2. Twilio opens WebSocket to /media-stream
 * 3. handleMediaStream() relays audio:
 *    Twilio (Ãƒâ€šÃ‚Âµ-law 8kHz) ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬Â Gemini Native Audio (PCM 24kHz bidirectional)
 * 4. Records both streams, saves WAV on call end
 * 5. AI analyzes transcript, updates customer + call logs
 * 
 * Model: gemini-2.5-flash-native-audio-preview
 * All keys from server/.env ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â no cloud dependencies
 */

import { Router } from 'express';
import { v4 as uuidv4 } from 'uuid';
import WebSocket from 'ws';
import db from '../db.js';
import { existsSync, mkdirSync, writeFileSync, appendFileSync, readFileSync } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import log, { sanitizeLogText } from '../logger.js';
import { getRetryConfig } from '../scheduler.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RECORDINGS_DIR = path.join(__dirname, '..', 'data', 'recordings');
if (!existsSync(RECORDINGS_DIR)) mkdirSync(RECORDINGS_DIR, { recursive: true });

const router = Router();
const GEMINI_TEXT_MODEL = 'gemini-2.5-flash';
const GEMINI_NATIVE_AUDIO_MODEL = 'gemini-2.5-flash-native-audio-preview-12-2025';
const USE_SEMANTIC_OUTCOME_MODEL = false;
const USE_NATIVE_AUDIO_FOR_OPENING_GREETING = false;
const CALL_PREPARATION_CACHE = new Map();
const CALL_PREPARATION_PROMISES = new Map();
const STATIC_GREETING_CACHE = new Map(); // voice ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ { payloads, durationMs }
let GENERIC_FALLBACK_PAYLOADS = []; // Last-resort: silence-breaking tone
const PREWARMED_GEMINI_WS = new Map(); // convId ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ WebSocket (pre-connected during RINGING)
const CONVERSATION_CALL_LOG_MAP = new Map(); // convId -> latest inserted call_log.id
const PREWARM_REUSE_STALE_MS = 12000;
const PREWARM_SETUP_GRACE_MS = 1500;

// Called by scheduler after a call ends so the next queued customer is dialed immediately
let afterCallHook = null;
export function setAfterCallHook(fn) { afterCallHook = fn; }
const STRICT_ROMAN_URDU_GUARD = `ROMAN URDU LANGUAGE LOCK (MANDATORY):
- Speak and write in PURE Pakistani Roman Urdu only.
- Use English alphabet only.
- Never output Urdu script, Hindi/Devanagari script, Arabic script, or mixed-script text.
- If any word comes to you in non-Roman text, transliterate it to Roman Urdu before responding.
- Keep phone-call speech natural, short, and conversational in Roman Urdu.`;

// Helper: atomically remove + close a pre-warmed Gemini WebSocket
function cleanupPrewarmed(convId) {
  const ws = PREWARMED_GEMINI_WS.get(convId);
  if (!ws) return;
  // Only delete if the stored instance is the same we fetched (avoid races)
  if (PREWARMED_GEMINI_WS.get(convId) === ws) PREWARMED_GEMINI_WS.delete(convId);
  try {
    // Prefer immediate termination to avoid lingering sockets; fall back to close
    if (typeof ws.terminate === 'function') {
      ws.terminate();
    } else if (typeof ws.close === 'function') {
      ws.close();
      // If close doesn't finish, force terminate after short timeout
      setTimeout(() => {
        try {
          if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
            if (typeof ws.terminate === 'function') ws.terminate();
          }
        } catch (e) {}
      }, 3000);
    }
  } catch (e) {}
}

// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
// NOISE GATE & VAD ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â Filter background noise before sending to Gemini
// Prevents ambient sounds (traffic, TV, wind, crowd) from confusing the AI
// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â

const NOISE_GATE = {
  // RMS energy threshold ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â RELAXED to avoid filtering real customer speech
  // Ãƒâ€šÃ‚Âµ-law decoded range is roughly -32768..32767; typical speech RMS is 300-5000
  SILENCE_RMS_THRESHOLD: 40,        // Very low floor ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â only filter dead silence
  SPEECH_RMS_THRESHOLD: 120,        // Confident speech level (was 350 ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â too aggressive)
  
  // Consecutive packet tracking for noise vs speech classification
  MIN_SPEECH_PACKETS: 1,            // Single speech packet opens gate (was 2 ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â missed first words)
  MAX_SILENCE_PACKETS: 25,          // Allow long natural pauses (was 8 ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â cut off mid-sentence gaps)
  
  // Adaptive noise floor ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â learns ambient noise level over first 30 packets
  CALIBRATION_PACKETS: 30,          // Faster calibration (was 50)
  NOISE_FLOOR_MULTIPLIER: 1.6,     // Speech must be 1.6x above noise floor (was 2.5 ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â too aggressive)
  
  // Energy smoothing (exponential moving average)
  EMA_ALPHA: 0.15,                  // Slower smoothing ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â prevents brief noise from opening gate (was 0.3)
};

function computeRmsEnergy(muLawBuffer) {
  if (!muLawBuffer || muLawBuffer.length === 0) return 0;
  let sumSquares = 0;
  for (let i = 0; i < muLawBuffer.length; i++) {
    const sample = MULAW_TO_LINEAR[muLawBuffer[i]];
    sumSquares += sample * sample;
  }
  return Math.sqrt(sumSquares / muLawBuffer.length);
}

// Per-call noise gate state factory
function createNoiseGateState() {
  return {
    noiseFloor: 0,
    calibrationSamples: [],
    calibrated: false,
    emaEnergy: 0,
    consecutiveSpeechPackets: 0,
    consecutiveSilencePackets: 0,
    gateOpen: false,                // true = passing audio through
    totalPackets: 0,
    filteredPackets: 0,
    speechPackets: 0,
  };
}

function shouldPassAudio(noiseGate, muLawBuffer) {
  const rms = computeRmsEnergy(muLawBuffer);
  noiseGate.totalPackets++;
  
  // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Phase 1: Calibration ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â learn the ambient noise floor ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
  if (!noiseGate.calibrated) {
    noiseGate.calibrationSamples.push(rms);
    if (noiseGate.calibrationSamples.length >= NOISE_GATE.CALIBRATION_PACKETS) {
      // Use median of lowest 70% as noise floor (excludes any speech during calibration)
      const sorted = [...noiseGate.calibrationSamples].sort((a, b) => a - b);
      const cutoff = Math.floor(sorted.length * 0.7);
      const lowSamples = sorted.slice(0, cutoff);
      noiseGate.noiseFloor = lowSamples.reduce((a, b) => a + b, 0) / lowSamples.length;
      noiseGate.calibrated = true;
      noiseGate.emaEnergy = rms;
      noiseGate.postCalibrationGrace = 50; // Pass next 50 packets unconditionally after calibration
    }
    // During calibration, pass all audio through (don't lose early speech)
    return { pass: true, rms, reason: 'calibrating' };
  }
  
  // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Grace period after calibration: pass all audio to avoid cutting first words ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
  if (noiseGate.postCalibrationGrace > 0) {
    noiseGate.postCalibrationGrace--;
    noiseGate.emaEnergy = NOISE_GATE.EMA_ALPHA * rms + (1 - NOISE_GATE.EMA_ALPHA) * noiseGate.emaEnergy;
    return { pass: true, rms, reason: 'post_calibration_grace' };
  }
  
  // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Phase 2: Adaptive energy check ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
  noiseGate.emaEnergy = NOISE_GATE.EMA_ALPHA * rms + (1 - NOISE_GATE.EMA_ALPHA) * noiseGate.emaEnergy;
  
  // Dynamic threshold: max of fixed threshold and adaptive noise floor
  const adaptiveThreshold = Math.max(
    NOISE_GATE.SILENCE_RMS_THRESHOLD,
    noiseGate.noiseFloor * NOISE_GATE.NOISE_FLOOR_MULTIPLIER
  );
  
  const isSpeechLevel = noiseGate.emaEnergy >= adaptiveThreshold;
  const isStrongSpeech = rms >= NOISE_GATE.SPEECH_RMS_THRESHOLD;
  
  if (isSpeechLevel || isStrongSpeech) {
    noiseGate.consecutiveSpeechPackets++;
    noiseGate.consecutiveSilencePackets = 0;
    
    // Open gate after MIN_SPEECH_PACKETS consecutive speech-level packets
    if (noiseGate.consecutiveSpeechPackets >= NOISE_GATE.MIN_SPEECH_PACKETS || isStrongSpeech) {
      noiseGate.gateOpen = true;
      noiseGate.speechPackets++;
      return { pass: true, rms, reason: 'speech' };
    }
    // Borderline ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â pass through if gate was already open
    if (noiseGate.gateOpen) {
      noiseGate.speechPackets++;
      return { pass: true, rms, reason: 'speech_continuation' };
    }
    // Building up speech detection ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â still pass through (was blocking ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â bad)
    noiseGate.speechPackets++;
    return { pass: true, rms, reason: 'speech_buildup' };
  } else {
    noiseGate.consecutiveSilencePackets++;
    noiseGate.consecutiveSpeechPackets = 0;
    
    // Keep gate open during natural pauses (up to MAX_SILENCE_PACKETS)
    if (noiseGate.gateOpen && noiseGate.consecutiveSilencePackets <= NOISE_GATE.MAX_SILENCE_PACKETS) {
      return { pass: true, rms, reason: 'pause_passthrough' };
    }
    
    // Close gate after sustained silence
    if (noiseGate.consecutiveSilencePackets > NOISE_GATE.MAX_SILENCE_PACKETS) {
      noiseGate.gateOpen = false;
    }
    
    noiseGate.filteredPackets++;
    return { pass: false, rms, reason: 'noise' };
  }
}

// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
// NETWORK QUALITY MONITOR ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â tracks Twilio WebSocket health
// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â

function createNetworkMonitor() {
  return {
    lastMediaAt: 0,              // Timestamp of last media packet
    mediaGaps: [],               // Array of gap durations (ms) for quality tracking
    maxGapMs: 0,                 // Largest gap between media packets
    totalMediaPackets: 0,
    networkQuality: 'good',      // good | degraded | poor
    degradedAt: 0,               // When quality first dropped
    jitterBuffer: [],            // Recent inter-packet intervals for jitter calc
    JITTER_WINDOW: 20,           // Packets to track for jitter
    GAP_THRESHOLD_MS: 500,       // Gap > 500ms = potential network issue
    POOR_THRESHOLD_MS: 2000,     // Gap > 2s = poor network
  };
}

function updateNetworkMonitor(monitor) {
  const now = Date.now();
  monitor.totalMediaPackets++;
  
  if (monitor.lastMediaAt > 0) {
    const gap = now - monitor.lastMediaAt;
    
    // Track jitter (inter-packet timing variation)
    monitor.jitterBuffer.push(gap);
    if (monitor.jitterBuffer.length > monitor.JITTER_WINDOW) {
      monitor.jitterBuffer.shift();
    }
    
    // Detect network quality changes
    if (gap > monitor.POOR_THRESHOLD_MS) {
      if (monitor.networkQuality !== 'poor') {
        monitor.networkQuality = 'poor';
        monitor.degradedAt = now;
      }
      monitor.maxGapMs = Math.max(monitor.maxGapMs, gap);
      monitor.mediaGaps.push({ at: now, gapMs: gap, quality: 'poor' });
    } else if (gap > monitor.GAP_THRESHOLD_MS) {
      if (monitor.networkQuality === 'good') {
        monitor.networkQuality = 'degraded';
        monitor.degradedAt = now;
      }
      monitor.maxGapMs = Math.max(monitor.maxGapMs, gap);
      monitor.mediaGaps.push({ at: now, gapMs: gap, quality: 'degraded' });
    } else if (monitor.networkQuality !== 'good') {
      // Recovery: if we get 10 consecutive normal packets, mark as good again
      const recentGaps = monitor.jitterBuffer.slice(-10);
      if (recentGaps.length >= 10 && recentGaps.every(g => g < monitor.GAP_THRESHOLD_MS)) {
        monitor.networkQuality = 'good';
        monitor.degradedAt = 0;
      }
    }
  }
  
  monitor.lastMediaAt = now;
  return monitor.networkQuality;
}

function getNetworkJitter(monitor) {
  if (monitor.jitterBuffer.length < 3) return 0;
  const avg = monitor.jitterBuffer.reduce((a, b) => a + b, 0) / monitor.jitterBuffer.length;
  const variance = monitor.jitterBuffer.reduce((sum, v) => sum + Math.pow(v - avg, 2), 0) / monitor.jitterBuffer.length;
  return Math.round(Math.sqrt(variance));
}

// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
// ACTIVE CALL REGISTRY ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â tracks every live call for monitoring & cleanup
// Each entry: { convId, callSid, startedAt, twilioWs, geminiWs, customerName }
// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
const ACTIVE_CALLS = new Map(); // convId ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ { ... }

// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
// RESOURCE LIMITS ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â prevent unbounded growth during concurrent calling
// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
const RESOURCE_LIMITS = {
  MAX_ACTIVE_CALLS: 10,              // Hard cap on simultaneous live calls
  MAX_PREP_CACHE_SIZE: 50,           // Max entries in CALL_PREPARATION_CACHE
  MAX_PREWARM_WS: 10,               // Max pre-warmed Gemini WebSockets
  TWILIO_API_TIMEOUT_MS: 10000,      // 10s timeout for Twilio REST API calls
  GEMINI_PREWARM_TIMEOUT_MS: 3000,   // 3s max wait for pre-warm
  TWILIO_STATUS_POLL_MS: 8000,       // Poll Twilio 8s after dial if no webhook callback received (faster cancel/no-answer detection)
  NO_PICKUP_FALLBACK_MS: 70000,      // Finalize as no-answer if call never reaches media stream
  CALL_PREP_TTL_MS: 5 * 60 * 1000,  // 5 min ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â discard stale prep cache entries
  IN_FLIGHT_CALLS: new Set(),        // Track calls being dialed (prevent double-dial)
};

// Periodic cache cleanup ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â prevent memory leaks
setInterval(() => {
  const now = Date.now();
  // Clean stale CALL_PREPARATION_CACHE entries
  for (const [id, assets] of CALL_PREPARATION_CACHE) {
    if (!ACTIVE_CALLS.has(id) && assets.preparedAt) {
      const age = now - new Date(assets.preparedAt).getTime();
      if (age > RESOURCE_LIMITS.CALL_PREP_TTL_MS) {
        CALL_PREPARATION_CACHE.delete(id);
      }
    }
  }
  // Clean orphaned pre-warmed WebSockets
  for (const [id, ws] of PREWARMED_GEMINI_WS) {
    if (!ACTIVE_CALLS.has(id) && ws._prewarmCreatedAt && now - ws._prewarmCreatedAt > 60000) {
      PREWARMED_GEMINI_WS.delete(id);
      try { ws.close(); } catch {}
    }
  }
  // Clean in-flight set (safety ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â entries older than 30s)
  // (in-flight is managed in makeCall, but clean stale entries just in case)
}, 30000);

export function getActiveCallCount() { return ACTIVE_CALLS.size; }
export function getActiveCalls() {
  const calls = [];
  for (const [convId, info] of ACTIVE_CALLS) {
    calls.push({
      convId,
      callSid: info.callSid,
      customerName: info.customerName,
      startedAt: info.startedAt,
      elapsedMs: Date.now() - info.startedAt,
      twilioOpen: info.twilioWs?.readyState === WebSocket.OPEN,
      geminiOpen: info.geminiWs?.readyState === WebSocket.OPEN,
    });
  }
  return calls;
}

// Periodic stale call cleanup ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â kill calls stuck > 10 minutes with no active WS
const STALE_CALL_MAX_MS = 10 * 60 * 1000;
setInterval(() => {
  const now = Date.now();
  for (const [convId, info] of ACTIVE_CALLS) {
    const age = now - info.startedAt;
    if (age > STALE_CALL_MAX_MS) {
      const twilioOpen = info.twilioWs?.readyState === WebSocket.OPEN;
      const geminiOpen = info.geminiWs?.readyState === WebSocket.OPEN;
      if (!twilioOpen && !geminiOpen) {
        log.warn('CLEANUP', `Removing stale call ${convId} (age=${Math.round(age/1000)}s, no active WS)`);
        ACTIVE_CALLS.delete(convId);
        CALL_PREPARATION_CACHE.delete(convId);
        PREWARMED_GEMINI_WS.delete(convId);
      }
    }
  }
}, 60000);
const SUPPORTED_GEMINI_VOICES = new Set([
  'Kore', 'Aoede', 'Zephyr', 'Achernar',
  'Puck', 'Charon', 'Fenrir', 'Orus',
  'Alnilam', 'Algenib', 'Iapetus', 'Umbriel',
  'Laomedeia', 'Pulcherrima', 'Vindemiatrix',
]);
const DEFAULT_VOICE_BY_GENDER = {
  female: 'Kore',
  male: 'Orus',
};

const FEMALE_VOICE_HINTS = new Set([
  'Kore', 'Aoede', 'Zephyr', 'Achernar',
  'Laomedeia', 'Pulcherrima', 'Vindemiatrix',
]);

const MALE_VOICE_HINTS = new Set([
  'Orus', 'Puck', 'Charon', 'Fenrir',
  'Alnilam', 'Algenib', 'Iapetus', 'Umbriel',
]);

function inferGenderFromVoice(voiceName) {
  const voice = String(voiceName || '').trim();
  if (!voice) return null;
  if (FEMALE_VOICE_HINTS.has(voice)) return 'female';
  if (MALE_VOICE_HINTS.has(voice)) return 'male';
  return null;
}

function normalizeGender(value, fallback = 'male') {
  const normalized = String(value || fallback).trim().toLowerCase();
  if (normalized === 'female') return 'female';
  if (normalized === 'male') return 'male';
  return fallback;
}

function resolveCallerGender(callerGender, callerName = '', fallback = 'male', callerVoice = '') {
  const direct = String(callerGender || '').trim().toLowerCase();
  if (direct === 'female' || direct === 'male') return direct;

  const byVoice = inferGenderFromVoice(callerVoice);
  if (byVoice) return byVoice;

  const firstName = String(callerName || '')
    .trim()
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean)[0] || '';

  if (firstName) {
    const maleNames = new Set([
      'bilal', 'omar', 'umair', 'ali', 'ahmed', 'muhammad', 'mohammad', 'hassan', 'hussain', 'faraz',
      'faisal', 'usman', 'osman', 'zain', 'waqas', 'saad', 'rizwan', 'kamran', 'adnan', 'salman',
      'imran', 'shoaib', 'junaid', 'hamza', 'arslan', 'talha', 'danish', 'irfan', 'nadeem', 'yasir',
    ]);
    const femaleNames = new Set([
      'ayesha', 'aisha', 'fatima', 'sana', 'hina', 'maham', 'zainab', 'iqra', 'maria', 'mariam',
      'sadia', 'anum', 'amna', 'saba', 'nimra', 'rabia', 'mehwish', 'sanam', 'laiba', 'komal',
      'sahar', 'sidra', 'tania', 'kiran', 'noor', 'saira', 'mahnoor', 'bisma', 'areeba', 'hira',
    ]);
    if (maleNames.has(firstName)) return 'male';
    if (femaleNames.has(firstName)) return 'female';
  }

  return fallback;
}

function normalizeVoiceName(voiceName, gender = 'female') {
  const normalizedGender = normalizeGender(gender, 'female');
  const candidate = String(voiceName || '').trim();
  if (SUPPORTED_GEMINI_VOICES.has(candidate)) return candidate;
  return DEFAULT_VOICE_BY_GENDER[normalizedGender] || 'Kore';
}

// Convert a Date to natural Urdu like "Chaudah April" (cardinal, not ordinal ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â native speech style)
function dateToNaturalUrdu(date) {
  const urduCardinal = [
    '', 'Pehli', 'Doosri', 'Teesri', 'Chaar', 'Paanch', 'Chhe', 'Saat',
    'Aath', 'Nau', 'Das', 'Gyaarah', 'Baarah', 'Terah', 'Chaudah',
    'Pandrah', 'Solah', 'Satrah', 'Atharah', 'Unnees', 'Bees',
    'Ikkees', 'Baais', 'Teis', 'Chaubees', 'Pacchees', 'Chabbees',
    'Sattais', 'Atthais', 'Untees', 'Tees', 'Ikattees',
  ];
  const months = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December',
  ];
  const d = date instanceof Date ? date : new Date(date);
  const day = d.getDate();
  const month = months[d.getMonth()];
  return `${urduCardinal[day] || day} ${month}`;
}

function getGeminiApiKey() {
  return process.env.GEMINI_API_KEY || process.env.API_KEY || '';
}

function hasEndCallDirective(value = '') {
  const raw = String(value || '');
  if (!raw) return false;
  return /\[\s*end[\s_-]*call\s*\]|\bend[\s_-]*call\b/i.test(raw);
}

function hasGoodbyeClosingPhrase(value = '') {
  const normalized = String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  if (!normalized) return false;
  return normalized.includes('allah hafiz')
    || normalized.includes('khuda hafiz')
    || normalized.includes('goodbye')
    || normalized.includes('good bye')
    || normalized.includes('ok bye')
    || normalized.includes('bye bye');
}

function normalizeBaseUrl(value = '') {
  return String(value || '').trim().replace(/\/+$/, '');
}

// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
// STATIC GREETING WARMUP ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â pre-generate common greetings at startup
// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â

// Generate brief silence instead of a beeping tone ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â customer hears natural silence while greeting loads
// Previous version used a 440Hz sine wave which caused annoying beeping
function generateConnectionTone() {
  const sampleRate = 8000;
  const durationSec = 0.15; // Very short silence placeholder
  const samples = Math.floor(sampleRate * durationSec);
  const muLawBuffer = Buffer.alloc(samples);
  // Fill with Ãƒâ€šÃ‚Âµ-law silence (0xFF = zero-level in Ãƒâ€šÃ‚Âµ-law encoding)
  muLawBuffer.fill(0xFF);
  return chunkTwilioPayloads(muLawBuffer);
}

// Warm up static greetings for commonly used voices at server startup
async function warmStaticGreetings() {
  const commonVoices = ['Kore', 'Aoede', 'Zephyr', 'Achernar', 'Puck', 'Charon', 'Fenrir', 'Orus'];
  const genericGreetingFemale = 'Assalam-o-Alaikum. Main JS Bank se bol rahi hoon. Ek lamha please.';
  const genericGreetingMale = 'Assalam-o-Alaikum. Main JS Bank se bol raha hoon. Ek lamha please.';

  GENERIC_FALLBACK_PAYLOADS = generateConnectionTone();
  log.info('STARTUP', `Generated connection tone fallback (${GENERIC_FALLBACK_PAYLOADS.length} chunks)`);

  const apiKey = getGeminiApiKey();
  if (!apiKey) {
    log.warn('STARTUP', 'No Gemini API key ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â static greeting warmup skipped');
    return;
  }

  for (const voice of commonVoices) {
    try {
      const [femaleRes, maleRes] = await Promise.allSettled([
        generatePreparedGreetingAudio(voice, genericGreetingFemale),
        generatePreparedGreetingAudio(voice, genericGreetingMale),
      ]);

      const entry = {};
      if (femaleRes.status === 'fulfilled' && femaleRes.value) {
        entry.female = { payloads: femaleRes.value.payloads, durationMs: femaleRes.value.durationMs };
        log.info('STARTUP', `Static female greeting cached for voice "${voice}" (${femaleRes.value.durationMs}ms, ${femaleRes.value.payloads.length} chunks)`);
      }
      if (maleRes.status === 'fulfilled' && maleRes.value) {
        entry.male = { payloads: maleRes.value.payloads, durationMs: maleRes.value.durationMs };
        log.info('STARTUP', `Static male greeting cached for voice "${voice}" (${maleRes.value.durationMs}ms, ${maleRes.value.payloads.length} chunks)`);
      }
      if (Object.keys(entry).length) STATIC_GREETING_CACHE.set(voice, entry);
    } catch (err) {
      log.warn('STARTUP', `Failed to warm static greetings for "${voice}": ${err.message}`);
    }
  }
}

// Start warming greetings after server starts (non-blocking)
setTimeout(() => warmStaticGreetings().catch(e => log.warn('STARTUP', `Static warmup error: ${e.message}`)), 2000);

function getServerBaseUrl() {
  return normalizeBaseUrl(process.env.SERVER_URL || process.env.BASE_URL || '');
}

function getRequestBaseUrl(req) {
  const forwardedProto = String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim();
  const forwardedHost = String(req.headers['x-forwarded-host'] || '').split(',')[0].trim();
  const host = forwardedHost || req.headers.host || '';
  const proto = forwardedProto || (req.secure ? 'https' : 'http');
  return host ? normalizeBaseUrl(`${proto}://${host}`) : '';
}

function buildTwilioStreamBaseUrl(req) {
  // 1. Prefer explicit WSS_URL
  const wssUrl = normalizeBaseUrl(process.env.WSS_URL || '');
  if (wssUrl) return wssUrl;

  // 2. Use SERVER_URL (convert httpÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ws, httpsÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢wss)
  const serverUrl = normalizeBaseUrl(getServerBaseUrl());
  if (serverUrl) return serverUrl.replace(/^http:\/\//i, 'ws://').replace(/^https:\/\//i, 'wss://');

  // 3. Derive from request headers
  const requestBaseUrl = getRequestBaseUrl(req);
  if (requestBaseUrl) return requestBaseUrl.replace(/^http:\/\//i, 'ws://').replace(/^https:\/\//i, 'wss://');

  // 4. Fallback to localhost
  const port = process.env.PORT || 3001;
  return `ws://localhost:${port}`;
}

function getDebugSessionDir(conversationId = 'unknown') {
  const debugDir = path.join(RECORDINGS_DIR, 'debug', conversationId || 'unknown');
  if (!existsSync(debugDir)) mkdirSync(debugDir, { recursive: true });
  return debugDir;
}

function sanitizePreparedText(value) {
  return String(value || '')
    .replace(/\r/g, '')
    .trim();
}

function escapeXml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/"/g, '&quot;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/'/g, '&apos;');
}

function buildExactGreeting(conv) {
  const customerName = conv?.customer_name || conv?.customerName || 'Customer';
  const callerName = conv?.caller_name || conv?.callerName || 'Omar';
  const custGender = normalizeGender(conv?.customer_gender || conv?.customerGender || 'male', 'male');
  const callerGender = resolveCallerGender(
    conv?.caller_gender || conv?.callerGender,
    callerName,
    'male',
    conv?.voice || conv?.callerVoice || ''
  );
  const honorific = custGender === 'female' ? 'Sahiba' : 'Sahab';
  const bolVerb = callerGender === 'female' ? 'bol rahi' : 'bol raha';
  const saktiVerb = callerGender === 'female' ? 'sakti' : 'sakta';
  // Keep pickup short, respectful, and identity-first so the customer can respond naturally.
  return `Assalam-o-Alaikum. Main ${callerName} ${bolVerb} hoon JS Bank se. Kya main ${customerName} ${honorific} se baat kar ${saktiVerb} hoon?`;
}

function buildFallbackPreparedScript(conv) {
  const greeting = buildExactGreeting(conv);
  const balance = Number(conv?.balance || 0).toLocaleString();
  const dpd = Number(conv?.dpd || 0);
  const custGender = normalizeGender(conv?.customer_gender || conv?.customerGender || 'male', 'male');
  const callerName = conv?.caller_name || conv?.callerName || 'Omar';
  const callerGender = resolveCallerGender(
    conv?.caller_gender || conv?.callerGender,
    callerName,
    'male',
    conv?.voice || conv?.callerVoice || ''
  );
  const honorific = custGender === 'female' ? 'Sahiba' : 'Sir';
  const samajhVerb = callerGender === 'female' ? 'samajh gayi' : 'samajh gaya';

  return [
    'Opening:',
    greeting,
    '',
    'Consent:',
    `${honorific}, main aapse aapke credit card dues ke baare mein baat karna ${callerGender === 'female' ? 'chahti' : 'chahta'} hoon. Kya ye munasib waqt hai?`,
    '',
    'Collection:',
    `Aap ke credit card par PKR ${balance} pending hai aur account ${dpd} din overdue hai.`,
    '',
    'Closing:',
    'Theek hai. Shukriya. Allah Hafiz.',
  ].join('\n');
}

function extractOpeningGreetingFromScript(script, conv) {
  const cleanScript = sanitizePreparedText(script);
  if (!cleanScript) return buildExactGreeting(conv);

  const lines = cleanScript
    .split('\n')
    .map(line => line.trim())
    .filter(Boolean);

  let takeNextLine = false;
  for (const rawLine of lines) {
    const line = rawLine.replace(/^[*-]\s*/, '');

    if (/^opening\b[:\-]?/i.test(line)) {
      const sameLineGreeting = line.replace(/^opening\b[:\-]?\s*/i, '').trim();
      if (sameLineGreeting) return sameLineGreeting;
      takeNextLine = true;
      continue;
    }

    if (takeNextLine) return line;
  }

  const greetingLine = lines.find(line => /assalam|salaam|main .*bank se|kaise hain/i.test(line));
  return greetingLine ? greetingLine.replace(/^[*-]\s*/, '') : buildExactGreeting(conv);
}

function ensureLowLatencyOpeningGreeting(candidate, conv) {
  const callerName = conv?.caller_name || conv?.callerName || 'Omar';
  const callerGender = resolveCallerGender(conv?.caller_gender || conv?.callerGender, callerName, 'male');
  const expectedBolVerb = callerGender === 'female' ? 'bol rahi' : 'bol raha';

  const cleanCandidate = sanitizePreparedText(candidate)
    // Never disclose bot identity in the spoken opening line.
    .replace(/\bai\s+debt\s+collection\s+agent\b/gi, 'recovery team')
    .replace(/\bvirtual\s+recovery\s+agent\b/gi, 'recovery team')
    .replace(/\bai\s+agent\b/gi, 'bank representative')
    // Natural phrasing: prefer "JS Bank se <name>" over "JS Bank ka/ki <name>".
    .replace(/\bmain\s+js\s*bank\s+k[ai]\s+([^,.!?\n]+?)\s+bol\s+(?:rahi|raha)\s+hoon\b/gi, (_m, detectedName) => `main JS Bank se ${String(detectedName || callerName).trim()} ${expectedBolVerb} hoon`)
    // Enforce caller-gender agreement for opening self-introduction.
    .replace(/\bbol\s+(?:rahi|raha)\s+hoon\b/gi, `${expectedBolVerb} hoon`);
  if (!cleanCandidate) return buildExactGreeting(conv);

  const lower = cleanCandidate.toLowerCase();
  const looksInteractive =
    cleanCandidate.includes('?') ||
    /kaise|kese|munasib waqt|sun sakte|awaz aa|aap theek|hello/.test(lower);

  return looksInteractive ? buildExactGreeting(conv) : cleanCandidate;
}

function createCallAssets(conv, options = {}) {
  const callScript = sanitizePreparedText(options.preparedScript) || buildFallbackPreparedScript(conv);
  const openingGreeting = ensureLowLatencyOpeningGreeting(extractOpeningGreetingFromScript(callScript, conv), conv);
  const customerGender = normalizeGender(conv?.customer_gender || conv?.customerGender || 'male', 'male');
  const callerName = conv?.caller_name || conv?.callerName || 'Omar';
  const callerGender = resolveCallerGender(
    conv?.caller_gender || conv?.callerGender,
    callerName,
    'male',
    conv?.voice || conv?.callerVoice || ''
  );
  const safeVoice = normalizeVoiceName(conv?.voice || conv?.callerVoice || '', callerGender);
  const assets = {
    source: options.preparedScript ? 'generated-script' : 'fallback-script',
    preparedAt: new Date().toISOString(),
    callScript,
    openingGreeting,
    customer_id: conv?.customer_id || conv?.customerId || '',
    customer_name: conv?.customer_name || conv?.customerName || 'Customer',
    caller_name: conv?.caller_name || conv?.callerName || 'Omar',
    customer_gender: customerGender,
    caller_gender: callerGender,
    voice: safeVoice,
    balance: Number(conv?.balance || 0),
    dpd: Number(conv?.dpd || 0),
    ptp_status: conv?.ptp_status || null,
    follow_up_count: Number(conv?.follow_up_count || 0),
    tone: conv?.tone || 'polite',
    agent_type: conv?.agent_type || 'fresh_call',
    fetchTwilioRecording: options.fetchTwilioRecording !== false,
    maxPtpDays: options.maxPtpDays || 5,
    customSystemPrompt: sanitizePreparedText(options.customSystemPrompt) || null,
    noiseCancellation: options.noiseCancellation === true,  // OFF by default
    openingGreetingPayloads: [],
    openingGreetingDurationMs: 0,
    greetingWarmSource: null,
    // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Previous call history for follow-up/redial context ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
    previousCallHistory: conv?.previousCallHistory || null,
    ptpDate: conv?.ptpDate || null,
    retryReason: conv?.retryReason || null,
  };

  if (conv?.id) {
    CALL_PREPARATION_CACHE.set(conv.id, assets);

    const debugDir = getDebugSessionDir(conv.id);
    writeFileSync(path.join(debugDir, 'prepared_script.txt'), `${callScript}\n`, 'utf8');
    writeFileSync(path.join(debugDir, 'opening_greeting.txt'), `${openingGreeting}\n`, 'utf8');
    writeFileSync(path.join(debugDir, 'call_assets.json'), JSON.stringify({
      conversationId: conv.id,
      customerName: conv.customer_name || conv.customerName || null,
      callerName: conv.caller_name || conv.callerName || null,
      balance: Number(conv.balance || 0),
      dpd: Number(conv.dpd || 0),
      source: assets.source,
      preparedAt: assets.preparedAt,
      scriptChars: callScript.length,
      openingGreeting,
      fetchTwilioRecording: assets.fetchTwilioRecording,
      maxPtpDays: assets.maxPtpDays,
    }, null, 2), 'utf8');
  }

  return assets;
}

function startCallAssetWarmup(conv, options = {}) {
  const assets = createCallAssets(conv, options);
  const warmPromise = warmPreparedGreetingAudio(assets, conv)
    .catch((err) => {
      log.warn('CALL', `Prepared greeting background warm failed: ${err.message}`);
      return assets;
    })
    .finally(() => {
      if (conv?.id) CALL_PREPARATION_PROMISES.delete(conv.id);
    });

  if (conv?.id) CALL_PREPARATION_PROMISES.set(conv.id, warmPromise);
  return { assets, warmPromise };
}

async function prepareCallAssets(conv, options = {}) {
  const { warmPromise } = startCallAssetWarmup(conv, options);
  return await warmPromise;
}

function clearCustomerRetrySchedule(customerId, source = 'call-flow') {
  if (!customerId) return;
  db.prepare(`UPDATE customers SET scheduled_retry_at = NULL, retry_reason = NULL, updated_at = datetime('now') WHERE id = ?`)
    .run(customerId);
  log.info('CALL', `Cleared pending retry schedule (${source}) for customer ${customerId}`);
}

function getConversationByIdOrCallSid(identifier) {
  if (!identifier) return null;
  return db.prepare('SELECT * FROM call_conversations WHERE id = ? OR call_sid = ? ORDER BY created_at DESC LIMIT 1').get(identifier, identifier);
}

// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
// Dashboard Broadcast ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â real-time telemetry to frontend
// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â

const dashboardClients = new Set();
export function registerDashboardClient(ws) {
  dashboardClients.add(ws);
  ws.on('close', () => dashboardClients.delete(ws));
}

function broadcast(data) {
  const message = JSON.stringify(data);
  dashboardClients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}

function emitStatusUpdate({
  conversationId = '',
  callSid = '',
  status = '',
  finalResponse = null,
  terminalReason = '',
  duration = 0,
  sipResponseCode = '',
  answeredBy = '',
}) {
  broadcast({
    type: 'STATUS_UPDATE',
    status,
    conversationId: conversationId || null,
    callSid: callSid || null,
    final_response: finalResponse || null,
    terminal_reason: terminalReason || null,
    duration: Number(duration || 0),
    sip_response_code: sipResponseCode || null,
    answered_by: answeredBy || null,
    updated_at: new Date().toISOString(),
  });
}

function logTwilioLearnedStatus({
  source = 'unknown',
  conversationId = '',
  callSid = '',
  status = '',
  duration = 0,
  sipResponseCode = '',
  answeredBy = '',
  note = '',
}) {
  const convShort = conversationId ? String(conversationId).slice(0, 8) : 'n/a';
  const sidSafe = callSid || 'n/a';
  const statusSafe = status || 'unknown';
  const dur = Number(duration || 0);
  const sip = sipResponseCode || 'n/a';
  const ans = answeredBy || 'n/a';
  const suffix = note ? ` note=${note}` : '';
  log.info('STATUS', `[TWILIO-STATUS] source=${source} conv=${convShort} sid=${sidSafe} status=${statusSafe} duration=${dur}s sip=${sip} answeredBy=${ans}${suffix}`);
}

// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
// Ãƒâ€šÃ‚Âµ-law CODEC ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â Twilio sends/receives Ãƒâ€šÃ‚Âµ-law 8kHz audio
// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â

const MULAW_BIAS = 0x84;
const MULAW_MAX = 32635;

function linearToMulaw(sample) {
  let sign = 0;
  if (sample < 0) { sign = 0x80; sample = -sample; }
  if (sample > MULAW_MAX) sample = MULAW_MAX;
  sample += MULAW_BIAS;
  let exponent = 7, mask = 0x4000;
  while ((sample & mask) === 0 && exponent > 0) { exponent--; mask >>= 1; }
  const mantissa = (sample >> (exponent + 3)) & 0x0F;
  return ~(sign | (exponent << 4) | mantissa) & 0xFF;
}

const MULAW_TO_LINEAR = new Int16Array(256);
for (let i = 0; i < 256; i++) {
  let val = ~i & 0xFF;
  const sign = val & 0x80;
  const exponent = (val >> 4) & 0x07;
  const mantissa = val & 0x0F;
  let magnitude = ((mantissa << 3) + MULAW_BIAS) << exponent;
  magnitude -= MULAW_BIAS;
  MULAW_TO_LINEAR[i] = sign ? -magnitude : magnitude;
}

// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
// Audio Transcoding ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â Twilio 8kHz ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬Â Gemini (16kHz input, 24kHz output)
// Uses linear interpolation for better audio quality
// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â

// INPUT: Twilio (8kHz Ãƒâ€šÃ‚Âµ-law) ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ Gemini (24kHz 16-bit PCM LE)
// Uses linear interpolation for clean upsampling (no sample repetition artifacts)
function decodeTwilioToGemini(muLawBuffer) {
  const inputSamples = muLawBuffer.length;
  const pcmBuffer = Buffer.alloc(inputSamples * 6); // 3 samples ÃƒÆ’Ã¢â‚¬â€ 2 bytes each

  // First decode all Ãƒâ€šÃ‚Âµ-law to PCM samples
  const decoded = new Int16Array(inputSamples);
  for (let i = 0; i < inputSamples; i++) {
    let muLaw = muLawBuffer[i];
    muLaw = ~muLaw; // Bit inversion (G.711 standard)

    const sign = (muLaw & 0x80);
    const exponent = (muLaw >> 4) & 0x07;
    const mantissa = muLaw & 0x0F;
    let sample = ((mantissa << 3) + 0x84) << exponent;
    sample -= 0x84;
    if (sign !== 0) sample = -sample;

    // Gentle volume boost (2x instead of 4x ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â prevents clipping distortion)
    sample = sample * 2;
    if (sample > 32767) sample = 32767;
    if (sample < -32768) sample = -32768;

    decoded[i] = sample;
  }

  // Linear interpolation upsample: 8kHz ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ 24kHz (ratio 3:1)
  for (let i = 0; i < inputSamples; i++) {
    const current = decoded[i];
    const next = (i + 1 < inputSamples) ? decoded[i + 1] : current;
    const offset = i * 6;

    // Sample 0: original
    pcmBuffer.writeInt16LE(current, offset);
    // Sample 1: 1/3 interpolation
    const interp1 = Math.round(current + (next - current) / 3);
    pcmBuffer.writeInt16LE(interp1, offset + 2);
    // Sample 2: 2/3 interpolation
    const interp2 = Math.round(current + (next - current) * 2 / 3);
    pcmBuffer.writeInt16LE(interp2, offset + 4);
  }
  return pcmBuffer;
}

// Helper: single PCM sample ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ Ãƒâ€šÃ‚Âµ-law byte
function pcmToMuLaw(pcm) {
  const BIAS = 0x84;
  const CLIP = 32635;
  let sign = (pcm & 0x8000) >> 8;
  if (sign !== 0) pcm = -pcm;
  if (pcm > CLIP) pcm = CLIP;
  pcm += BIAS;
  let exponent = 7;
  for (let expMask = 0x4000; (pcm & expMask) === 0 && exponent > 0; expMask >>= 1) {
    exponent--;
  }
  let mantissa = (pcm >> (exponent + 3)) & 0x0F;
  let res = (sign | (exponent << 4) | mantissa);
  return (~res) & 0xFF;
}

// OUTPUT: Gemini (24kHz 16-bit PCM LE) ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ Twilio (8kHz Ãƒâ€šÃ‚Âµ-law)
// Uses averaging downsample (anti-aliasing) instead of point sampling
function encodeGeminiToTwilio(pcmBuffer) {
  const totalSamples = Math.floor(pcmBuffer.length / 2);
  const outputSize = Math.floor(totalSamples / 3); // 24kHz ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ 8kHz
  const muLawBuffer = Buffer.alloc(outputSize);
  for (let i = 0; i < outputSize; i++) {
    const baseIdx = i * 3;
    // Average 3 consecutive samples for anti-aliased downsampling
    let sum = 0;
    let count = 0;
    for (let j = 0; j < 3 && (baseIdx + j) < totalSamples; j++) {
      sum += pcmBuffer.readInt16LE((baseIdx + j) * 2);
      count++;
    }
    const averaged = Math.round(sum / count);
    muLawBuffer[i] = pcmToMuLaw(averaged);
  }
  return muLawBuffer;
}

function encodePcmRateToTwilio(pcmBuffer, sourceSampleRate = 24000) {
  const sampleCount = Math.floor(pcmBuffer.length / 2);
  if (sampleCount <= 0 || !sourceSampleRate) return Buffer.alloc(0);

  const outputSampleCount = Math.max(1, Math.floor(sampleCount * 8000 / sourceSampleRate));
  const muLawBuffer = Buffer.alloc(outputSampleCount);

  for (let i = 0; i < outputSampleCount; i++) {
    const sourceIndex = Math.min(sampleCount - 1, Math.floor(i * sourceSampleRate / 8000));
    const sample = pcmBuffer.readInt16LE(sourceIndex * 2);
    muLawBuffer[i] = pcmToMuLaw(sample);
  }

  return muLawBuffer;
}

function chunkTwilioPayloads(muLawBuffer, bytesPerChunk = 160) {
  const payloads = [];
  for (let offset = 0; offset < muLawBuffer.length; offset += bytesPerChunk) {
    payloads.push(muLawBuffer.subarray(offset, offset + bytesPerChunk).toString('base64'));
  }
  return payloads;
}

async function generatePreparedGreetingAudio(voiceName, text, callerGender = 'male') {
  const apiKey = getGeminiApiKey();
  const safeVoiceName = normalizeVoiceName(voiceName, callerGender);
  if (!apiKey || !safeVoiceName || !text) return null;

  const modelConfigs = [
    { model: 'gemini-2.5-flash-preview-tts', api: 'v1beta' },
    { model: 'gemini-2.0-flash', api: 'v1alpha' },
  ];

  let lastError = '';

  for (const { model, api } of modelConfigs) {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 5000);

    try {
      const response = await fetch(`https://generativelanguage.googleapis.com/${api}/models/${model}:generateContent?key=${apiKey}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        signal: controller.signal,
        body: JSON.stringify({
          contents: [{ parts: [{ text }] }],
          generationConfig: {
            responseModalities: ['AUDIO'],
            speechConfig: {
              voiceConfig: {
                prebuiltVoiceConfig: { voiceName: safeVoiceName }
              }
            }
          }
        }),
      });

      clearTimeout(timeoutId);
      const data = await response.json();
      if (!response.ok) {
        lastError = data?.error?.message || JSON.stringify(data);
        continue;
      }

      const audioPart = data.candidates?.[0]?.content?.parts?.find((part) => part.inlineData?.data);
      const audioBase64 = audioPart?.inlineData?.data || '';
      const mimeType = audioPart?.inlineData?.mimeType || 'audio/L16;rate=24000';
      if (!audioBase64) {
        lastError = 'No audio returned for prepared greeting';
        continue;
      }

      const sampleRateMatch = mimeType.match(/rate=(\d+)/i);
      const sampleRate = sampleRateMatch ? Number(sampleRateMatch[1]) : 24000;
      const pcmBuffer = Buffer.from(audioBase64, 'base64');
      const muLawBuffer = encodePcmRateToTwilio(pcmBuffer, sampleRate);
      const payloads = chunkTwilioPayloads(muLawBuffer);

      if (payloads.length === 0) {
        lastError = 'Prepared greeting audio was empty after transcoding';
        continue;
      }

      return {
        model,
        mimeType,
        payloads,
        muLawBuffer,
        durationMs: Math.round((muLawBuffer.length / 8000) * 1000),
      };
    } catch (error) {
      clearTimeout(timeoutId);
      lastError = error?.name === 'AbortError' ? 'Timed out warming greeting audio' : error.message;
    }
  }

  log.warn('CALL', `Prepared greeting warm-up failed: ${lastError || 'unknown error'}`);
  return null;
}

async function warmPreparedGreetingAudio(callAssets, conv) {
  if (!callAssets?.openingGreeting || callAssets?.openingGreetingPayloads?.length) return callAssets;

  const callerGender = resolveCallerGender(
    callAssets?.caller_gender || conv?.caller_gender || conv?.callerGender,
    callAssets?.caller_name || conv?.caller_name || conv?.callerName || 'Omar',
    'male'
  );
  const voiceName = normalizeVoiceName(callAssets?.voice || conv?.voice || conv?.callerVoice || 'Kore', callerGender);
  const result = await generatePreparedGreetingAudio(voiceName, callAssets.openingGreeting, callerGender);
  if (!result) {
    // Do NOT use generic static-cache greeting when personalized caller identity is required.
    callAssets.greetingWarmSource = 'personalized-greeting-warm-failed';
    log.warn('CALL', `Skipping static-cache fallback to preserve caller identity in greeting (voice: ${voiceName})`);
    if (conv?.id && callAssets?.openingGreetingPayloads?.length) CALL_PREPARATION_CACHE.set(conv.id, callAssets);
    return callAssets;
  }

  callAssets.openingGreetingPayloads = result.payloads;
  callAssets.openingGreetingDurationMs = result.durationMs;
  callAssets.greetingWarmSource = `${result.model} | ${result.mimeType}`;

  if (conv?.id) {
    CALL_PREPARATION_CACHE.set(conv.id, callAssets);
    try {
      const debugDir = getDebugSessionDir(conv.id);
      writeFileSync(path.join(debugDir, 'opening_greeting.mulaw'), result.muLawBuffer);
      writeFileSync(path.join(debugDir, 'opening_greeting_meta.json'), JSON.stringify({
        voice: conv?.voice || 'Kore',
        greeting: callAssets.openingGreeting,
        payloadCount: result.payloads.length,
        durationMs: result.durationMs,
        source: callAssets.greetingWarmSource,
      }, null, 2), 'utf8');
    } catch {}
  }

  log.info('CALL', `Prepared greeting warmed (${result.durationMs}ms, ${result.payloads.length} chunks)`);
  return callAssets;
}

function resolveGreetingPayloads(callAssets, voiceName, options = {}) {
  const { allowToneFallback = false, forceConnectionTone = false } = options;
  const resolvedCallerGender = resolveCallerGender(
    callAssets?.caller_gender,
    callAssets?.caller_name || 'Omar',
    'male',
    callAssets?.voice || voiceName || ''
  );
  const safeVoiceName = normalizeVoiceName(voiceName, resolvedCallerGender);
  const desiredGender = resolvedCallerGender === 'male' ? 'male' : 'female';
  let payloadsToPlay = callAssets?.openingGreetingPayloads || [];
  let greetingSource = payloadsToPlay.length ? 'prepared' : 'none';
  const enforcePersonalizedGreeting = Boolean(callAssets?.caller_name);

  if (forceConnectionTone) {
    payloadsToPlay = [];
    greetingSource = 'none';
  }

  // For personalized caller identity flows, never use generic static cache audio.
  // Static cache clips can have mismatched persona/voice and cause female<->male switching.
  if (enforcePersonalizedGreeting && !payloadsToPlay.length) {
    if (allowToneFallback && GENERIC_FALLBACK_PAYLOADS.length) {
      payloadsToPlay = GENERIC_FALLBACK_PAYLOADS;
      greetingSource = 'connection-tone';
    }
    return { payloadsToPlay, greetingSource };
  }

  // If no prepared greeting, try gender-matched static cache first (even for personalized flows)
  // so customer hears immediate spoken greeting instead of waiting for live model speech.
  if (!payloadsToPlay.length) {
    const staticCache = STATIC_GREETING_CACHE.get(safeVoiceName);
    if (staticCache) {
      if (staticCache[desiredGender]?.payloads?.length) {
        payloadsToPlay = staticCache[desiredGender].payloads;
        greetingSource = enforcePersonalizedGreeting
          ? `static-voice-cache-${desiredGender}-personalized-fallback`
          : `static-voice-cache-${desiredGender}`;
      } else if (staticCache[desiredGender]?.payloads?.length === 0) {
        // explicit empty ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â skip
      }
    }
  }

  if (!payloadsToPlay.length) {
    for (const [, cached] of STATIC_GREETING_CACHE) {
      // Keep gender consistent even when falling back to any cached voice.
      if (cached?.[desiredGender]?.payloads?.length) {
        payloadsToPlay = cached[desiredGender].payloads;
        greetingSource = enforcePersonalizedGreeting
          ? `static-any-cache-${desiredGender}-personalized-fallback`
          : `static-any-cache-${desiredGender}`;
        break;
      }
    }
  }

  if (!payloadsToPlay.length && allowToneFallback && GENERIC_FALLBACK_PAYLOADS.length) {
    payloadsToPlay = GENERIC_FALLBACK_PAYLOADS;
    greetingSource = 'connection-tone';
  }

  return { payloadsToPlay, greetingSource };
}

// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
// Gemini AI Helper (text, for post-call analysis ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â REST API)
// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â

async function callGemini(prompt, systemPrompt, temperature = 0.7, maxTokens = 256) {
  const apiKey = getGeminiApiKey();
  if (!apiKey) return { ok: false, text: '', error: 'No GEMINI_API_KEY or API_KEY' };

  const contents = [];
  if (systemPrompt) {
    contents.push({ role: 'user', parts: [{ text: systemPrompt }] });
    contents.push({ role: 'model', parts: [{ text: 'Samajh gaya. Main tayar hoon.' }] });
  }
  contents.push({ role: 'user', parts: [{ text: prompt }] });

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000); // 15s timeout ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â prevents stuck calls
    const res = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/${GEMINI_TEXT_MODEL}:generateContent?key=${apiKey}`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ contents, generationConfig: { temperature, maxOutputTokens: maxTokens } }),
        signal: controller.signal,
      }
    );
    clearTimeout(timeout);
    const data = await res.json();
    if (!res.ok) return { ok: false, text: '', error: JSON.stringify(data) };
    return { ok: true, text: data.candidates?.[0]?.content?.parts?.[0]?.text || '' };
  } catch (err) {
    if (err.name === 'AbortError') {
      log.warn('CALL', 'callGemini timed out after 15s ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â using transcript fallback');
      return { ok: false, text: '', error: 'Request timed out (15s)' };
    }
    return { ok: false, text: '', error: err.message };
  }
}

function extractGeminiJsonSnippet(rawText, expectArray = false) {
  let raw = String(rawText || '').trim();
  if (!raw) return '';

  if (raw.startsWith('```')) {
    raw = raw.replace(/^```(?:json)?\n?/, '').replace(/\n?```$/, '').trim();
  }

  const primary = expectArray ? raw.match(/\[[\s\S]*\]/) : raw.match(/\{[\s\S]*\}/);
  if (primary) return primary[0];

  const secondary = expectArray ? raw.match(/\{[\s\S]*\}/) : raw.match(/\[[\s\S]*\]/);
  return secondary ? secondary[0] : '';
}

async function callGeminiJson(prompt, systemPrompt, options = {}) {
  const {
    temperature = 0.1,
    maxTokens = 900,
    retries = 1,
    expectArray = false,
  } = options;

  const attempts = Math.max(1, Number(retries || 1) + 1);
  let lastError = 'Unknown Gemini JSON failure';

  for (let attempt = 1; attempt <= attempts; attempt++) {
    const ai = await callGemini(prompt, systemPrompt, temperature, maxTokens);
    if (!ai.ok || !ai.text) {
      lastError = ai.error || 'Empty Gemini response';
      continue;
    }

    const snippet = extractGeminiJsonSnippet(ai.text, expectArray);
    if (snippet) {
      try {
        return { ok: true, data: JSON.parse(snippet), raw: ai.text, attempt };
      } catch (err) {
        lastError = err.message;
      }
    } else {
      lastError = 'No JSON found in Gemini response';
    }

    if (attempt < attempts) {
      const repairPrompt = `${prompt}\n\nYour previous output was not valid JSON. Return ONLY valid JSON (${expectArray ? 'array' : 'object'}) and nothing else.`;
      const repaired = await callGemini(repairPrompt, systemPrompt, 0, maxTokens);
      if (repaired.ok && repaired.text) {
        const repairedSnippet = extractGeminiJsonSnippet(repaired.text, expectArray);
        if (repairedSnippet) {
          try {
            return { ok: true, data: JSON.parse(repairedSnippet), raw: repaired.text, attempt };
          } catch (err) {
            lastError = err.message;
          }
        }
      }
    }
  }

  return { ok: false, data: null, error: lastError };
}

function getMimeTypeFromRecordingExt(ext = 'mp3') {
  const normalized = String(ext || '').toLowerCase();
  if (normalized === 'wav') return 'audio/wav';
  if (normalized === 'm4a') return 'audio/mp4';
  return 'audio/mpeg';
}

function getRecordingExtFromUrl(url = '') {
  const cleaned = String(url || '').split('?')[0].trim();
  const ext = path.extname(cleaned).replace('.', '').toLowerCase();
  return ext || 'mp3';
}

function getLocalRecordingPath(recordingUrl = '') {
  const cleanUrl = String(recordingUrl || '').trim();
  if (!cleanUrl.startsWith('/api/recordings/')) return null;
  const fileName = path.basename(cleanUrl.split('?')[0]);
  if (!fileName) return null;
  return path.join(RECORDINGS_DIR, fileName);
}

async function fetchTwilioRecordingAudio({ accountSid, authToken, recordingSid }) {
  const authHeader = 'Basic ' + Buffer.from(`${accountSid}:${authToken}`).toString('base64');
  const variants = [
    { ext: 'mp3', query: '?RequestedChannels=2', channels: 'dual' },
    { ext: 'wav', query: '?RequestedChannels=2', channels: 'dual' },
    { ext: 'mp3', query: '', channels: 'mono' },
    { ext: 'wav', query: '', channels: 'mono' },
  ];

  for (const variant of variants) {
    const url = `https://api.twilio.com/2010-04-01/Accounts/${accountSid}/Recordings/${recordingSid}.${variant.ext}${variant.query}`;
    const response = await fetch(url, { headers: { Authorization: authHeader } });
    if (!response.ok) continue;
    const audioBuffer = Buffer.from(await response.arrayBuffer());
    if (!audioBuffer.length) continue;
    return {
      buffer: audioBuffer,
      ext: variant.ext,
      mimeType: getMimeTypeFromRecordingExt(variant.ext),
      channels: variant.channels,
    };
  }

  return null;
}

async function transcribeRecordingWithGemini(audioBuffer, mimeType, context = {}) {
  const apiKey = getGeminiApiKey();
  if (!apiKey) return { ok: false, transcript: '', error: 'No GEMINI_API_KEY configured' };
  if (!audioBuffer || !audioBuffer.length) return { ok: false, transcript: '', error: 'Empty audio buffer' };

  const maxInlineBytes = 12 * 1024 * 1024;
  if (audioBuffer.length > maxInlineBytes) {
    return { ok: false, transcript: '', error: `Audio too large for inline transcription (${audioBuffer.length} bytes)` };
  }

  const customerName = context.customerName || 'Customer';
  const callerName = context.callerName || 'Agent';
  const prompt =
    `${STRICT_ROMAN_URDU_GUARD}
Transcribe this call recording exactly.
If both speakers are available, format each line as "${callerName}: ..." or "${customerName}: ...".
If speaker separation is unclear, still provide best-effort labels.
Write the spoken content in Pakistani Roman Urdu only using English letters.
Never output Urdu script, Arabic script, or Devanagari.
Return plain text transcript only, no markdown and no explanation.`;

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 30000);
    const response = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/${GEMINI_TEXT_MODEL}:generateContent?key=${apiKey}`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        signal: controller.signal,
        body: JSON.stringify({
          contents: [{
            role: 'user',
            parts: [
              { text: prompt },
              { inlineData: { mimeType: mimeType || 'audio/mpeg', data: audioBuffer.toString('base64') } },
            ],
          }],
          generationConfig: {
            temperature: 0.1,
            maxOutputTokens: 1800,
          },
        }),
      }
    );
    clearTimeout(timeout);

    const data = await response.json();
    if (!response.ok) {
      return { ok: false, transcript: '', error: data?.error?.message || JSON.stringify(data) };
    }

    const transcript = String(data?.candidates?.[0]?.content?.parts?.[0]?.text || '').trim();
    if (!transcript) return { ok: false, transcript: '', error: 'Gemini returned empty transcript' };

    const cleaned = await enforceRomanUrduStructuredTranscript(transcript, { callerName, customerName });
    if (!cleaned.ok || !cleaned.transcript) {
      return { ok: false, transcript: '', error: cleaned.reason || 'roman_urdu_transcript_guard_failed' };
    }
    return { ok: true, transcript: cleaned.transcript };
  } catch (err) {
    if (err?.name === 'AbortError') return { ok: false, transcript: '', error: 'Transcription timed out (30s)' };
    return { ok: false, transcript: '', error: err.message };
  }
}

function containsNonRomanUrduScript(text = '') {
  return /[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\u0900-\u097F]/.test(String(text || ''));
}

function countStructuredSpeakerLines(text = '', callerName = 'Agent', customerName = 'Customer') {
  const safeCaller = String(callerName || 'Agent').trim();
  const safeCustomer = String(customerName || 'Customer').trim();
  return String(text || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.startsWith(`${safeCaller}:`) || line.startsWith(`${safeCustomer}:`))
    .length;
}

async function enforceRomanUrduStructuredTranscript(rawTranscript = '', context = {}) {
  const callerName = String(context?.callerName || 'Agent').trim() || 'Agent';
  const customerName = String(context?.customerName || 'Customer').trim() || 'Customer';
  const source = String(rawTranscript || '').trim();
  if (!source) return { ok: false, transcript: '', reason: 'empty_transcript' };

  const structuredLines = countStructuredSpeakerLines(source, callerName, customerName);
  if (!containsNonRomanUrduScript(source) && structuredLines > 0) {
    return { ok: true, transcript: source };
  }

  const systemPrompt = `${STRICT_ROMAN_URDU_GUARD}
You are a transcript formatter for debt collection calls.
Task: transliterate and structure the transcript into Pakistani Roman Urdu.
Rules:
- Keep speaker labels exactly "${callerName}:" and "${customerName}:".
- Use English alphabet only.
- Never output Urdu script, Arabic script, or Devanagari.
- Preserve original meaning exactly.
- Output one utterance per line only.`;
  const prompt = `Convert this transcript into strict Roman Urdu with speaker labels preserved:\n\n${source}`;
  const ai = await callGemini(prompt, systemPrompt, 0.1, 1900);
  if (!ai.ok || !String(ai.text || '').trim()) {
    return { ok: false, transcript: '', reason: ai.error || 'roman_urdu_formatter_failed' };
  }

  const cleaned = String(ai.text || '')
    .replace(/^```(?:text|markdown)?\s*/i, '')
    .replace(/```\s*$/i, '')
    .trim();

  if (!cleaned || containsNonRomanUrduScript(cleaned)) {
    return { ok: false, transcript: '', reason: 'roman_urdu_script_guard' };
  }

  if (countStructuredSpeakerLines(cleaned, callerName, customerName) === 0) {
    return { ok: false, transcript: '', reason: 'roman_urdu_speaker_guard' };
  }

  return { ok: true, transcript: cleaned };
}

function computeScheduledRetryAt(hours = 0) {
  if (!(hours > 0)) return null;
  const retryTime = new Date(Date.now() + hours * 3600000);
  const retryHour = retryTime.getHours();

  if (retryHour >= 18 || retryHour < 9) {
    if (retryHour >= 18) retryTime.setDate(retryTime.getDate() + 1);
    retryTime.setHours(9, 0, 0, 0);
  }

  while (retryTime.getDay() === 0 || retryTime.getDay() === 6) {
    retryTime.setDate(retryTime.getDate() + 1);
    retryTime.setHours(9, 0, 0, 0);
  }

  return retryTime.toISOString();
}

const RECORDING_PENDING_MARKER = '[RECORDING_PENDING]';

function withRecordingPendingMarker(value = '') {
  const clean = String(value || '').trim();
  return clean ? `${RECORDING_PENDING_MARKER} ${clean}` : RECORDING_PENDING_MARKER;
}

function clearRecordingPendingMarker(value = '') {
  return String(value || '')
    .replace(/\[RECORDING_PENDING\]\s*/g, '')
    .trim();
}

function hasRecordingPendingMarker(value = '') {
  return String(value || '').includes(RECORDING_PENDING_MARKER);
}

async function finalizePendingRecordingFallback({ callLogId, reason = 'recording_unavailable' }) {
  if (!callLogId) return { updated: false, reason: 'missing_call_log_id' };

  const logRow = db.prepare('SELECT id, customer_id, outcome, ptp_date, notes FROM call_logs WHERE id = ?').get(callLogId);
  if (!logRow?.customer_id) return { updated: false, reason: 'call_log_not_found' };

  const customer = db.prepare('SELECT id, final_response, ptp_date, retry_reason FROM customers WHERE id = ?').get(logRow.customer_id);
  const finalResponse = logRow.outcome || customer?.final_response || 'no_answer';
  const ptpDate = logRow.ptp_date || customer?.ptp_date || null;
  const retryHours = getSemanticRetryHours(finalResponse);
  const scheduledRetryAt = computeScheduledRetryAt(retryHours || 0);
  const finalNotes = `Recording prediction unavailable - finalized using provisional ${finalResponse}${reason ? ` (${reason})` : ''}`;

  db.prepare('UPDATE call_logs SET outcome = ?, ptp_date = ?, notes = ? WHERE id = ?')
    .run(finalResponse, ptpDate, finalNotes, callLogId);

  db.prepare(`UPDATE customers SET final_response = ?, ptp_status = ?, ptp_date = ?, scheduled_retry_at = ?, retry_reason = ?, updated_at = datetime('now') WHERE id = ?`)
    .run(
      finalResponse,
      finalResponse === 'ptp_secured' ? 'pending' : null,
      ptpDate,
      scheduledRetryAt,
      scheduledRetryAt ? `${finalResponse || 'no_outcome'} - ${retryHours}h retry` : null,
      logRow.customer_id
    );

  const conv = db.prepare(
    `SELECT id FROM call_conversations WHERE customer_id = ? ORDER BY updated_at DESC, created_at DESC LIMIT 1`
  ).get(logRow.customer_id);
  if (conv?.id) {
    db.prepare(`UPDATE call_conversations SET status='completed', final_response=?, ptp_date=?, notes=?, schedule_retry_hours=?, updated_at=datetime('now') WHERE id=?`)
      .run(finalResponse, ptpDate, finalNotes, retryHours || 0, conv.id);
  }

  broadcast({
    type: 'CALL_RESULT',
    outcome: finalResponse,
    ptp_date: ptpDate,
    ptp_status: finalResponse === 'ptp_secured' ? 'pending' : null,
    recording_prediction_pending: false,
    finalization_source: 'RECORDING-FALLBACK-PROVISIONAL',
    customer_id: logRow.customer_id,
    call_log_id: callLogId,
  });

  log.warn('TRANSCRIPT', `[RECORDING-FALLBACK] call_log=${callLogId} outcome=${finalResponse || 'null'} ptp=${ptpDate || 'none'}`);
  return { updated: true, final_response: finalResponse, ptp_date: ptpDate };
}

async function reanalyzeStoredCallFromTranscript({ callLogId, transcript, customerName, callerName }) {
  if (!callLogId || !String(transcript || '').trim()) return { updated: false, reason: 'missing_transcript' };

  const parsedMessages = parseTranscriptToMessages(transcript, callerName, customerName);
  if (parsedMessages.length === 0) return { updated: false, reason: 'parse_failed' };

  const logRow = db.prepare('SELECT customer_id, status FROM call_logs WHERE id = ?').get(callLogId);
  if (!logRow?.customer_id) return { updated: false, reason: 'call_log_not_found' };

  const conv = db.prepare(
    `SELECT id, customer_id, customer_name, caller_name
     FROM call_conversations
     WHERE customer_id = ?
     ORDER BY updated_at DESC, created_at DESC
     LIMIT 1`
  ).get(logRow.customer_id) || { customer_id: logRow.customer_id, customer_name: customerName, caller_name: callerName };

  const analysis = finalizeSemanticOutcome(await analyzeConversation(parsedMessages, {
    customer_id: conv.customer_id,
    customer_name: conv.customer_name || customerName,
    caller_name: conv.caller_name || callerName,
  }, {
    forceGeminiOutcomeRules: true,
  }));

  const ptpDateISO = analysis.ptp_date
    ? (() => {
        const d = new Date(analysis.ptp_date);
        return Number.isNaN(d.getTime()) ? null : d.toISOString();
      })()
    : null;
  const scheduledRetryAt = computeScheduledRetryAt(analysis.schedule_retry_hours || 0);

  db.prepare('UPDATE call_logs SET outcome = ?, ptp_date = ?, notes = ?, transcript = ? WHERE id = ?')
    .run(analysis.final_response, ptpDateISO, analysis.notes, transcript, callLogId);

  if (conv?.id) {
    db.prepare(`UPDATE call_conversations SET final_response = ?, ptp_date = ?, notes = ?, schedule_retry_hours = ?, messages = ?, updated_at = datetime('now') WHERE id = ?`)
      .run(analysis.final_response, analysis.ptp_date || null, analysis.notes, analysis.schedule_retry_hours || 0, JSON.stringify(parsedMessages), conv.id);
  }

  db.prepare(`UPDATE customers SET final_response = ?, ptp_status = ?, ptp_date = ?, scheduled_retry_at = ?, retry_reason = ?, updated_at = datetime('now') WHERE id = ?`)
    .run(
      analysis.final_response || null,
      analysis.final_response === 'ptp_secured' ? 'pending' : null,
      ptpDateISO,
      scheduledRetryAt,
      scheduledRetryAt ? `${analysis.final_response || 'no_outcome'} - ${analysis.schedule_retry_hours}h retry` : null,
      logRow.customer_id
    );

  broadcast({
    type: 'CALL_RESULT',
    outcome: analysis.final_response || null,
    ptp_date: analysis.ptp_date || null,
    ptp_status: analysis.final_response === 'ptp_secured' ? 'pending' : null,
    recording_prediction_pending: false,
    finalization_source: 'GEMINI-RECORDING-PRIMARY',
    customer_id: logRow.customer_id,
    call_log_id: callLogId,
  });

  log.success('TRANSCRIPT', `[REANALYZE] call_log=${callLogId} outcome=${analysis.final_response || 'null'} ptp=${analysis.ptp_date || 'none'}`);
  return { updated: true, analysis };
}

async function ensureCallLogTranscriptFromRecording({ callLogId, audioBuffer, mimeType, customerName, callerName, force = false }) {
  if (!callLogId || !audioBuffer?.length) return { updated: false, reason: 'missing_input' };

  const row = db.prepare('SELECT transcript FROM call_logs WHERE id = ?').get(callLogId);
  const existingTranscript = String(row?.transcript || '').trim();
  if (existingTranscript && !force) return { updated: false, reason: 'already_present' };

  const tx = await transcribeRecordingWithGemini(audioBuffer, mimeType, { customerName, callerName });
  if (!tx.ok || !tx.transcript) {
    log.warn('TRANSCRIPT', `Recording transcription skipped for ${callLogId}: ${tx.error || 'unknown error'}`);
    return { updated: false, reason: tx.error || 'transcription_failed' };
  }

  db.prepare('UPDATE call_logs SET transcript = ? WHERE id = ?').run(tx.transcript, callLogId);
  await reanalyzeStoredCallFromTranscript({ callLogId, transcript: tx.transcript, customerName, callerName });
  log.success('TRANSCRIPT', `Transcript generated from recording for call_log ${callLogId}`);
  return { updated: true, transcript: tx.transcript };
}

function parseTranscriptToMessages(transcript = '', callerName = 'Agent', customerName = 'Customer') {
  const safeCaller = String(callerName || 'Agent').trim().toLowerCase();
  const safeCustomer = String(customerName || 'Customer').trim().toLowerCase();
  return String(transcript || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const m = line.match(/^([^:]{1,60}):\s*(.+)$/);
      if (!m) return null;
      const speaker = String(m[1] || '').trim().toLowerCase();
      const text = String(m[2] || '').trim();
      if (!text) return null;
      const role = speaker === safeCaller ? 'agent' : (speaker === safeCustomer ? 'customer' : null);
      if (!role) return null;
      return { role, text, timestamp: new Date().toISOString() };
    })
    .filter(Boolean);
}

async function fetchTranscriptFromTwilioRecordingNow({ callSid, customerName, callerName, maxAttempts = 4 }) {
  const accountSid = (process.env.TWILIO_ACCOUNT_SID || '').replace(/['"]/g, '').trim();
  const authToken = (process.env.TWILIO_AUTH_TOKEN || '').replace(/['"]/g, '').trim();
  if (!callSid || !accountSid || !authToken || callSid === 'pending' || callSid.startsWith('MOCK_')) {
    return { ok: false, reason: 'missing_twilio_context' };
  }

  const authHeader = 'Basic ' + Buffer.from(`${accountSid}:${authToken}`).toString('base64');
  let lastReason = 'recording_not_available';

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      const recRes = await fetch(`https://api.twilio.com/2010-04-01/Accounts/${accountSid}/Calls/${callSid}/Recordings.json`, {
        headers: { Authorization: authHeader },
      });
      if (!recRes.ok) {
        lastReason = `recording_list_http_${recRes.status}`;
      } else {
        const recData = await recRes.json();
        const recordings = Array.isArray(recData?.recordings) ? recData.recordings : [];
        const recording = recordings.find(r => String(r?.status || '').toLowerCase() === 'completed') || recordings[0];
        if (recording?.sid) {
          const downloaded = await fetchTwilioRecordingAudio({ accountSid, authToken, recordingSid: recording.sid });
          if (downloaded?.buffer?.length) {
            const tx = await transcribeRecordingWithGemini(downloaded.buffer, downloaded.mimeType, {
              customerName,
              callerName,
            });
            if (tx.ok && String(tx.transcript || '').trim().length >= 20) {
              return {
                ok: true,
                transcript: tx.transcript,
                recordingSid: recording.sid,
                ext: downloaded.ext,
                mimeType: downloaded.mimeType,
                buffer: downloaded.buffer,
              };
            }
            lastReason = tx.error || 'transcription_failed';
          } else {
            lastReason = 'recording_download_failed';
          }
        } else {
          lastReason = 'recording_sid_missing';
        }
      }
    } catch (err) {
      lastReason = err?.message || 'recording_fetch_exception';
    }

    if (attempt < maxAttempts - 1) {
      const waitMs = attempt === 0 ? 2000 : 3000;
      await new Promise(resolve => setTimeout(resolve, waitMs));
    }
  }

  return { ok: false, reason: lastReason };
}

async function correctCustomerMessagesWithGemini(messages = []) {
  if (!Array.isArray(messages) || messages.length === 0) return messages;
  const customerItems = messages
    .map((m, idx) => ({ idx, role: m?.role, text: String(m?.text || '').trim() }))
    .filter((item) => item.role === 'customer' && item.text);

  if (customerItems.length === 0) return messages;

  const payload = customerItems.slice(0, 40).map((item) => {
    const previousTurn = messages[item.idx - 1];
    const nextTurn = messages[item.idx + 1];
    return {
      index: item.idx,
      text: item.text.slice(0, 320),
      previous_agent: previousTurn?.role === 'agent' ? String(previousTurn.text || '').slice(0, 220) : '',
      next_agent: nextTurn?.role === 'agent' ? String(nextTurn.text || '').slice(0, 220) : '',
    };
  });

  const systemPrompt = `${STRICT_ROMAN_URDU_GUARD}
You are a transcript clean-up engine.
Task: Correct noisy customer ASR text into clean Roman Urdu.
Rules:
- Keep original intent/meaning exactly.
- Do NOT invent new facts.
- Keep each corrected line concise and natural.
- Use adjacent agent lines only to disambiguate broken short replies like dates, yes/no, or payment intent.
- If the customer line is a terse but valid answer (for example "18" after a payment-date question), preserve that meaning in a readable way.
- Output ONLY JSON array of objects: [{"index":number,"corrected":"text"}]`;

  const prompt = `Correct these customer transcript lines and return JSON only:\n${JSON.stringify(payload)}`;
  const ai = await callGeminiJson(prompt, systemPrompt, {
    temperature: 0.1,
    maxTokens: 900,
    retries: 1,
    expectArray: true,
  });
  if (!ai.ok || !Array.isArray(ai.data)) {
    log.warn('CALL', `Customer transcript correction skipped: ${ai.error || 'invalid Gemini JSON output'}`);
    return messages;
  }

  try {
    const parsed = ai.data;

    const correctedByIndex = new Map();
    for (const row of parsed) {
      const idx = Number(row?.index);
      const original = customerItems.find((item) => item.idx === idx)?.text || '';
      const corrected = normalizeTranscriptToRomanUrdu(String(row?.corrected || '').trim());
      if (!Number.isInteger(idx) || !corrected) continue;
      if (!isSafeCustomerCorrection(original, corrected)) {
        log.warn('CALL', `[TRANSCRIPT-GUARD] Rejected unsafe customer correction at index ${idx}: original="${String(original).slice(0, 80)}" corrected="${corrected.slice(0, 80)}"`);
        continue;
      }
      correctedByIndex.set(idx, corrected);
    }

    if (correctedByIndex.size === 0) return messages;

    return messages.map((m, idx) => {
      if (m?.role !== 'customer') return m;
      const corrected = correctedByIndex.get(idx);
      return corrected ? { ...m, text: corrected } : m;
    });
  } catch (err) {
    log.warn('CALL', `Customer transcript correction parse failed: ${err.message}`);
    return messages;
  }
}

function hasCustomerNegativePolarity(text = '') {
  const normalized = normalizeTranscriptToRomanUrdu(String(text || ''))
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  if (!normalized) return false;

  const negativeSignals = [
    /\b(nahi|nhi|nahin|na)\b/,
    /\bmat\b/,
    /\brefus(e|ed|al)\b/,
    /\bpaise\s+nahi\b/,
    /\bpayment\s+nahi\b/,
    /\bde\s+nahi\b/,
    /\bkar\s+nahi\b/,
    /\bnahi\s+de\s+sakta\b/,
    /\bnahi\s+de\s+sakti\b/,
    /\bnahi\s+kar\s+sakta\b/,
    /\bnahi\s+kar\s+sakti\b/,
    /\bpossible\s+nahi\b/,
    /\bho\s+nahi\s+sakta\b/,
    /\bho\s+nahi\s+sakti\b/,
  ];

  return negativeSignals.some((pattern) => pattern.test(normalized));
}

function isSafeCustomerCorrection(originalText = '', correctedText = '') {
  const original = normalizeTranscriptToRomanUrdu(String(originalText || '')).trim();
  const corrected = normalizeTranscriptToRomanUrdu(String(correctedText || '')).trim();

  if (!corrected) return false;
  if (!original) return true;

  const originalRefusal = detectRefusalWithNuance(original).isRefusal;
  const correctedRefusal = detectRefusalWithNuance(corrected).isRefusal;
  const originalNegative = hasCustomerNegativePolarity(original);
  const correctedNegative = hasCustomerNegativePolarity(corrected);
  const originalPaymentDone = detectPaymentDoneSemantic(original);
  const correctedPaymentDone = detectPaymentDoneSemantic(corrected);
  const originalDigits = (original.match(/\b\d{1,2}\b/g) || []).join(',');
  const correctedDigits = (corrected.match(/\b\d{1,2}\b/g) || []).join(',');

  if (originalRefusal !== correctedRefusal) return false;
  if (originalNegative !== correctedNegative) return false;
  if (originalPaymentDone !== correctedPaymentDone) return false;

  // Keep terse numeric customer answers stable; do not let correction change the committed day.
  if (originalDigits && correctedDigits && originalDigits !== correctedDigits) return false;

  return true;
}

function buildTranscriptFromMessages(messages = [], callerName = 'Agent', customerName = 'Customer') {
  if (!Array.isArray(messages) || messages.length === 0) return '';
  const safeCaller = String(callerName || 'Agent').trim() || 'Agent';
  const safeCustomer = String(customerName || 'Customer').trim() || 'Customer';

  return messages
    .map((m) => {
      const role = String(m?.role || '').toLowerCase();
      const text = String(m?.text || '').trim();
      if (!text) return '';
      const speaker = role === 'agent' ? safeCaller : role === 'customer' ? safeCustomer : 'System';
      return `${speaker}: ${text}`;
    })
    .filter(Boolean)
    .join('\n');
}

async function generateFinalTranscriptWithGemini(messages = [], context = {}) {
  const callerName = String(context?.callerName || 'Agent').trim() || 'Agent';
  const customerName = String(context?.customerName || 'Customer').trim() || 'Customer';
  const rawTranscript = buildTranscriptFromMessages(messages, callerName, customerName);
  if (!rawTranscript) return { ok: false, transcript: '', source: 'empty', reason: 'no_messages' };

  // Keep prompt bounded for long calls while still preserving enough context.
  const boundedTranscript = rawTranscript.length > 12000
    ? rawTranscript.slice(rawTranscript.length - 12000)
    : rawTranscript;

  const systemPrompt = `${STRICT_ROMAN_URDU_GUARD}
You are a transcript formatter for debt collection calls.
Task: clean and structure the transcript while preserving original meaning.
Rules:
- Do NOT invent facts.
- Keep speaker labels exactly "${callerName}:" and "${customerName}:".
- Remove obvious ASR junk/noise filler when it does not affect meaning.
- Merge broken fragments into readable full utterances.
- Output plain text only (no markdown, no code fences, no explanation).
- Output one utterance per line: "Speaker: text".`;

  const prompt = `Clean and format this transcript:\n\n${boundedTranscript}`;
  const ai = await callGemini(prompt, systemPrompt, 0.1, 1900);
  if (!ai.ok || !ai.text) {
    return { ok: false, transcript: rawTranscript, source: 'fallback_raw', reason: ai.error || 'gemini_failed' };
  }

  const cleaned = String(ai.text || '')
    .replace(/^```(?:text|markdown)?\s*/i, '')
    .replace(/```\s*$/i, '')
    .trim();

  if (!cleaned || cleaned.length < 20) {
    return { ok: false, transcript: rawTranscript, source: 'fallback_raw', reason: 'gemini_empty_output' };
  }

  const countSpeakerLines = (text = '') => String(text || '')
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(line => line && (line.startsWith(`${callerName}:`) || line.startsWith(`${customerName}:`)))
    .length;

  const rawSpeakerLines = countSpeakerLines(rawTranscript);
  const cleanedSpeakerLines = countSpeakerLines(cleaned);
  if (rawSpeakerLines > 0 && cleanedSpeakerLines > 0) {
    const retentionRatio = cleanedSpeakerLines / rawSpeakerLines;
    if (retentionRatio < 0.75) {
      return { ok: false, transcript: rawTranscript, source: 'fallback_raw', reason: 'gemini_line_drop_guard' };
    }
  } else if (rawSpeakerLines > 0 && cleanedSpeakerLines === 0) {
    return { ok: false, transcript: rawTranscript, source: 'fallback_raw', reason: 'gemini_speaker_guard' };
  }

  return { ok: true, transcript: cleaned, source: 'gemini' };
}

// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
// Gemini-based Date & Outcome Extractor (direct from full transcript)
// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
async function extractDateAndOutcomeFromTranscript(transcript, messages) {
  if (!transcript || !messages || messages.length === 0) {
    return { ok: false, data: null, error: 'Empty transcript or messages' };
  }

  const systemPrompt = `${STRICT_ROMAN_URDU_GUARD}

You are a call analysis expert for Pakistani bank debt collection. Your job is to extract the ACTUAL outcome and payment date from the ACTUAL customer and agent words in the transcript.

RULES:
1. Read the ENTIRE transcript word-by-word.
2. Look at what the CUSTOMER actually said, not what the agent said.
3. Extract two things: final_response (call outcome) and ptp_date (payment commitment date).
4. Do NOT invent or guess. If unclear, return null for that field.
5. Return ONLY valid JSON object: {"final_response":"...", "ptp_date":"YYYY-MM-DD or null", "notes":"short explanation"}

OUTCOMES (pick ONE):
- ptp_secured: Customer committed to pay on a specific date (even if negotiated, if they finally said YES to a date)
- payment_done: Customer claims payment was already made
- callback_requested: Customer asked to call back later (too busy now)
- non_customer_pickup: Wrong person answered
- refused: Customer said NO to payment, no date committed
- negotiation_barrier: Customer asked for extension/installment but no final date
- no_answer: No conversation happened
- busy: Quick busy response, hung up
- switched_off: Phone off or unreachable
- abuse_detected: Customer abused or threatened
- partial_payment: Customer offered to pay only part

DATE EXTRACTION:
- Look for customer words like "kal" (tomorrow), "kl to 15 hai" (tomorrow is 15), "15 ko" (on 15), "pandra april" (15 april), etc.
- Extract as YYYY-MM-DD format. Today is ${new Date().toISOString().slice(0,10)}.
  * "kal" means tomorrow = add 1 day to today
  * A bare Urdu/Roman-Urdu number word from customer like "Unnees" (19), "Bees" (20), "Pandrah" (15), etc. is a date answer when agent just asked about a payment date — treat it as that day in the current or next month
  * If agent then confirmed that number (e.g. "main Unnees April note kar raha hoon"), extract that as the ptp_date
  * Numeric date like "16" or "16 ko" = that day in current month
- Return null if no date mentioned.

CRITICAL RULES:
- If customer/caller explicitly says they are NOT the named customer, such as "main Faraz nahi hoon", "main Salah nahi hoon", "bhai Salah nahi hoon", "main Faraz baat nahi kar raha", "main koi aur hoon", "Faraz available nahi", "wrong person", or "wrong number dial kiya hai", mark non_customer_pickup.
- If agent leaves a message for the real customer after identity mismatch, that is still non_customer_pickup, not callback_requested.
- If caller says "wrong number" repeatedly and then agent says "message de dena" / "dobara try karenge", keep outcome as non_customer_pickup (never callback_requested).
- If customer said "kl 15 hai" AND agent/customer discussed payment date, mark ptp_secured with date 15.
- If customer only said "tomorrow" without specific date, return ptp_secured with default +1 day (2026-04-15).
- If customer said "payment already done" or "paid", mark payment_done.
- If customer said "call me later" or "abhi busy", mark callback_requested.
- If agent asked an either-or date question like "kal ya parson?" and customer only said "okay/ji/theek" WITHOUT choosing one option and WITHOUT a later exact-date confirmation from agent, do NOT mark ptp_secured. In that case use callback_requested with ptp_date=null because date lock nahi hui.
- If customer only said "okay/ji/theek" AFTER agent explicitly confirmed one exact date like "theek hai, main 17 April note kar raha hoon", then mark ptp_secured with that confirmed date.
- BARE DATE WORD RULE: If customer replied with ONLY a number or date word (like "Unnees", "Bees", "Pandrah", "19", "20") in response to agent asking about payment date, AND agent then confirmed that date (e.g. "main Unnees April note kar raha hoon"), mark ptp_secured with that confirmed date. The customer giving a date IS a commitment even without saying "okay".
- NEVER mark payment_done if a date was discussed.
- REFUSAL DETECTION (CRITICAL): Words like "nahi doonga", "nahi dunga", "paise nahi dene", "nahi karunga", "main nahi dunga", "nahi dena" all mean the customer REFUSED to pay. If customer said ANY of these ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â especially in their LATER statements ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â mark as "refused". A single ambiguous word like "okay" or a number like "10" does NOT override an explicit verbal refusal. If the customer's clearest and most recent statements are refusals, the outcome is "refused", NOT "ptp_secured".`;

  const prompt = `Extract the exact outcome and payment date from this full call transcript. Read customer words carefully.

TRANSCRIPT:
${transcript}

Return JSON with exactly: {"final_response":"outcome_here", "ptp_date":"YYYY-MM-DD or null", "notes":"1 line explanation"}`;

  const result = await callGeminiJson(prompt, systemPrompt, {
    temperature: 0.1,
    maxTokens: 600,
    retries: 1,
    expectArray: false,
  });

  if (!result.ok || !result.data) {
    log.warn('TIMING', `[GEMINI-EXTRACT] Failed: ${result.error}`);
    return { ok: false, data: null, error: result.error };
  }

  const extracted = result.data;
  
  // Validate response
  const VALID_RESPONSES = ['ptp_secured', 'no_answer', 'non_customer_pickup', 'switched_off', 'negotiation_barrier', 'refused', 'callback_requested', 'partial_payment', 'payment_done', 'busy', 'abuse_detected'];
  
  if (!VALID_RESPONSES.includes(extracted.final_response)) {
    log.warn('CALL', `[GEMINI-EXTRACT] Invalid outcome: ${extracted.final_response}`);
    extracted.final_response = 'negotiation_barrier'; // Safe fallback
  }
  
  // Validate date format if present
  if (extracted.ptp_date && !/^\d{4}-\d{2}-\d{2}$/.test(extracted.ptp_date)) {
    log.warn('CALL', `[GEMINI-EXTRACT] Invalid date format: ${extracted.ptp_date}`);
    extracted.ptp_date = null;
  }

  log.info('CALL', `[GEMINI-EXTRACT] Outcome: ${extracted.final_response}, Date: ${extracted.ptp_date || 'none'}`);
  return { ok: true, data: extracted, raw: result.raw };
}

async function generateCallSummary(transcript, analysis, conv) {
  if (!transcript || !analysis) return null;
  const prompt = `Summarize this JS Bank Pakistan debt collection call for a supervisor dashboard. Write in English only.

IMPORTANT:
- Summary MUST include both sides of the conversation:
  1) what the AGENT asked/offered/confirmed
  2) what the CUSTOMER replied/committed/refused
- Do NOT summarize only one side.
- Keep it concise but complete.

CALL FACTS:
- Customer: ${conv?.customer_name || 'Unknown'}
- Outcome: ${analysis.final_response || 'unknown'}
- PTP Date: ${analysis.ptp_date || 'none'}

TRANSCRIPT:
${transcript}

Return ONLY this JSON:
{"outcome":"${analysis.final_response || 'unknown'}","ptp_date":"${analysis.ptp_date || null}","summary":"<2-3 sentence overall summary covering BOTH agent and customer>","agent_summary":"<1 sentence: agent side>","customer_summary":"<1 sentence: customer side>","customer_response":"positive|negative|neutral|not_reached","key_customer_statement":"<most important thing customer said>","next_action":"<what should happen next>","call_quality":"successful|partial|failed"}`;

  const result = await callGeminiJson(prompt, STRICT_ROMAN_URDU_GUARD, {
    temperature: 0.2,
    maxTokens: 500,
    retries: 1,
    expectArray: false,
  });

  if (!result.ok || !result.data) {
    log.warn('TIMING', `[CALL-SUMMARY] Failed: ${result.error}`);
    return null;
  }
  log.info('TIMING', `[CALL-SUMMARY] quality=${result.data.call_quality} | next=${result.data.next_action}`);
  return result.data;
}

// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
// Build System Prompt for Gemini Native Audio
// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â

function buildSystemPrompt(conv) {
  const toneMap = {
    polite: 'Bohat izzat aur pyaar se baat karo. Naram lekin professional andaz rakhein.',
    assertive: `SAKHT, BOLD, aur POWERFUL andaz mein baat karo. Tum friendly reminder nahi de rahe ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â tum SERIOUS recovery agent ho. Account ${conv.dpd || 0} din overdue hai ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â yeh CRITICAL stage hai. CONSEQUENCES wazeh sunao: CIBIL credit bureau mein naam report hoga, account FREEZE shuru ho chuka hai, legal recovery proceedings ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â court notice ghar aa sakta hai, 3% monthly penalty har din barh raha hai. Mazeed delay BILKUL bardasht nahi hoga. Extension KHATAM. Sirf IMMEDIATE payment hi bacha sakti hai. Short punchy sentences. Rude mat ho lekin BOHOT SAKHT aur DIRECT zaroor ho.`,
    empathetic: 'Customer ki mushkil samjho, unki situation ka ehsaas dikhao, supportive bano lekin payment ki taraf guide karo.',
  };
  const agentMap = {
    fresh_call: 'Pehli baar call. Introduce karo aur balance batao. PTP lo.',
    broken_promise: 'Customer ne pehle promise toda. Serious reminder. Strict tone rakhein ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â bolo ke pichli commitment miss hui, ab mazeed delay nahi ho sakta.',
    ptp_reminder: 'PTP date aa rahi hai. Yaad dilao ke kal payment due hai.',
    ptp_followup: 'Aaj PTP date hai. Confirm karo payment hui ya nahi. Agar nahi hui to serious follow-up.',
    non_customer: 'Kisi aur ne phone uthaya. Message choro. Financial details SHARE MAT KARO.',
    negotiation: 'Extension maang raha hai. Original date par insist karo. Flexibility SIRF genuine cases mein.',
    escalation: 'High priority. Bohot serious tone. Legal action aur account freeze ka mention karo.',
    general_inquiry: 'General payment inquiry. Professional jawab do.',
  };
  const maxPtpDaysVal = Number(conv.maxPtpDays ?? conv.max_ptp_days ?? 5) || 5;
  const maxPtpDate = dateToNaturalUrdu(new Date(Date.now() + maxPtpDaysVal * 86400000));
  const custGender = normalizeGender(conv.customer_gender || conv.customerGender || 'male', 'male');
  const callerName = conv.caller_name || conv.callerName || 'Omar';
  const callerGender = resolveCallerGender(conv.caller_gender || conv.callerGender, callerName, 'male');
  const honorific = custGender === 'female' ? 'Sahiba' : 'Sahab';
  const sirMadam = custGender === 'female' ? 'Madam' : 'Sir';
  // Agent's own verb forms (based on agent/caller gender)
  const bolVerb = callerGender === 'female' ? 'bol rahi' : 'bol raha';
  const chahVerb = callerGender === 'female' ? 'chahti' : 'chahta';
  const samajhVerb = callerGender === 'female' ? 'samajh gayi' : 'samajh gaya';
  const karunVerb = callerGender === 'female' ? 'karungi' : 'karunga';
  const kartiVerb = callerGender === 'female' ? 'karti' : 'karta';
  const saktiVerb = callerGender === 'female' ? 'sakti' : 'sakta';
  // Customer-directed verb forms (based on customer gender) ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â "aap kar sakte/sakti hain"
  const custSakteVerb = custGender === 'female' ? 'sakti' : 'sakte';
  const custKarVerb = custGender === 'female' ? 'kar sakti' : 'kar sakte';

  return `${STRICT_ROMAN_URDU_GUARD}

IDENTITY: Your name is ${callerName}, a virtual recovery agent for JS Bank Karachi. You are professional, human-like, and advisory.
STRICT SCOPE: You ONLY discuss pending credit card dues.

FINANCIAL CONTEXT:
- Base Dues: ${Number(conv.balance || 0).toLocaleString()} PKR
- Days Past Due: ${conv.dpd || 0}
- Due Date: ${maxPtpDate}
- Penalty Interest: 3% (Say "three percent" or "teen percent")
- Interest Rule: No interest if paid by ${maxPtpDate}
- PTP Status: ${conv.ptp_status || 'None'}
- Follow-ups: ${conv.follow_up_count || 0}

LANGUAGE: EXCLUSIVELY PAKISTANI ROMAN URDU with specific English words.

STRICT LINGUISTIC RULES:
1. DATES: Always use full month names in natural Urdu. Say dates like "Pandra January" or "Bees February". Never use numeric format.
1a. INTERNAL THINKING (CRITICAL): NEVER speak your internal reasoning out loud. Do NOT say things like "Acknowledge and Initiate", "Moving on to consent phase", "Detailing Credit Card Dues", "My next step is", "I've outlined", "Per the flow". These are your PRIVATE thoughts ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â the customer must NEVER hear them. ONLY speak the actual dialogue lines.
2. NUMBERS & DECIMALS: Use "point" for decimals in English (e.g., "two point five percent"). For Urdu, say "teen percent" for 3%.
3. ROUNDING: Always round off amounts in speech. Say "Atharah hazaar" instead of exact decimals.
4. KEYWORDS: Inject these English words naturally: point, ${sirMadam.toLowerCase()}, interest, fees, bank, loan, cash, ensure, payment, credit card.
5. REPETITION: If the customer asks you to repeat or says "kia kaha?", respond with the full sentence in clear Urdu. If audio is unclear, say "Awaaz thori clear nahi aa rahi, ${conv.customer_name} ${honorific}. Kya aap dobara bol sakte hain?" instead of "line clear nahi hai".
6. SCOPE LIMIT: If asked for a loan, account, or other services, respond: "Main sirf aapke pending dues ke baare mein assist kar ${saktiVerb} hoon. Baqi cheezon ke liye aap hamesha JS Bank customer support ko call kar sakte hain ya apni qareebi branch visit kar sakte hain. JS Bank hamesha aapki khidmat ke liye hazir hai."
7. ACCENT: Speak with natural Pakistani Karachi-style accent. Soft, friendly, conversational.
8. FLOW: Use natural fillers like "acha", "theek hai", "bas", "thora sa". Keep sentences short and flowing. NEVER address the customer with standalone "${honorific}" or standalone "${sirMadam}" in mid-conversation. Prefer either their first name (for example "${String(conv.customer_name || 'Customer').split(' ')[0]}") or full form "${conv.customer_name} ${honorific}" only when needed.
9. AVOID: Do not use "Jee" excessively. Use "Ji haan" or "Ji" sparingly. Make speech sound like a real Pakistani bank agent ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â friendly but professional, not robotic.
10. GENDER: Customer is ${custGender}. Addressing style must be natural: use customer name (preferred) or "${conv.customer_name} ${honorific}" when needed, but do NOT use standalone "${honorific}" / "${sirMadam}" as a lone address in mid-conversation. When talking about what the CUSTOMER can do, use "${custSakteVerb}" (e.g., "aap ${custKarVerb} hain"). When talking about what YOU (agent) can do, use "${saktiVerb}" (e.g., "main kar ${saktiVerb} hoon"). You (agent) are ${callerGender} ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â use "${bolVerb}", "${chahVerb}", "${samajhVerb}", "${karunVerb}", "${kartiVerb}", "${saktiVerb}" for YOUR OWN actions consistently.
11. AGENT IDENTITY (CRITICAL): You are a ${callerGender} agent. ${callerGender === 'male' ? 'You are MALE. Speak like a Pakistani man. Use MASCULINE verb forms ALWAYS: "main bol raha hoon", "main ne kaha", "main samajh gaya", "main karunga", "main karta hoon". NEVER use feminine forms like "rahi", "gayi", "karungi", "karti". Your voice, tone, and language must sound like a confident Pakistani man.' : 'You are FEMALE. Speak like a Pakistani woman. Use FEMININE verb forms ALWAYS: "main bol rahi hoon", "main ne kaha", "main samajh gayi", "main karungi", "main karti hoon". NEVER use masculine forms like "raha", "gaya", "karunga", "karta". Your voice, tone, and language must sound like a professional Pakistani woman.'}

Agent Type: ${conv.agent_type} ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â ${agentMap[conv.agent_type] || 'General call.'}
Tone: ${conv.tone} ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â ${toneMap[conv.tone] || 'Professional.'}

STRICT CONVERSATION FLOW:
- TURN 1: Greet naturally ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â introduce yourself first, then confirm identity: "Assalam-o-Alaikum! Main ${callerName} ${bolVerb} hoon JS Bank Karachi se ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â kya main ${conv.customer_name} ${honorific} se baat kar ${saktiVerb} hoon?" STOP AND WAIT.
  * If confirmed (haan/ji/main hoon etc.): briefly acknowledge ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â "JazakAllah ${honorific}" or "Shukriya" ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â then TURN 2.
  * If unclear or hesitant: "Ji ${honorific}, main JS Bank se ${callerName} ${bolVerb} hoon ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â bas ek minute ka kaam tha."
- TURN 2: After response, acknowledge well-being in MAX 3 words. Then: "${sirMadam}, main aapse aapke credit card dues ke baare mein baat karna ${chahVerb} hoon, kya ye munasib waqt hai?" STOP AND WAIT.
- DO NOT REPEAT CONSENT: Once you have asked "kya ye munasib waqt hai?" ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â NEVER ask it again. If customer says "yes/haan/ji/theek", move DIRECTLY to dues discussion. Do NOT re-ask consent.
- TURN 3+: Discuss the dues ${Number(conv.balance || 0).toLocaleString()} PKR and the ${maxPtpDate} deadline. Ensure they know no interest is charged if paid by ${maxPtpDate}.
- DATE FORMAT (CRITICAL): NEVER say dates in numeric format like "2026-04-14". ALWAYS say dates the way a native Urdu speaker would in casual conversation. Use CARDINAL numbers (not ordinals): "Chaudah April", "Pandrah January", "Bees March". For 1st-3rd use: "Pehli", "Doosri", "Teesri". For 4+: "Chaar", "Paanch", "Chhe", "Saat", "Aath", "Nau", "Das", "Gyaarah", "Baarah", "Terah", "Chaudah", "Pandrah", "Solah", "Satrah", "Atharah", "Unnees", "Bees", "Ikkees", "Baais", "Teis", "Chaubees", "Pacchees", "Chabbees", "Sattais", "Atthais", "Untees", "Tees", "Ikattees". NEVER use ordinal suffixes like "vi" (e.g., NEVER say "Chaudahvi" or "Pandrahvi" ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â say "Chaudah" or "Pandrah").

SCENARIO RESPONSES:

1. CUSTOMER COOPERATIVE ("Haan bolien"):
"Shukriya. Aapke credit card par ${Number(conv.balance || 0).toLocaleString()} rupees pending hain. Payment ${maxPtpDate} tak due hai ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â us se pehle koi extra charges nahi. Baad mein teen percent lag sakte hain. Kab tak ${custKarVerb} hain?"

2. HOW TO PAY ("Kaise pay karun?"):
"Theek hai. Main short steps bata ${callerGender === 'female' ? 'deti' : 'deta'} hun. Sb se pehle, JS Bank mobile app ya online banking open karein. Phir credit card section mein jaa kar, pending amount check karein. Amount enter karke confirm karein. Payment complete ho jayegi. Agar kisi step par issue aaye, JS Bank helpline guide kar degi."

3. INSTALLMENTS ("Installments ka option hai?"):
"Ji haan. Installments ka option aapki card eligibility par depend karta hai. Is ke liye, JS Bank helpline ya branch se confirmation hoti hai. Agar aap chahen, main note kar ${saktiVerb} hun ke aap installments mein interested hain."

4. CUSTOMER BUSY ("Abhi busy hoon"):
- FIRST ask for callback time: "Theek hai ${honorific}, koi baat nahi. Aapko kab call karun? 10 minute baad, aadha ghanta baad, ya kisi aur waqt?"
- If customer gives time ("10 min baad", "1 ghante baad", "shaam ko", "2 baje"): "Theek hai, main aapko [time] par dobara call karungi. Shukriya. Allah Hafiz."
- If customer gives NO time and just says bye: "Theek hai. Bas yaad rakhein payment ${maxPtpDate} tak due hai. Main thori dair baad dobara try karungi. Shukriya. Allah Hafiz."
- ALSO try to get PTP date if possible: "Aur ${honorific}, sirf ek baat ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â payment kab tak ${custKarVerb} hain? Bas date bata dein."

5. REFUSES HELP ("Mujhe help nahi chahiye"):
"Theek haiÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ ${samajhVerb}. Bas itna yaad rahe, payment ${maxPtpDate} tak due hai. Us ke baad late charges add ho sakte hain. Shukriya aapke time ka. Allah Hafiz."

6. CUSTOMER LOOPS ("Ji bolien" repeats):
"Ji. Payment ${maxPtpDate} tak due hai. Is ke baad teen percent charges lag sakte hain. Aap payment khud manage kar lenge, ya payment ka tareeqa bata doon?" If repeats again: "Theek hai. Allah Hafiz."

7. PAYMENT DONE ("Payment ho chuki hai"):
"Acha. Shukriya batane ka. Kabhi kabhi system mein update thora late hota hai. Agar payment reflect ho jaye, to koi further action required nahi hota. Shukriya. Allah Hafiz."

8. NON-CUSTOMER PICKUP (Someone else picks up):
- DO NOT discuss any financial details or balance information with them
- Simply say: "Theek hai, koi baat nahi. Kya aap ${conv.customer_name} ${honorific} ko message de sakte hain ke JS Bank se call aayi thi? Hum dobara try karenge. Shukriya. Allah Hafiz."
- If they ask what it's about: "Ye ek routine call thi, hum dobara contact karenge. Shukriya. Allah Hafiz."

PTP DATE COLLECTION (MANDATORY ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â MOST IMPORTANT RULE):
- You MUST get a specific payment date from the customer before ending the call.
- This is NON-NEGOTIABLE. Do NOT end call without getting a date commitment.

5-DAY MAXIMUM ENFORCEMENT (CRITICAL):
- Maximum PTP date: ${maxPtpDaysVal} din (5 days from today). NEVER accept a date beyond this.
- If customer gives ANY date beyond 5 days (for example: "20 April", "next week", "mahine ki pehli"), treat it as LIVE NEGOTIATION ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â NOT as refusal, NOT as goodbye, and NOT as a reason to end the call.
- On the FIRST through FIFTH resistance turns for a beyond-5-day date, NEVER say "Allah Hafiz", NEVER say goodbye, NEVER say "main baad mein dobara call karungi", NEVER say "hum dobara try karenge", and NEVER use [END_CALL]. Stay in the same live call and keep negotiating for an in-window date.
- If customer says "das din baad" or "next week" or "mahine ki pehli" or ANY date beyond 5 days:
  * APPROACH 1 (Incentive): "${honorific}, agar aap ${maxPtpDate} tak payment kar dein to koi extra charges nahi lagenge. Us ke baad teen percent penalty shuru ho jati hai. Aapke liye yahi best hai."
  * APPROACH 2 (Policy): "${honorific}, bank policy ke mutabiq maximum ${maxPtpDaysVal} din ka time de sakte hain. Us se zyada possible nahi hai. Kal ya parson, kab convenient hoga?"
  * APPROACH 3 (Urgency): "${honorific}, account already ${conv.dpd || 0} din overdue hai. Mazeed delay se CIBIL report mein naam aa sakta hai. ${maxPtpDate} tak karna zaroori hai."
- If customer STILL insists on more than 5 days after 2 redirects:
  * FINAL OFFER: "Theek hai, main apni taraf se maximum ${maxPtpDate} tak ki date note kar ${callerGender === 'female' ? 'rahi' : 'raha'} hoon. Yahi last date hai jo system accept karega. Pakka?"
  * If customer refuses even this ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ mark as negotiation_barrier.
  * ONLY after this final offer is rejected may you close politely with a goodbye and [END_CALL].

SMART DATE EXTRACTION:
- When customer mentions ANY time reference, IMMEDIATELY convert to specific date and confirm:
  * "kal" ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ "To kal, matlab [tomorrow's date in Urdu], theek hai?"
  * "do teen din" ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ "Theek hai, to [date 3 days from now], main note kar ${callerGender === 'female' ? 'rahi' : 'raha'} hoon"
  * "is hafte" ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ "To [end of week date], confirm hai?"
  * "jaldi" or vague ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ PROPOSE a date: "Kya parson tak, matlab [date], possible hai?"
- If customer CORRECTS your date, the CUSTOMER'S corrected date ALWAYS wins immediately.
- Example: if you said "Unnees April" and customer says "19 nahi, 17" or "main 17 April ko kar doonga", you MUST acknowledge the correction, update to "Satrah April", and stop insisting on the old date.
- Never repeat your old proposed date after customer correction. Confirm the corrected date only once, briefly.

MULTIPLE NEGOTIATION APPROACHES (use different ones based on customer mood):
- APPROACH A ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â MOUTH-SPEAK (Default): Put date IN customer's mouth. Instead of "kab?", say: "Agar aap kal tak kar dein to koi charges nahi lagenge. Main kal ki date note kar loon?"
- APPROACH B ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â EITHER-OR CHOICE: Give exactly 2 options: "Kal karenge ya parson? Dono mein se jo bhi convenient ho."
- APPROACH C ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â ASSUMPTIVE CLOSE: Act as if date is already decided: "Main aapki madad ke liye [date 2 days] ki date likh ${callerGender === 'female' ? 'rahi' : 'raha'} hoon, theek hai?"
- APPROACH D ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â BENEFIT LOCK: "Agar parson tak ho jaye to main aapka case positive close kar ${saktiVerb} hun, phir koi aur follow-up nahi hoga."
- APPROACH E ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â SOCIAL PROOF: "Zyada tar customers do teen din mein kar lete hain. Aap bhi parson tak ${custKarVerb} hain?"
- If customer says vague timing: LOCK IT DOWN: "Theek hai, to [specific date] pakka? Main record update kar ${callerGender === 'female' ? 'rahi' : 'raha'} hoon."

OFF-TOPIC DURING PTP (CRITICAL):
- If customer tries to change topic DURING PTP negotiation (after you asked for date):
  * "Ji, woh to theek hai. Lekin pehle payment date confirm kar lein ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â kal ya parson?"
  * Do NOT engage with off-topic until date is secured.
  * If customer keeps deflecting: "Bas ek second ${honorific} ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â sirf date bata dein, phir aapka time nahi loon${callerGender === 'female' ? 'gi' : 'ga'}."
- If customer goes completely off-topic 3+ times without giving date: use FINAL attempt: "Main last time pooch ${callerGender === 'female' ? 'rahi' : 'raha'} hoon ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â ${maxPtpDate} tak payment ho jayegi? Haan ya nahi?"
- After final attempt if still no date ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ mark as negotiation_barrier and close politely.

- ALWAYS repeat confirmed date using FULL URDU MONTH NAME (e.g., "Pandra July")
- If customer COMPLETELY refuses to give any date after 3 attempts, note it as negotiation_barrier.
- For BUSY customers: "Bas ek second ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â payment kab tak? Kal? Parson? Sirf date bata dein."
- Exception: payment_done, non_customer_pickup ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â do NOT ask for date in these cases.

9. CALLBACK TIME REQUEST ("Baad mein call karo", "10 min baad", "1 ghante baad"):
- ALWAYS ask for specific callback time: "Theek hai, kab call karun? 10 minute mein, aadha ghanta mein, ya kisi specific waqt?"
- If time given: Confirm: "Main aapko [time] par call karungi. Shukriya. Allah Hafiz." [END_CALL]
- If "kal" or "parson": "Theek hai, main aapko [kal/parson] subah call karungi. Aur sirf payment date bata dein ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â kab tak karenge?"
- IMPORTANT: Even if busy, STILL try to get PTP date before hanging up. One quick ask is enough.
- If customer refuses everything: "Theek hai, main thori dair baad try karungi. Allah Hafiz." [END_CALL]

CLOSING (ONE TIME ONLY): "Shukriya. JS Bank hamesha aapki khidmat ke liye hazir hai. Allah Hafiz." 
- Closing line ko sirf ek dafa bolo. "JS Bank" ya "Allah Hafiz" ko repeat mat karo.

10. OFF-TOPIC / IRRELEVANT CONVERSATION:
Customer may talk about things completely unrelated to payment ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â weather, politics, personal stories, complaints about other things, asking YOUR name/age/location, flirting, testing if you're AI, random questions, etc.

HANDLING RULES:
- REDIRECT LIMIT: You get MAX 3 polite redirects back to payment topic. Count internally.
  * Redirect 1: "${honorific}, main samajh ${saktiVerb} hun. Lekin abhi humein payment ki baat karni hai ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â ${Number(conv.balance || 0).toLocaleString()} rupees pending hain. Kab tak ${custKarVerb} hain?"
  * Redirect 2: "${honorific}, waqt kam hai. Payment ${maxPtpDate} tak due hai. Sirf date bata dein ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â kal ya parson?"
  * Redirect 3 (FINAL): "${conv.customer_name} ${honorific}, main sirf payment ke hawale se baat kar ${saktiVerb} hun. Agar abhi baat nahi ho sakti, main baad mein call kar ${callerGender === 'female' ? 'ungi' : 'unga'}. Shukriya. Allah Hafiz. [END_CALL]"
- After 3rd redirect with no cooperation ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ label as "refused" or "callback_requested" based on tone.

- "KYA AAP AI HO?" / "ROBOT HO?": Confidently say: "Nahi ${honorific}, main JS Bank ki collection team se ${bolVerb} hun. Chalein, payment ki baat karte hain?"
- PERSONAL QUESTIONS (name/age/location): "Main JS Bank se ${callerName} ${bolVerb} hun. ${honorific}, payment ke bare mein baat karte hain."
- RANDOM SILENCE + THEN OFF-TOPIC: Still redirect. Don't engage.
- CUSTOMER DELIBERATELY WASTING TIME: If customer keeps talking nonsense for 3+ turns, politely end: "Lagta hai abhi munasib waqt nahi hai. Main baad mein dobara call karungi. Allah Hafiz. [END_CALL]"
- COMPLAINTS ABOUT BANK/SERVICE: "Main aapki frustration samajh ${saktiVerb} hun. Is ke liye aap JS Bank helpline par complaint register kar sakte hain. Abhi payment ke hawale se ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â kab tak ho jayegi?"
- THREATS (legal action/court/lawyer): Stay calm. "Ji ${honorific}, aapka haq hai. Lekin pending amount ab bhi due hai. Payment ka koi plan bata dein to main madad kar ${saktiVerb} hun."

CRITICAL RULES:
- Always start with opening greeting and WAIT for response
- Match customer's tone and pace
- Keep responses short, natural, conversational (1 sentence MAX per turn ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â NEVER give long paragraphs)
- Use natural Pakistani Urdu pronunciation
- End call cleanly when appropriate ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â include [END_CALL] in response when conversation should end
- Never repeat full introduction if customer loops
- Sound like a real Pakistani call center agent, not a robot
- Speak with warmth and professionalism
- If non-customer picks up: NEVER share financial details, just leave a callback message and end
- Use correct gender verb forms consistently ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â NEVER mix male/female verb forms
- NEVER abruptly end call ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â use the exact one-time closing line once, then stop speaking.

NOISE HANDLING (CRITICAL):
- The customer's phone may have background noise (traffic, TV, wind, crowd, music, children).
- IGNORE all background sounds. Focus ONLY on the customer's spoken words.
- If you hear unclear audio with noise, ask politely: "Sorry ${honorific}, awaz thori clear nahi aa rahi. Kya aap dobara bol sakte hain?"
- Do NOT respond to background conversations, TV dialogue, or random sounds as if the customer spoke.
- If audio is consistently unclear for 3+ turns, suggest: "${honorific}, lagta hai line clear nahi hai. Main thori der baad dobara call karta/karti hoon."
- Never get confused or distracted by ambient sounds ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â stay focused on the debt recovery conversation.
- IMPORTANT: If you only hear noise/static/background sounds with NO clear speech, DO NOT proceed to the next step. Wait for clear customer speech before advancing the conversation.
- NEVER treat background noise as customer consent or agreement. Only proceed when customer clearly says "haan", "ji", "theek hai" etc.`;
}

// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
// Build pre-warm setup config (used during RINGING phase to eliminate post-pickup latency)
// This builds the SAME setup config that setupGeminiHandlers.on('open') would build,
// but does it BEFORE the customer answers so setupComplete arrives during ringing.
// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â

function buildPrewarmSetupConfig({ convData, callAssets, voiceName, custGender, callerGender, systemPrompt, maxPtpDays }) {
  const honor = custGender === 'female' ? 'Sahiba' : 'Sahab';
  const sirMadam = custGender === 'female' ? 'Madam' : 'Sir';
  const bolVerb = callerGender === 'female' ? 'bol rahi' : 'bol raha';
  const chahVerb = callerGender === 'female' ? 'chahti' : 'chahta';
  const samajhVerb = callerGender === 'female' ? 'samajh gayi' : 'samajh gaya';
  const karunVerb = callerGender === 'female' ? 'karungi' : 'karunga';
  const kartiVerb = callerGender === 'female' ? 'karti' : 'karta';
  const saktiVerb = callerGender === 'female' ? 'sakti' : 'sakta';
  const custSakteVerb = custGender === 'female' ? 'sakti' : 'sakte';
  const custKarVerb = custGender === 'female' ? 'kar sakti' : 'kar sakte';
  
  const agentName = convData.caller_name || 'Omar';
  const custName = convData.customer_name || 'customer';
  const amount = String(convData.balance || 0);
  const dpdValue = Number(convData.dpd || 0);
  const followUpValue = Number(convData.follow_up_count || 0);
  const ptpStatusValue = convData.ptp_status || 'None';
  const agentTypeValue = convData.agent_type || 'fresh_call';
  const maxPtpDaysVal = maxPtpDays || 5;
  const maxPtpDateForPrompt = dateToNaturalUrdu(new Date(Date.now() + maxPtpDaysVal * 86400000));
  
  const exactOpeningGreeting = sanitizePreparedText(callAssets?.openingGreeting) || buildExactGreeting({
    customer_name: custName,
    caller_name: agentName,
    customer_gender: custGender,
    caller_gender: callerGender,
  });
  const exactConsentPrompt = `${sirMadam}, main aapse aapke credit card dues ke baare mein baat karna ${chahVerb} hoon, kya ye munasib waqt hai?`;

  // Use buildSystemPrompt for the base prompt (same as live prompt uses)
  const systemText = buildSystemPrompt({
    ...convData,
    customer_gender: custGender,
    caller_gender: callerGender,
    maxPtpDays: maxPtpDaysVal,
  });
  
  let finalSystemText = systemText;
  if (systemPrompt) finalSystemText += `\n\nÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â ADDITIONAL RULES FROM SETTINGS ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â\n${systemPrompt}`;

  return {
    setup: {
      model: `models/${GEMINI_NATIVE_AUDIO_MODEL}`,
      generation_config: {
        response_modalities: ['AUDIO'],
        speech_config: {
          voice_config: {
            prebuilt_voice_config: { voice_name: voiceName }
          }
        }
      },
      safety_settings: [
        { category: 'HARM_CATEGORY_HARASSMENT', threshold: 'OFF' },
        { category: 'HARM_CATEGORY_HATE_SPEECH', threshold: 'OFF' },
        { category: 'HARM_CATEGORY_SEXUALLY_EXPLICIT', threshold: 'OFF' },
        { category: 'HARM_CATEGORY_DANGEROUS_CONTENT', threshold: 'OFF' },
      ],
      system_instruction: {
        parts: [{ text: finalSystemText }]
      },
      input_audio_transcription: {},
      output_audio_transcription: {},
      realtime_input_config: {
        activity_handling: 'START_OF_ACTIVITY_INTERRUPTS',
        automatic_activity_detection: {
          disabled: false,
          prefix_padding_ms: 100,       // Increased from 50ms ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â more robust against noise bursts triggering false speech
          silence_duration_ms: 500,     // Increased from 300ms ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â prevents noise gaps from being interpreted as turn boundaries
        }
      }
    }
  };
}

// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
// PTP Safety Net ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â reusable transcript-based PTP detection
// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â

// Shared transcript normalizer (must stay at module scope because post-call analyzers use it)
function normalizeTranscriptToRomanUrdu(text) {
  let value = String(text || '').trim();
  if (!value) return value;

  const directPhraseMap = new Map([
    ['ÃƒÂ Ã‚Â¸Ã‚Â®ÃƒÂ Ã‚Â¸Ã‚Â±ÃƒÂ Ã‚Â¸Ã‚Â¥ÃƒÂ Ã‚Â¹Ã¢â‚¬Å¡ÃƒÂ Ã‚Â¸Ã‚Â«ÃƒÂ Ã‚Â¸Ã‚Â¥', 'hello'],
    ['ÃƒÂ Ã‚Â¤Ã‚Â¹ÃƒÂ Ã‚Â¥Ã‹â€ ÃƒÂ Ã‚Â¤Ã‚Â²ÃƒÂ Ã‚Â¥Ã¢â‚¬Â¹', 'hello'],
    ['ÃƒÂ Ã‚Â¤Ã‚Â¹ÃƒÂ Ã‚Â¥Ã¢â‚¬Â¡ÃƒÂ Ã‚Â¤Ã‚Â²ÃƒÂ Ã‚Â¥Ã¢â‚¬Â¹', 'hello'],
    ['ÃƒÂ Ã‚Â¤Ã‚Â¹ÃƒÂ Ã‚Â¥Ã¢â‚¬Â¦ÃƒÂ Ã‚Â¤Ã‚Â²ÃƒÂ Ã‚Â¥Ã¢â‚¬Â¹', 'hello'],
    ['ÃƒÂ Ã‚Â¤Ã‚Â¹ÃƒÂ Ã‚Â¤Ã‚Â²ÃƒÂ Ã‚Â¥Ã¢â‚¬Â¹', 'hello'],
    ['Ãƒâ€ºÃ‚ÂÃƒâ€ºÃ…â€™Ãƒâ„¢Ã¢â‚¬Å¾Ãƒâ„¢Ã‹â€ ', 'hello'],
    ['Ãƒâ€ºÃ‚ÂÃƒËœÃ‚Â§Ãƒâ„¢Ã¢â‚¬Å¾Ãƒâ„¢Ã‹â€ ', 'hello'],
    ['Ãƒâ€ºÃ‚ÂÃƒâ„¢Ã¢â‚¬Å¾Ãƒâ„¢Ã‹â€ ', 'hello'],
    ['ÃƒËœÃ‚Â³Ãƒâ„¢Ã¢â‚¬Å¾ÃƒËœÃ‚Â§Ãƒâ„¢Ã¢â‚¬Â¦', 'salaam'],
    ['ÃƒËœÃ‚Â§Ãƒâ„¢Ã¢â‚¬Å¾ÃƒËœÃ‚Â³Ãƒâ„¢Ã¢â‚¬Å¾ÃƒËœÃ‚Â§Ãƒâ„¢Ã¢â‚¬Â¦ ÃƒËœÃ‚Â¹Ãƒâ„¢Ã¢â‚¬Å¾Ãƒâ€ºÃ…â€™ÃƒÅ¡Ã‚Â©Ãƒâ„¢Ã¢â‚¬Â¦', 'assalam o alaikum'],
    ['ÃƒËœÃ‚Â§Ãƒâ„¢Ã¢â‚¬Å¾ÃƒËœÃ‚Â³Ãƒâ„¢Ã¢â‚¬Å¾ÃƒËœÃ‚Â§Ãƒâ„¢Ã¢â‚¬Â¦ ÃƒËœÃ‚Â¹Ãƒâ„¢Ã¢â‚¬Å¾Ãƒâ„¢Ã…Â Ãƒâ„¢Ã†â€™Ãƒâ„¢Ã¢â‚¬Â¦', 'assalam o alaikum'],
    ['ÃƒËœÃ‚Â¬Ãƒâ€ºÃ…â€™', 'ji'],
    ['ÃƒÂ Ã‚Â¤Ã…â€œÃƒÂ Ã‚Â¥Ã¢â€šÂ¬', 'ji'],
    ['ÃƒÂ Ã‚Â¤Ã…â€œÃƒÂ Ã‚Â¥Ã¢â€šÂ¬ ÃƒÂ Ã‚Â¤Ã…â€œÃƒÂ Ã‚Â¥Ã¢â€šÂ¬', 'ji ji'],
    ['ÃƒÂ Ã‚Â¤Ã‚Â¹ÃƒÂ Ã‚Â¤Ã‚Â¾ÃƒÂ Ã‚Â¤Ã‚Â', 'haan'],
    ['ÃƒÂ Ã‚Â¤Ã‚Â¹ÃƒÂ Ã‚Â¤Ã‚Â¾ÃƒÂ Ã‚Â¤Ã¢â‚¬Å¡', 'haan'],
    ['ÃƒÂ Ã‚Â¤Ã‚Â¹ÃƒÂ Ã‚Â¤Ã‚Â¾ÃƒÂ Ã‚Â¤Ã‚ÂÃƒÂ Ã‚Â¤Ã…â€œÃƒÂ Ã‚Â¥Ã¢â€šÂ¬', 'haan ji'],
    ['ÃƒÂ Ã‚Â¤Ã‚Â¹ÃƒÂ Ã‚Â¤Ã‚Â¾ÃƒÂ Ã‚Â¤Ã¢â‚¬Å¡ÃƒÂ Ã‚Â¤Ã…â€œÃƒÂ Ã‚Â¥Ã¢â€šÂ¬', 'haan ji'],
    ['ÃƒÂ Ã‚Â¤Ã‚Â¨ÃƒÂ Ã‚Â¤Ã‚Â¹ÃƒÂ Ã‚Â¥Ã¢â€šÂ¬ÃƒÂ Ã‚Â¤Ã¢â‚¬Å¡', 'nahi'],
    ['ÃƒÂ Ã‚Â¤Ã‚Â ÃƒÂ Ã‚Â¥Ã¢â€šÂ¬ÃƒÂ Ã‚Â¤Ã¢â‚¬Â¢', 'theek'],
    ['ÃƒÂ Ã‚Â¤Ã‚Â ÃƒÂ Ã‚Â¥Ã¢â€šÂ¬ÃƒÂ Ã‚Â¤Ã¢â‚¬Â¢ ÃƒÂ Ã‚Â¤Ã‚Â¹ÃƒÂ Ã‚Â¥Ã‹â€ ', 'theek hai'],
    ['ÃƒÂ Ã‚Â¤Ã¢â‚¬Â¦ÃƒÂ Ã‚Â¤Ã…Â¡ÃƒÂ Ã‚Â¥Ã‚ÂÃƒÂ Ã‚Â¤Ã¢â‚¬ÂºÃƒÂ Ã‚Â¤Ã‚Â¾', 'acha'],
    ['ÃƒÂ Ã‚Â¤Ã‚Â¬ÃƒÂ Ã‚Â¥Ã¢â‚¬Â¹ÃƒÂ Ã‚Â¤Ã‚Â²ÃƒÂ Ã‚Â¤Ã‚Â¿ÃƒÂ Ã‚Â¤Ã‚Â', 'boliye'],
    ['ÃƒÂ Ã‚Â¤Ã‚Â¬ÃƒÂ Ã‚Â¥Ã¢â‚¬Â¹ÃƒÂ Ã‚Â¤Ã‚Â²ÃƒÂ Ã‚Â¥Ã¢â‚¬Â¡ÃƒÂ Ã‚Â¤Ã¢â‚¬Å¡', 'bolen'],
    ['ÃƒÂ Ã‚Â¤Ã¢â‚¬Â¢ÃƒÂ Ã‚Â¥Ã…â€™ÃƒÂ Ã‚Â¤Ã‚Â¨', 'kon'],
    ['ÃƒÂ Ã‚Â¤Ã‚Â¶ÃƒÂ Ã‚Â¥Ã‚ÂÃƒÂ Ã‚Â¤Ã¢â‚¬Â¢ÃƒÂ Ã‚Â¥Ã‚ÂÃƒÂ Ã‚Â¤Ã‚Â°ÃƒÂ Ã‚Â¤Ã‚Â¿ÃƒÂ Ã‚Â¤Ã‚Â¯ÃƒÂ Ã‚Â¤Ã‚Â¾', 'shukriya'],
    ['ÃƒÂ Ã‚Â¤Ã¢â‚¬Â¦ÃƒÂ Ã‚Â¤Ã‚Â²ÃƒÂ Ã‚Â¥Ã‚ÂÃƒÂ Ã‚Â¤Ã‚Â²ÃƒÂ Ã‚Â¤Ã‚Â¾ÃƒÂ Ã‚Â¤Ã‚Â¹', 'Allah'],
    ['ÃƒÂ Ã‚Â¤Ã‚Â¹ÃƒÂ Ã‚Â¤Ã‚Â¾ÃƒÂ Ã‚Â¤Ã‚Â«ÃƒÂ Ã‚Â¤Ã‚Â¼ÃƒÂ Ã‚Â¤Ã‚Â¿ÃƒÂ Ã‚Â¤Ã…â€œÃƒÂ Ã‚Â¤Ã‚Â¼', 'Hafiz'],
    ['ÃƒÂ Ã‚Â¤Ã‚Â¹ÃƒÂ Ã‚Â¤Ã‚Â¾ÃƒÂ Ã‚Â¤Ã‚Â«ÃƒÂ Ã‚Â¤Ã‚Â¿ÃƒÂ Ã‚Â¤Ã…â€œ', 'Hafiz'],
    ['ÃƒÂ Ã‚Â¤Ã‚Â¨ÃƒÂ Ã‚Â¥Ã¢â€šÂ¬', 'ni'],
    ['ÃƒÂ Ã‚Â¸Ã¢â€žÂ¢ÃƒÂ Ã‚Â¸Ã‚ÂµÃƒÂ Ã‚Â¹Ã‹â€ ', 'hello'],
    ['Okay', 'okay'],
  ]);

  for (const [source, target] of directPhraseMap.entries()) {
    value = value.split(source).join(target);
  }

  if (/[\u0900-\u097F]/.test(value)) {
    const map = {
      'ÃƒÂ Ã‚Â¤Ã¢â‚¬Â¦':'a','ÃƒÂ Ã‚Â¤Ã¢â‚¬Â ':'aa','ÃƒÂ Ã‚Â¤Ã¢â‚¬Â¡':'i','ÃƒÂ Ã‚Â¤Ã‹â€ ':'ee','ÃƒÂ Ã‚Â¤Ã¢â‚¬Â°':'u','ÃƒÂ Ã‚Â¤Ã…Â ':'oo','ÃƒÂ Ã‚Â¤Ã‚Â':'e','ÃƒÂ Ã‚Â¤Ã‚Â':'ai','ÃƒÂ Ã‚Â¤Ã¢â‚¬Å“':'o','ÃƒÂ Ã‚Â¤Ã¢â‚¬Â':'au',
      'ÃƒÂ Ã‚Â¤Ã‚Â¾':'a','ÃƒÂ Ã‚Â¤Ã‚Â¿':'i','ÃƒÂ Ã‚Â¥Ã¢â€šÂ¬':'ee','ÃƒÂ Ã‚Â¥Ã‚Â':'u','ÃƒÂ Ã‚Â¥Ã¢â‚¬Å¡':'oo','ÃƒÂ Ã‚Â¥Ã¢â‚¬Â¡':'e','ÃƒÂ Ã‚Â¥Ã‹â€ ':'ai','ÃƒÂ Ã‚Â¥Ã¢â‚¬Â¹':'o','ÃƒÂ Ã‚Â¥Ã…â€™':'au','ÃƒÂ Ã‚Â¤Ã¢â‚¬Å¡':'n','ÃƒÂ Ã‚Â¤Ã†â€™':'h',
      'ÃƒÂ Ã‚Â¤Ã‚Â':'n','ÃƒÂ Ã‚Â¥Ã‚Â':'',
      'ÃƒÂ Ã‚Â¤Ã¢â‚¬Â¢':'k','ÃƒÂ Ã‚Â¤Ã¢â‚¬â€œ':'kh','ÃƒÂ Ã‚Â¤Ã¢â‚¬â€':'g','ÃƒÂ Ã‚Â¤Ã‹Å“':'gh','ÃƒÂ Ã‚Â¤Ã¢â€žÂ¢':'ng','ÃƒÂ Ã‚Â¤Ã…Â¡':'ch','ÃƒÂ Ã‚Â¤Ã¢â‚¬Âº':'chh','ÃƒÂ Ã‚Â¤Ã…â€œ':'j','ÃƒÂ Ã‚Â¤Ã‚Â':'jh','ÃƒÂ Ã‚Â¤Ã…Â¾':'n',
      'ÃƒÂ Ã‚Â¤Ã…Â¸':'t','ÃƒÂ Ã‚Â¤Ã‚Â ':'th','ÃƒÂ Ã‚Â¤Ã‚Â¡':'d','ÃƒÂ Ã‚Â¤Ã‚Â¢':'dh','ÃƒÂ Ã‚Â¤Ã‚Â£':'n','ÃƒÂ Ã‚Â¤Ã‚Â¤':'t','ÃƒÂ Ã‚Â¤Ã‚Â¥':'th','ÃƒÂ Ã‚Â¤Ã‚Â¦':'d','ÃƒÂ Ã‚Â¤Ã‚Â§':'dh','ÃƒÂ Ã‚Â¤Ã‚Â¨':'n',
      'ÃƒÂ Ã‚Â¤Ã‚Âª':'p','ÃƒÂ Ã‚Â¤Ã‚Â«':'ph','ÃƒÂ Ã‚Â¤Ã‚Â¬':'b','ÃƒÂ Ã‚Â¤Ã‚Â­':'bh','ÃƒÂ Ã‚Â¤Ã‚Â®':'m','ÃƒÂ Ã‚Â¤Ã‚Â¯':'y','ÃƒÂ Ã‚Â¤Ã‚Â°':'r','ÃƒÂ Ã‚Â¤Ã‚Â²':'l','ÃƒÂ Ã‚Â¤Ã‚Âµ':'v',
      'ÃƒÂ Ã‚Â¤Ã‚Â¶':'sh','ÃƒÂ Ã‚Â¤Ã‚Â·':'sh','ÃƒÂ Ã‚Â¤Ã‚Â¸':'s','ÃƒÂ Ã‚Â¤Ã‚Â¹':'h','ÃƒÂ Ã‚Â¤Ã¢â‚¬Â¢ÃƒÂ Ã‚Â¤Ã‚Â¼':'q','ÃƒÂ Ã‚Â¤Ã¢â‚¬â€œÃƒÂ Ã‚Â¤Ã‚Â¼':'kh','ÃƒÂ Ã‚Â¤Ã¢â‚¬â€ÃƒÂ Ã‚Â¤Ã‚Â¼':'gh','ÃƒÂ Ã‚Â¤Ã…â€œÃƒÂ Ã‚Â¤Ã‚Â¼':'z','ÃƒÂ Ã‚Â¤Ã‚Â¡ÃƒÂ Ã‚Â¤Ã‚Â¼':'r','ÃƒÂ Ã‚Â¤Ã‚Â¢ÃƒÂ Ã‚Â¤Ã‚Â¼':'rh','ÃƒÂ Ã‚Â¤Ã‚Â«ÃƒÂ Ã‚Â¤Ã‚Â¼':'f',
      'ÃƒÂ Ã‚Â¥Ã‚Â¤':'.','ÃƒÂ Ã‚Â¥Ã‚Â¥':'.','ÃƒÂ Ã‚Â¥Ã‚Â¦':'0','ÃƒÂ Ã‚Â¥Ã‚Â§':'1','ÃƒÂ Ã‚Â¥Ã‚Â¨':'2','ÃƒÂ Ã‚Â¥Ã‚Â©':'3','ÃƒÂ Ã‚Â¥Ã‚Âª':'4','ÃƒÂ Ã‚Â¥Ã‚Â«':'5','ÃƒÂ Ã‚Â¥Ã‚Â¬':'6','ÃƒÂ Ã‚Â¥Ã‚Â­':'7','ÃƒÂ Ã‚Â¥Ã‚Â®':'8','ÃƒÂ Ã‚Â¥Ã‚Â¯':'9','ÃƒÂ Ã‚Â¤Ã¢â‚¬Ëœ':'o','ÃƒÂ Ã‚Â¥Ã¢â‚¬Â°':'o',
    };
    const sortedKeys = Object.keys(map).sort((a, b) => b.length - a.length);
    let result = '';
    let i = 0;
    while (i < value.length) {
      let matched = false;
      for (const key of sortedKeys) {
        if (value.substring(i, i + key.length) === key) {
          result += map[key];
          i += key.length;
          matched = true;
          break;
        }
      }
      if (!matched) {
        result += value[i];
        i++;
      }
    }
    value = result;
  }

  value = value
    .replace(/[\u0E00-\u0E7F]+/g, ' hello ')
    .replace(/[\u0600-\u06FF]+/g, ' ')
    .replace(/[^A-Za-z0-9\s.,!?\-:'"]/g, ' ')
    .replace(/\bhello\s+hello\b/gi, 'hello')
    .replace(/\s+/g, ' ')
    .trim();

  const rawTokens = value.split(/\s+/).filter(Boolean);
  const deduped = [];
  for (const token of rawTokens) {
    const t = token.toLowerCase();
    const last = deduped[deduped.length - 1]?.toLowerCase();
    if (last && last === t) continue;

    if (deduped.length >= 3) {
      const a1 = deduped[deduped.length - 3]?.toLowerCase();
      const b1 = deduped[deduped.length - 2]?.toLowerCase();
      const a2 = deduped[deduped.length - 1]?.toLowerCase();
      if (a1 && b1 && a2 && a1 === a2 && b1 === t) {
        deduped.pop();
        continue;
      }
    }

    deduped.push(token);
  }
  value = deduped.join(' ').replace(/\s+/g, ' ').trim();

  value = value
    .replace(/\bbo\s+le\b/gi, 'bole')
    .replace(/\bhukum\s+kre\b/gi, 'hukam karein')
    .replace(/\bgltee\b/gi, 'galti')
    .replace(/\bmsla\b/gi, 'masla')
    .replace(/\b(shee|shi|sahee|sahii)\b/gi, 'sahi')
    .replace(/\bbat\s+kr\b/gi, 'baat kar')
    .replace(/\bbat\s+kar\b/gi, 'baat kar')
    .replace(/\bkr\s+rhe\b/gi, 'kar rahe')
    .replace(/\bkr\s+rhe\s+hain\b/gi, 'kar rahe hain')
    .replace(/\bkr\s+rhi\b/gi, 'kar rahi')
    .replace(/\bkr\s+rahe\b/gi, 'kar rahe')
    .replace(/\bbaink\b/gi, 'bank')
    .replace(/\bbank\s*pt\b/gi, 'bankrupt')
    .replace(/\bbaink\s*pt\b/gi, 'bankrupt')
    .replace(/\bho\s*gya\b/gi, 'ho gaya')
    .replace(/\bbi\s*ji\b/gi, 'busy')
    .replace(/\bjl\s*dee\b/gi, 'jaldi')
    .replace(/\bkol\b/gi, 'call')
    .replace(/\bkren\b/gi, 'karein')
    .replace(/\bpe\s+kr\b/gi, 'pay kar')
    .replace(/\bkr\s+chu\s*ka\b/gi, 'kar chuka')
    .replace(/\bdee\s+dee\b/gi, 'ji ji')
    .replace(/\ba\s+prail\b/gi, 'april')
    // STT syllable-split repairs ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â Twilio streaming often breaks words mid-phoneme
    .replace(/\bhe\s+lo\b/gi, 'hello')
    .replace(/\btee\s+n\b/gi, 'teen')
    .replace(/\bpai\s+se\b/gi, 'paise')
    .replace(/\bta\s+in\b/gi, 'tain')
    .replace(/\bpoo\s+chh?e?\b/gi, 'puchhe')
    .replace(/\bta\s+im\b/gi, 'taim')
    .replace(/\bna\s+hi\b(?=\s+(?:doonga|dunga|dena|dene|karunga|karungi))/gi, 'nahi')
    .replace(/\bde\s+kh\b/gi, 'dekh')
    .replace(/\bl\s+e\s+t\b/gi, 'let')
    .replace(/\bch\s+he\b/gi, 'chhe')
    .replace(/\bpoo\s+ch\b/gi, 'puch')
    .replace(/\byh\s+aan\b/gi, 'yhaan')
    .replace(/\s+/g, ' ')
    .trim();

  return value;
}

function checkTranscriptForPtp(txLower, messages) {
  const datePatterns = [
    // Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ TOMORROW / NEXT DAY Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
    'kal', 'kl', 'tomorrow', 'tmrw', 'aaj kal',
    
    // Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ DAY AFTER TOMORROW (2 DAYS) Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
    'parson', 'parso', 'parsoon', 'parsson', 'parsun', 'pursun', 'purson', 'purso',
    'person', 'perso', 'persoon', 'prson', 'prso', 'parsan', 'parsn', 'prsn',
    'pasron', 'pason', 'paso', 'parzon', 'parzo', 'parrson', 'parrso',
    'parosn', 'paron', 'paro', 'parsoo', 'parsou', 'parsoan', 'parsoen',
    'paarson', 'paarso', 'paarsoon', 'paerson', 'pareson',
    'parsone', 'parsoni', 'prsoon', 'pirson', 'pirso',
    'parsin', 'parsein', 'parsen', 'parsonn',
    'par son', 'par so', 'per son', 'per so', 'prshon', 'parsho',
    'persion', 'paraon', 'parison', 'parsion',
    
    // Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ 3+ DAYS (TARSON/NARSON) Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
    'tarson', 'tarso', 'tarsoon', 'tarsson', 'tarsun', 'turson', 'turso',
    'tarsn', 'trson', 'trso', 'tarsan', 'trsn', 'tasron', 'tason', 'taso',
    'tarzon', 'tarzo', 'tarrson', 'tarrso', 'tarosn', 'taron', 'taro',
    'tarsoo', 'tarsou', 'tarsoan', 'tarsoen', 'taarson', 'taarso',
    'taarsoon', 'tareson', 'tarsone', 'tarsoni', 'trsoon', 'tirson', 'tirso',
    'tarsin', 'tarsein', 'tarsen', 'tarsonn', 'tersn', 'tersen',
    'tar son', 'tar so', 'ter son', 'taarshon', 'tarsho',
    'narson', 'narso', 'narsoon', 'narsson', 'narsun', 'nurson', 'nurso',
    'narsn', 'nrson', 'nrso', 'narsan', 'nrsn', 'nasron', 'nason', 'naso',
    'narzon', 'narzo', 'narrson', 'narrso', 'narosn', 'naron', 'naro',
    'narsoo', 'narsou', 'narsoan', 'narsoen', 'naarson', 'naarso',
    'naarsoon', 'nareson', 'narsone', 'narsoni', 'nrsoon', 'nirson', 'nirso',
    'narsin', 'narsein', 'narsen', 'narsonn', 'nersn', 'nersen',
    'nar son', 'nar so', 'ner son', 'naarshon', 'narsho',
    
    // Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ DAYS COUNT Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
    'do din', '2 din', 'do din mein', 'doo din',
    'teen din', '3 din', 'tin din', 'teen din mein',
    'char din', '4 din', 'char din mein',
    'panch din', '5 din', 'paanch din', 'panch din mein',
    'chhah din', '6 din', 'chhe din',
    'saat din', '7 din', 'ek hafte',
    'aath din', '8 din',
    'nau din', '9 din',
    'das din', '10 din', 'das din mein',
    
    // Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ WEEK REFERENCES Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
    'agle hafte', 'agla hafte', 'agle week', 'next week',
    'is hafte', 'is week', 'this week',
    'hafte bhar mein', 'ek hafte mein',
    
    // Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ DAY NAMES (ENGLISH) Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
    'monday', 'mon', 'tuesday', 'tue', 'wednesday', 'wed',
    'thursday', 'thu', 'friday', 'fri', 'saturday', 'sat', 'sunday', 'sun',
    
    // Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ DAY NAMES (URDU) Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
    'somwar', 'somvaar', 'somwaar', 'sombar',
    'mangal', 'mangalvar',
    'budh', 'budhwar', 'badhwar',
    'jumerat', 'jumma', 'juma', 'jumma var',
    'hafta', 'hafta var',
    'itwar', 'itwaar',
    
    // Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ MONTH END Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
    'end of month', 'month end', 'mahine ke ant mein', 'mahine ke akher mein',
    'month ke end', 'akhri din', 'last day',
    
    // Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ ORDINAL NUMBERS (Full forms) Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
    'pehli', 'pehla', 'pahla', 'pehli tarikh',
    'doosri', 'doosra', 'doosri tarikh',
    'teesri', 'teesra', 'teesri tarikh',
    'chauthi', 'chautha', 'chautha tarikh',
    'paanchvi', 'paanchva', 'panchvi tarikh',
    'chhai', 'chhavi', 'chhvi',
    'saatvi', 'saatva', 'satvi',
    'aathvi', 'aathva', 'atvi',
    'nauvi', 'nauva', 'navvi',
    'dasvi', 'dasva', 'dahvi',
    'gyaarvi', 'gyaarva', 'gyarahvi', 'gyarahva',
    'baarvi', 'baarva', 'baarah', 'barhvi',
    'tervi', 'terva', 'tervi tarikh',
    'chaudahvi', 'chaudahva', 'chaudah',
    'pandrahvi', 'pandrahva', 'pandrah',
    'solahvi', 'solahva', 'solah',
    'satrahvi', 'satrahva', 'satrah',
    'atharahvi', 'atharahva', 'atharah',
    'unneesvi', 'unneesva', 'unnees',
    'beesvi', 'beesva', 'bees',
    'ikkisvi', 'ikkisva', 'ikkees', 'ik kis',
    'baisvi', 'baisva', 'baais', 'bai is',
    'teisvi', 'teisva', 'teis', 'te is',
    'chaubisvi', 'chaubisva', 'chaubis', 'cha bis',
    'pacchisvi', 'pacchisva', 'pacchis', 'pac chis', 'pachis',
    'chhabbisvi', 'chhabbisva', 'chhabbis', 'chha bis',
    'sattaisvi', 'sattaisva', 'sattais', 'sat tais',
    'atthaisvi', 'atthaisva', 'atthais', 'at thais',
    'untisvi', 'untisva', 'untees', 'un tees', 'anti',
    'teesvi', 'teesva', 'tees',
    'ikattisvi', 'ikattisva', 'ikattis', 'ik attis', 'ikti',
    
    // Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ CARDINAL NUMBERS (SPOKEN FORMS) Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
    'ek', 'ek ko', 'ek tarikh', 'ek tarikh ko',
    'do', 'do ko', 'do tarikh', 'do tarikh ko',
    'teen', 'teen ko', 'teen tarikh', 'teen tarikh ko',
    'char', 'char ko', 'char tarikh', 'char tarikh ko',
    'paanch', 'panch', 'paanch ko', 'panch ko',
    'chhe', 'chhah', 'chhai', 'chhe ko', 'chhah ko',
    'saat', 'saat ko', 'saat tarikh',
    'aath', 'aath ko', 'aath tarikh',
    'nau', 'nau ko', 'nau tarikh',
    'das', 'das ko', 'das tarikh', 'dahh',
    'gyarah', 'gyarah ko', 'gyarah tarikh',
    'barah', 'barah ko', 'barah tarikh',
    'terah', 'terah ko', 'terah tarikh',
    
    // Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ MISSPELLINGS OF BEE/BEES Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
    'bee', 'bee ko', 'bee tarikh',
    'bees', 'bees ko', 'bees tarikh',
    'beez', 'beez ko', 'beez tarikh',
    'bi', 'bi ko', 'bi tarikh',
    'b', 'b ko',
    'beees', 'beeees',
    'bee s', 'bee s ko',
    'bi s', 'bi s ko',
    'be', 'be ko', 'be tarikh',
    'bey', 'bey ko', 'bey tarikh',
    'bei', 'bei ko', 'bei tarikh',
    
    // Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ MONTH NAMES Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
    'january', 'february', 'march', 'april', 'may', 'june',
    'july', 'august', 'september', 'october', 'november', 'december',
    'jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec',
    
    // Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ GENERIC DATE ANCHORS Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
    'tarikh', 'tarik', 'taarikh', 'tareekh', 'ta reekh', 'ta rikh',
    'ko', 'tak', 'tk', 'sa', 'sey',
  ];
  const commitPatterns = [
    'karunga', 'karungi', 'kar doon', 'kar dunga', 'kar dungi',
    'de dunga', 'de dungi', 'kr dunga', 'kr dungi', 'kr doonga', 'kr doongi',
    'bhej dunga', 'bhej dungi', 'ho jayegi', 'haan theek', 'ok theek',
    'daal dunga', 'daal dungi', 'pay kar', 'payment kar',
    'kar doonga', 'de doonga', 'haan haan',
    'bilkul sahi', 'bilkul theek', 'ji bilkul', 'sahi hai',
    'kroonga', 'krungi', 'kronga', 'krlonga', 'krlungi', 'krlunga',
    'tareekh ko', 'tarikh ko', 'taarikh ko', 'tareekh', 'tarikh', 'ta reekh', 'ta rikh',
    'shee hai', 'shi hai', 'sahee', 'sahee hai', // typo variants of sahi hai
  ];

  // Also check customer-only lines for stronger signal
  let customerText = txLower;
  if (messages && messages.length > 0) {
    customerText = messages.filter(m => m.role === 'customer').map(m => m.text).join(' ').toLowerCase();
  }

  // CRITICAL: evaluate PTP primarily from customer speech to avoid agent-proposed dates forcing ptp_secured.
  const sourceText = normalizeTranscriptToRomanUrdu((customerText && customerText.trim()) ? customerText : txLower)
    .toLowerCase()
    .replace(/\b(\d)\s+(\d)(?=\s*(?:ko|tk|tak|tarikh|tarik|taarikh|tareekh|april|may|june|july|march|february|january|august|september|october|november|december|nahi)\b)/g, '$1$2');
  if (detectNonCustomerSemantic(sourceText)) {
    return null;
  }
  const hasDate = datePatterns.some(p => sourceText.includes(p));
  const hasCommit = commitPatterns.some(p => sourceText.includes(p));
  const numericDateMatch = sourceText.match(/(\d{1,2})\s*(ko|tk|tak|tarikh|tarik|taarikh|tareekh|april|may|june|july|march)/);
  const paymentIntent = /\b(payment|pay|paisa|paise|amount|due|dues|jama|de\s+dunga|de\s+dungi|kar\s+dunga|kar\s+dungi|karunga|karungi|commit)\b/.test(sourceText);
  const callbackOnlyIntent = /\b(call\s+later|baad\s+mein\s+call|dobara\s+call|phir\s+call|abhi\s+busy|busy\s+hoon)\b/.test(sourceText);

  // If customer is clearly saying payment is already done, do not force ptp_secured.
  if (detectPaymentDoneSemantic(customerText, txLower)) {
    return null;
  }

  // Date mentions without payment context are often callback scheduling, not PTP.
  if (callbackOnlyIntent && !paymentIntent) {
    return null;
  }

  // Handle terse customer replies like "18" when they are answering a payment-date question
  // and the agent immediately confirms the same date in the next turn.
  if (Array.isArray(messages) && messages.length > 0) {
    const normalizeTurn = (value) => normalizeTranscriptToRomanUrdu(String(value || ''))
      .toLowerCase()
      .replace(/[^a-z0-9\s]/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
    const paymentDatePromptPattern = /\b(payment\s+ki\s+date|payment\s+date|date\s+bata|tarikh\s+bata|tareekh\s+bata|kab\s+tak|kab\s+kar\s+sakte|kab\s+karenge|kis\s+date|kis\s+tarikh|payment\s+kab|due\s+hai.*kab|date\s+note)\b/;
    const agentConfirmationPattern = /\b(theek\s+hai|note\s+kar|note\s+kr|confirm|pakka|shukriya|date\s+note|tarikh\s+note)\b/;

    for (let i = 0; i < messages.length; i++) {
      const current = messages[i];
      if (current?.role !== 'customer') continue;

      const customerTurn = normalizeTurn(current.text);
      const compactDigits = customerTurn.replace(/\s+/g, '');
      if (!/^\d{1,3}$/.test(compactDigits)) continue;

      let numericDay = Number(compactDigits);
      if (!Number.isInteger(numericDay) || numericDay < 1) continue;

      if (numericDay > 31 && compactDigits.length === 3) {
        const lastTwo = Number(compactDigits.slice(-2));
        if (Number.isInteger(lastTwo) && lastTwo >= 1 && lastTwo <= 31) {
          numericDay = lastTwo;
        }
      }
      if (!Number.isInteger(numericDay) || numericDay < 1 || numericDay > 31) continue;

      const previousAgentTurns = messages
        .slice(Math.max(0, i - 4), i)
        .filter((msg) => msg?.role === 'agent')
        .map((msg) => normalizeTurn(msg?.text || ''));
      const nextAgent = messages.slice(i + 1, i + 4).find((msg) => msg?.role === 'agent');
      const previousAgentText = previousAgentTurns.join(' | ');
      const nextAgentText = normalizeTurn(nextAgent?.text || '');

      if (!paymentDatePromptPattern.test(previousAgentText) || !nextAgentText) continue;

      const combinedContext = `${previousAgentText} ${customerTurn} ${nextAgentText} ${txLower}`;
      const extractedDate = smartExtractPtpDate(combinedContext) || smartExtractPtpDate(nextAgentText) || smartExtractPtpDate(txLower);
      if (!extractedDate) continue;

      const agentConfirmsDate = agentConfirmationPattern.test(nextAgentText)
        || nextAgentText.includes(String(numericDay))
        || /\b(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec|kal|kl|parson|parso|tarikh|tareekh)\b/.test(nextAgentText);
      if (!agentConfirmsDate) continue;

      log.warn('CALL', `[PTP-CONTEXT] Bare numeric customer reply "${customerTurn}" (parsed day=${numericDay}) matched payment-date prompt + agent confirmation; classifying as ptp_secured (${extractedDate})`);
      return { final_response: 'ptp_secured', ptp_date: extractedDate, ptp_status: 'pending', notes: 'Customer ne contextual payment date di', schedule_retry_hours: 0 };
    }
  }

  const hasStrongPtpEvidence = hasCommit || (numericDateMatch && paymentIntent) || (hasDate && paymentIntent);

  if (hasStrongPtpEvidence) {
    // CRITICAL BARRIER GUARD: If customer expressed barrier context (installment/extension/waqt chahiye)
    // alongside a commit phrase but WITHOUT a specific near-term date (kal/parson/numeric),
    // suppress ptp_secured so the caller can correctly classify as negotiation_barrier.
    // Exception: if the customer gave a specific near date (e.g. "installment mein 15 ko"), PTP is valid.
    const hasSpecificNearDate = !!numericDateMatch || /\b(kal|kl|parson|parso|parsoon|tarson|narson)\b/.test(sourceText);
    if (detectNegotiationBarrierSemantic(sourceText) && !hasSpecificNearDate) {
      log.info('CALL', `[PTP-GUARD] Barrier context (installment/extension) detected with commit phrase but no specific near date ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â suppressing ptp_secured, returning null for barrier classification`);
      return null;
    }
    log.warn('CALL', `ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â PTP SAFETY NET: customer speech has date/commit patterns ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â overriding to ptp_secured (date=${hasDate}, commit=${hasCommit}, numeric=${!!numericDateMatch})`);
    // Prefer extracting from customer text for accuracy
    const extractedDate = smartExtractPtpDate(sourceText) || smartExtractPtpDate(customerText) || smartExtractPtpDate(txLower);
    return { final_response: 'ptp_secured', ptp_date: extractedDate, ptp_status: 'pending', notes: 'Customer ne payment ka wada kiya', schedule_retry_hours: 0 };
  }
  return null;
}

function detectPaymentDoneSemantic(...chunks) {
  const text = normalizeTranscriptToRomanUrdu(chunks
    .filter(Boolean)
    .join(' ')
  )
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  if (!text) return false;

  // Guard FIRST: reject if ANY date/future context is present
  const dateContextMarkers = /\b(kal|kl|parson|tarson|narson|agle|next|ko|tk|tak|tarikh|tarik|april|may|june|july|march|february|january|do din|teen din|char din|panch din|hafte|week|monday|tuesday|wednesday|thursday|friday|somwar|mangal|budh|jumerat|juma)\b/i;
  if (dateContextMarkers.test(text)) return false; // Date + payment signals = PTP commitment, NOT completed payment

  // Guard: reject if future tense marker is present (egee, unga, ungi, dungi, jaegee, etc.)
  const futureTenseMarkers = /\b(egee|ga|gee|unga|ungi|dunga|dungi|jaega|jaegi|jaegee|jayega|jayegi|jayegee|karunga|karungi|karun|kar(na|unga|ungi|dunga|dungi))\b/i;
  if (futureTenseMarkers.test(text)) return false;

  // Normalize broken STT splits like "pe ment" -> "payment" and "kar chu ka" -> "kar chuka"
  const normalizedText = text
    .replace(/\bpe\s*ment\b/g, 'payment')
    .replace(/\bpay\s*ment\b/g, 'payment')
    .replace(/\bpe\s*kr\b/g, 'pay kar')
    .replace(/\bpay\s*kr\b/g, 'pay kar')
    .replace(/\bkar\s*chu\s*ka\b/g, 'kar chuka')
    .replace(/\bkr\s*chu\s*ka\b/g, 'kar chuka')
    .replace(/\bde\s*chu\s*ka\b/g, 'de chuka');

  // Guard: do not classify future intent as payment_done (legacy, now redundant but kept as safety)
  const futureIntentPatterns = [
    /kal\s+pay/, /pay\s+kar(na|unga|ungi|dunga|dungi)/, /payment\s+kar(na|unga|ungi|dunga|dungi)/,
    /installment/, /qist/, /settlement/, /arrange\s+kar/, /baad\s+mein\s+pay/
  ];
  if (futureIntentPatterns.some((p) => p.test(normalizedText))) return false;

  const paidSignals = [
    /already\s+paid/, /i\s+have\s+paid/, /i\s+already\s+paid/, /payment\s+done/,
    /paid\s+it/, /bill\s+paid/, /dues\s+clear/, /clear\s+kar\s+di/, /clear\s+kr\s+di/,
    /jama\s+kar\s+di/, /payment\s+kar\s+di/, /payment\s+kr\s+di/, /pay\s+kar\s+di/,
    /payment\s+ho\s+chuk[ai]/, /payment\s+already\s+ho\s+gayi/,
    /pay\s+kar\s+chuk[ai]/, /payment\s+kar\s+chuk[ai]/, /de\s+chuk[ai]/,
    /transaction\s+(ho\s+gayi|done)/, /amount\s+deduct\s+ho\s+gaya/, /reference\s+number/,
    // CRITICAL: Only match past-tense completions, NOT fragments
    /payment\s+(ho\s+chuk[ai]|already\s+(done|paid|ho\s+gayi))/, // Past perfect forms only
    /\b(paid|cleared|done)\b.*\b(already|already paid|just now)\b/ // Explicit past reference
  ];

  const proofSignals = [
    /trx/, /transaction\s*id/, /reference\s*(id|number)?/, /receipt/, /screenshot/, /sms\s+aya/
  ];

  const strongPaidClaimSignals = [
    /\b(main|mai|mein|mn|i)\b.*\b(pay|payment|de)\b.*\bkar\b.*\bchuk[ai]\b/, // past completion
    /\b(main|mai|mein|mn|i)\b.*\b(pay|payment)\b.*\bkar\b.*\b(di|diya|chuk[ai])\b/, // explicit past completion
    /\b(main|mai|mein|mn|i)\b.*\bde\s+chuk[ai]\b/, // past completion
    /\balready\b.*\b(pay|payment|paid)\b/, // AND must have "already" explicitly
  ];

  const paidMatchCount = paidSignals.reduce((count, pattern) => count + (pattern.test(normalizedText) ? 1 : 0), 0);
  const hasProofSignal = proofSignals.some((p) => p.test(normalizedText));
  const hasStrongPaidClaim = strongPaidClaimSignals.some((p) => p.test(normalizedText));

  // CRITICAL: Require either a strong claim OR 2+ proof signals (never single weak matches)
  // AND proof signal support (receipt, transaction id, or explicit "already")
  const result = hasStrongPaidClaim || (paidMatchCount >= 2 && (hasProofSignal || normalizedText.includes('already')));
  if (result) {
    log.warn('CALL', `[PAYMENT-DONE] Detected: strong=${hasStrongPaidClaim}, count=${paidMatchCount}, proof=${hasProofSignal}, text sample: ${normalizedText.substring(0, 100)}`);
  }
  return result;
}

function detectCallbackRequestedSemantic(...chunks) {
  const text = normalizeTranscriptToRomanUrdu(chunks
    .filter(Boolean)
    .join(' ')
  )
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  if (!text) return false;

  const normalized = text
    .replace(/\bbi\s*ji\b/g, 'busy')
    .replace(/\bbiji\b/g, 'busy')
    .replace(/\ba\s*dhe\s*gh\s*nte\b/g, 'aadhe ghante')
    .replace(/\bgh\s*nte\b/g, 'ghante')
    .replace(/\bko\s*l\b/g, 'call')
    .replace(/\bkr\s*ta\b/g, 'karta')
    .replace(/\bkr\s*ti\b/g, 'karti');

  const callbackSignals = [
    /\bcall\s+later\b/, /\bdobara\s+call\b/, /\bphir\s+call\b/,
    /\babhi\s+busy\b/, /\bbusy\s+hoon\b/, /\babhi\s+free\s+nahi\b/, /\bthori\s+der\s+baad\b/,
    /\b(10|15|20|30|\d+)\s*(min|minute|minut|mint)\b/, /\b(1|2|3|\d+)\s*(ghanta|ghante|hour|hours)\b/,
    /\bbaad\s+mein\s+(?:call|karna|karo|phone|rabta|baat)\b/,
    /\b(?:call|phone|rabta)\s+(?:baad\s+mein|later)\b/,
  ];

  if (callbackSignals.some((p) => p.test(normalized))) return true;

  const baadMeinContext = /\bbaad\s+mein\b/.test(normalized);
  const callbackContext = /\b(call|phone|dobara|phir|later|busy|free\s+nahi|minute|minut|mint|ghanta|ghante|hour|hours|rabta|baat)\b/.test(normalized);
  return baadMeinContext && callbackContext;
}

function detectNegotiationBarrierSemantic(...chunks) {
  const text = normalizeTranscriptToRomanUrdu(chunks
    .filter(Boolean)
    .join(' ')
  )
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  if (!text) return false;

  // Barrier intent should reflect negotiation constraints, not random vocabulary.
  const barrierSignals = [
    /\binstallment\b/, /\bqist\b/, /\bqisten\b/, /\bkist\b/, /\bkisten\b/, /\bkisst\b/, /\bkissht\b/,
    /\bextension\b/, /\bwaqt\s+chahiye\b/, /\bthora\s+time\b/, /\bmore\s+time\b/, /\bsamay\s+chahiye\b/,
    /\bagle\s+mahine\b/, /\bagla\s+mahina\b/, /\bnext\s+month\b/, /\bmonth\s+end\b/, /\bek\s+mahina\b/,
    /\bsettlement\b/, /\brestructure\b/, /\badjustment\b/,
    /\bkam\s+kar\s+do\b/, /\bkuch\s+kam\s+kar\b/, /\bthoda\s+kam\b/, /\bthora\s+kam\b/,
    /\bpaisa\s+nahi\s+hai\b/, /\bfund\s+nahi\b/, /\bmoney\s+nahi\b/, /\bpaise\s+ki\s+kami\b/, /\bmushkil\b/,
  ];

  return barrierSignals.some((p) => p.test(text));
}

function detectRefusalWithNuance(...chunks) {
  const text = normalizeTranscriptToRomanUrdu(chunks
    .filter(Boolean)
    .join(' ')
  )
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  if (!text) return { isRefusal: false, strength: 0 };

  // Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ ABSOLUTE REFUSAL (Strong refusal) Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
  const absoluteRefusal = [
    /\b(nahi|nhi)\s+de(nunga|ng|ta|ti)\b/, // Will not give
    /\b(nahi|nhi)\s+kar(unga|ungi)\b/, // Will not do
    /\bnahi\s+(hi)\s+dunga\b/, // Never will give
    /\bkabi\s+nahi\b/, // Never/not ever
    /\blevel\s+nahi\b/, // Not at all
    /\bpaise\s+(nahi|nhi)\b/, // No money (definitive)
    /\brefuse\b/, /\brefused\b/, /\brefusal\b/,
    /\b(nahi|nhi)\s+de\s+sakta\b/, /\b(nahi|nhi)\s+de\s+sakti\b/,
  ];

  // Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ CONDITIONAL REFUSAL (Medium refusal) Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
  const conditionalRefusal = [
    /\bagar\s+(nahi|nhi)\b/, // If not
    /\b(nahi|nhi)\s+to\s+nahi\b/, // Not, then not
    /\b(nahi|nhi)\s+kar\s+sakte\b/, // Cannot do
    /\bkar\s+nahi\s+sakte\b/, // Cannot do
    /\bde\s+nahi\s+sakte\b/, // Cannot give
    /\bnahi\s+a\s+sakte\b/, // Cannot come
  ];

  // Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ SOFT REFUSAL (Weak refusal - needs context) Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
  const softRefusal = [
    /\bnahin\b/, /\bnahe\b/, /\bna\s+na\b/, /\bna\s+na\s+na\b/,
    /\b(busy|banda|band|poocha)\b.*\b(nahi|nhi)\b/,
  ];

  const hasAbsoluteRefusal = absoluteRefusal.some((p) => p.test(text));
  const hasConditionalRefusal = conditionalRefusal.some((p) => p.test(text));
  const hasSoftRefusal = softRefusal.some((p) => p.test(text));

  if (hasAbsoluteRefusal) {
    return { isRefusal: true, strength: 3 }; // Strong
  }
  if (hasConditionalRefusal) {
    return { isRefusal: true, strength: 2 }; // Medium
  }
  if (hasSoftRefusal) {
    return { isRefusal: true, strength: 1 }; // Weak
  }

  return { isRefusal: false, strength: 0 };
}

function detectAbusiveLanguageSemantic(...chunks) {
  const text = normalizeTranscriptToRomanUrdu(chunks
    .filter(Boolean)
    .join(' ')
  )
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  if (!text) return { isAbusive: false, severity: 0 };

  // Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ SEVERE ABUSE Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
  const severeAbuse = [
    /\b(bhenchod|bhen\s+chod|madarchod|maa\s+chod|chutiya|chutiay|harami|haramkhor)\b/,
    /\b(fuck|shit|asshole|bastard)\b/,
  ];

  // Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ MODERATE ABUSE Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
  const moderateAbuse = [
    /\b(gandu|gaddu|gande|bakwas|bewkoof|achcha|chal|aaj)\s+(nahi|nhi)\b/,
    /\b(idiot|stupid|fool)\b/,
    /\bturant\s+(band\s+karo|karo)\b/, // Cut the call urgently
    /\b(dekh|sun|suno)\s+na\s+(baat|kahani)\b.*\b(nahi|nhi)\b/, // Listen but won't pay
  ];

  // Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ MILD DISRESPECT Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
  const mildDisrespect = [
    /\b(banda|harkat|jhooth|jhuta|bakwaas)\b/,
    /\bthik\s+nahi\s+hai\b/, // Not okay (dismissive)
  ];

  const hasSevereAbuse = severeAbuse.some((p) => p.test(text));
  const hasModerateAbuse = moderateAbuse.some((p) => p.test(text));
  const hasMildDisrespect = mildDisrespect.some((p) => p.test(text));

  if (hasSevereAbuse) {
    return { isAbusive: true, severity: 3 }; // Severe
  }
  if (hasModerateAbuse) {
    return { isAbusive: true, severity: 2 }; // Moderate
  }
  if (hasMildDisrespect) {
    return { isAbusive: true, severity: 1 }; // Mild
  }

  return { isAbusive: false, severity: 0 };
}

function detectUnreachableNumber(...chunks) {
  const text = normalizeTranscriptToRomanUrdu(chunks
    .filter(Boolean)
    .join(' ')
  )
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  if (!text) return false;

  // Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ SWITCHED OFF / UNREACHABLE PATTERNS Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
  const unreachableSignals = [
    /\b(switched\s+off|switch\s+off|off\s+hai)\b/, /\b(band\s+hai|band\s+ho|band)\b/,
    /\b(out\s+of\s+coverage|no\s+coverage|coverage\s+nahi)\b/,
    /\b(not\s+reachable|reachable\s+nahi|unavailable)\b/,
    /\b(navailable|nave lable|nuvailable)\b/, // STT errors
    /\b(off\s+ho\s+gayi|off\s+ho\s+gaya|switch\s+karo|switched)\b/,
    /\b(invalid\s+number|galat\s+number|wrong\s+num|number\s+nahi)\b/,
    /\b(rang\s+nahi\s+aya|call\s+connect\s+nahi)\b/,
  ];

  return unreachableSignals.some((p) => p.test(text));
}

function analyzePaymentCommitmentQuality(customerText) {
  const text = normalizeTranscriptToRomanUrdu(String(customerText || ''))
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  // Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ ANALYZE COMMITMENT COMPONENTS Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
  const hasCommitPhrase = /\b(kar\s+dunga|kar\s+dungi|de\s+dunga|de\s+dungi|karunga|karungi|hojaegee|hojayega|hojayegi|bilkul)\b/.test(text);
  const hasSpecificDate = /\b(\d{1,2}|\d\s+\d|\d\s+\d\s+\d)\s*(ko|tk|tak|tarikh)\b/.test(text) || /\b(kal|kl|parson|parso|parsoon|tarson|narson)\b/.test(text);
  const hasAmount = /\b(\d+)\s*(aur|or|rupee|rs|thousand|lakh|paisa|paise)\b/.test(text) || /\b(pura|full|sara|sari|amount|paisa)\b/.test(text);
  const hasPaymentIntent = /\b(payment|pay|jama|de|bhej)\b/.test(text);
  
  let qualityScore = 0;
  if (hasCommitPhrase) qualityScore += 30;
  if (hasSpecificDate) qualityScore += 35; // Date is most valuable
  if (hasAmount) qualityScore += 20;
  if (hasPaymentIntent) qualityScore += 15;
  
  // Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ DEDUCT FOR BARRIERS Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
  if (/\b(installment|qist|extension|waqt\s+chahiye)\b/.test(text)) qualityScore -= 15;
  if (/\b(agar|maybe|maybe|probably|shuayad|balke|shayad)\b/.test(text)) qualityScore -= 10; // Conditional
  
  return Math.max(0, Math.min(100, qualityScore)); // 0-100 scale
}

function calculateOutcomeConfidence(result) {
  // Returns 1-100 confidence score for the outcome
  const outcome = result?.final_response;
  const customerMessageCount = result?.customerMessages || 1;
  const hasMultipleSignals = (result?.signalCount || 1) > 1;
  
  let confidence = 60; // Base confidence
  
  // Increase for high-conviction outcomes
  if (outcome === 'ptp_secured' && result?.ptp_date) confidence = 90;
  if (outcome === 'payment_done') confidence = 85;
  if (outcome === 'abuse_detected') confidence = 95;
  if (outcome === 'non_customer_pickup') confidence = 85;
  if (outcome === 'switched_off') confidence = 88;
  
  // Increase with more customer engagement
  if (customerMessageCount >= 3) confidence += 10;
  if (hasMultipleSignals) confidence += 5;
  
  // Decrease for ambiguous outcomes
  if (outcome === 'callback_requested') confidence = 65;
  if (outcome === 'negotiation_barrier') confidence = 70;
  if (outcome === 'refused') confidence = 75;
  
  return Math.min(100, confidence);
}

function buildSemanticSignalMatrix(normalizedCustomer, normalizedTranscript, messages) {
  const hasSpecificNearDate =
    /\b(\d{1,2}|\d\s+\d|\d\s+\d\s+\d)\s*(ko|tk|tak|tarikh|tarik|taarikh|tareekh)\b/.test(normalizedCustomer) ||
    /\b(kal|kl|parson|parso|parsoon|tarson|narson)\b/.test(normalizedCustomer);
  const abuse = detectAbusiveLanguageSemantic(normalizedCustomer, normalizedTranscript);
  const refusal = detectRefusalWithNuance(normalizedCustomer, normalizedTranscript);
  const agentHints = deriveAgentSemanticHints(messages, normalizedTranscript);
  const customerPtp = checkTranscriptForPtp(normalizedTranscript, messages);
  const customerCallback = detectCallbackRequestedSemantic(normalizedCustomer);
  const customerBarrier = detectNegotiationBarrierSemantic(normalizedCustomer);
  const ptpFromAgent = agentHints.ptpFromAgent
    ? {
        final_response: 'ptp_secured',
        ptp_date: agentHints.extractedPtpDate || smartExtractPtpDate(normalizedTranscript),
        ptp_status: 'pending',
        notes: 'Customer ne short reply ke sath agent-confirmed payment date di',
        schedule_retry_hours: 0,
      }
    : null;

  return {
    ptp: customerPtp || ptpFromAgent,
    paymentDone: detectPaymentDoneSemantic(normalizedCustomer, normalizedTranscript),
    callback: customerCallback || agentHints.callbackFromAgent,
    unresolvedForcedChoice: agentHints.unresolvedForcedChoice,
    barrier: customerBarrier,
    unreachable: detectUnreachableNumber(normalizedCustomer, normalizedTranscript),
    partial:
      /\b(partial|aadha|half|thora|kuch\s+amount|kam\s+amount|installment\s+abhi)\b/.test(normalizedCustomer) &&
      /\b(pay|payment|dunga|dungi|karunga|karungi|de\s+dunga|de\s+dungi)\b/.test(normalizedCustomer),
    busyShort:
      /\b(busy|abhi\s+busy|later|baad\s+mein|thora\s+der|dobara\s+call)\b/.test(normalizedCustomer),
    abuse,
    refusal,
    hasSpecificNearDate,
  };
}

function detectNonCustomerSemantic(...chunks) {
  const text = normalizeTranscriptToRomanUrdu(chunks
    .filter(Boolean)
    .join(' ')
  )
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  if (!text) return false;

  // Guard: if customer is discussing payment/date commitment, do not classify as non-customer.
  const paymentOrDateIntent = [
    /\b(ptp|payment|pay|kar\s+dunga|kar\s+dungi|de\s+dunga|de\s+dungi|bhej\s+dunga|bhej\s+dungi)\b/,
    /\b(kal|parson|aaj|aj|tarikh|tareekh|date|din|haft[ea]|mahina|month|april|may|june|july|august|september|october|november|december)\b/,
  ];
  if (paymentOrDateIntent.some((p) => p.test(text))) return false;

  const nonCustomerSignals = [
    /\bwrong\s+number\b/, /\bgalat\s+number\b/, /\bghalat\s+number\b/,
    /\bwrong\s+number\s+(hai|he|hy|tha)\b/,
    /\bwrong\s+number\s+dial\b/,
    /\bwrong\s+number\s+dial\s+kiya\b/,
    /\b(?:w\s*r\s*o\s*n\s*g|r\s*o\s*n\s*g|r\s*o\s*g)\s*(?:n\s*u?\s*m\s*b\s*e\s*r|n\s*n\s*b\s*r|n\s*m\s*b\s*r)\b/,
    /\b(main|mai|mein|mn)\s+(to\s+)?(nahi|nhi)\s+(hun|hoon)\b/,
    /\b(main|mai|mein|mn)\s+koi\s+aur\s+(hun|hoon)\b/,
    /\b(main|mai|mein|mn)\s+faraz\s+(nahi|nhi)\s+(hun|hoon)\b/,
    /\b(main|mai|mein|mn)\s+salah\s+(nahi|nhi)\s+(hun|hoon)\b/,
    /\bbhai\s+[a-z]{2,}\s+(nahi|nhi)\s+(hun|hoon)\b/,
    /\bbhai\s+[a-z]{2,},?\s*(main|mai|mein|mn)\s+(nahi|nhi)\s+(hun|hoon)\b/,
    /\bfaraz\s+baat\s+nahi\s+kar\s+raha\b/,
    /\bmain\s+.*\s+baat\s+nahi\s+kar\s+raha\b/,
    /\bmain\s+.*\s+baat\s+nahi\s+kar\s+rahi\b/,
    /\bwrong\s+person\b/,
    /\bkoi\s+aur\s+bol\s+raha\b/,
    /\bkoi\s+aur\s+bol\s+rahi\b/,
    /\b[a-z]{3,}\s+(nahi|nhi)\s+(hun|hoon)\b/, // e.g., "salah nahi hoon"
    /\b[a-z]\s+[a-z]{2,}\s+(nahi|nhi)\s+(hun|hoon)\b/, // e.g., "s lah nahi hoon"
    /\b(ye|yeh)\s+(nam|naam)\s+nahi\s+(hai\s+)?mera\b/,
    /\bmera\s+(nam|naam)\s+nahi\s+(hai\s+)?(ye|yeh)?\b/,
    /\b(not\s+)?(account\s+holder|customer)\s+nahi\b/,
    /\bye\s+(mera|meri)\s+number\s+nahi\b/,
    /\b(i\s+am\s+not|im\s+not)\b/,
    /\b(woh|wo)\b.*\b(nahi\s+hain|available\s+nahi)\b/,
    /\b(main|mai|mein)\b.*\b(unka|unki|inka|inki)\b/,
    /\b(mujhe\s+nahi\s+pata|nahi\s+jaanta|nahi\s+jaanti)\b/,
    /\b(message\s+nahi\s+de\s+sakta|message\s+nahi\s+de\s+sakti)\b/,
  ];

  return nonCustomerSignals.some((p) => p.test(text));
}

function tokenizeNameForAnalysis(raw) {
  return normalizeTranscriptToRomanUrdu(String(raw || ''))
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .split(' ')
    .map((s) => s.trim())
    .filter((s) => s.length >= 3);
}

function hasIdentityConfirmation(text, expectedFullName) {
  const normalized = normalizeTranscriptToRomanUrdu(String(text || ''))
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  if (!normalized) return false;

  const expectedTokens = tokenizeNameForAnalysis(expectedFullName);
  const expectedFirst = expectedTokens[0] || '';

  const selfConfirmPatterns = [
    /\b(ji|jee|haan|han)?\s*(main|mai|mein)\s+(hi\s+)?(bol\s+raha|bol\s+rahi|baat\s+kar\s+raha|baat\s+kar\s+rahi|hoon|hun)\b/i,
    /\b(ji|jee|haan|han)?\s*(main|mai|mein)\s+bolta\s+hoon\b/i,
    /\b(ji|jee|haan|han)?\s*(main|mai|mein)\s+bol\s+raha\s+hoon\b/i,
    /\b(ji|jee|haan|han)?\s*(main|mai|mein)\s+bol\s+rahi\s+hoon\b/i,
    /\b(yes|yeah)\s+i\s+am\b/i,
  ];

  if (expectedFirst) {
    selfConfirmPatterns.push(new RegExp(`\\b(ji|jee|haan|han)?\\s*(main|mai|mein)\\s+${expectedFirst}\\b`, 'i'));
    selfConfirmPatterns.push(new RegExp(`\\b(yes|yeah)\\s+i\\s+am\\s+${expectedFirst}\\b`, 'i'));
  }

  return selfConfirmPatterns.some((pattern) => pattern.test(normalized));
}

function isLowSignalCustomerTranscript(text = '') {
  const normalized = normalizeTranscriptToRomanUrdu(String(text || ''))
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  if (!normalized) return true;

  const tokens = normalized.split(' ').filter(Boolean);
  if (tokens.length === 0) return true;

  const filler = new Set([
    'ji', 'jee', 'haan', 'han', 'hmm', 'hmmm', 'ok', 'okay', 'theek', 'thik',
    'bilkul', 'sahi', 'acha', 'achha', 'h', 'hm', 'mn', 'main', 'mai', 'mein',
    'a', 'lot', 'of',
  ]);

  const meaningful = tokens.filter((t) => {
    if (/^\d{1,2}$/.test(t)) return false;
    if (filler.has(t)) return false;
    return /[a-z]/.test(t) && t.length >= 3;
  });

  return meaningful.length <= 1 && tokens.length <= 8;
}

function deriveAgentSemanticHints(messages = [], transcript = '') {
  if (!Array.isArray(messages) || messages.length === 0) {
    return {
      ptpFromAgent: false,
      callbackFromAgent: false,
      nonCustomerFromAgent: false,
      extractedPtpDate: null,
      source: 'none',
    };
  }

  const normalize = (value) => normalizeTranscriptToRomanUrdu(String(value || ''))
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  const customerTexts = messages.filter((m) => m.role === 'customer').map((m) => m.text || '');
  const customerCombined = normalize(customerTexts.join(' '));
  const customerHasHardRefusal = /\b(nahi\s+d(oonga|unga|unga|ene|ega|egi)|paise\s+nahi|refuse|nahi\s+karunga|nahi\s+karungi|nahi\s+dunga)\b/.test(customerCombined);
  const customerHasPaymentDone = detectPaymentDoneSemantic(customerCombined);
  const customerCallbackIntent = detectCallbackRequestedSemantic(customerCombined);
  const customerLooksNonCustomer = detectNonCustomerSemantic(customerCombined);
  const customerLowSignal = isLowSignalCustomerTranscript(customerCombined);

  const shortAffirm = /\b(ji|jee|haan|han|hmm|hmmm|theek|thik|ok|okay|bilkul|sahi)\b/;
  let ptpConfirmedByAgent = false;
  let callbackConfirmedByAgent = false;
  let nonCustomerConfirmedByAgent = false;
  let unresolvedForcedChoice = false;
  let ptpDateFromAgent = null;

  for (let i = 0; i < messages.length; i++) {
    const msg = messages[i];
    if (msg?.role !== 'agent') continue;

    const agentText = normalize(msg.text || '');
    if (!agentText) continue;

    const nextCustomer = messages.slice(i + 1, i + 3).find((m) => m?.role === 'customer');
    const nextCustomerText = normalize(nextCustomer?.text || '');
    const hasAffirmAfter = Boolean(nextCustomerText && shortAffirm.test(nextCustomerText) && !/\b(nahi|nhi|not)\b/.test(nextCustomerText));
    const hasNoisyAckAfter = Boolean(
      nextCustomerText && (
        hasAffirmAfter ||
        /^\d{1,2}$/.test(nextCustomerText) ||
        /^(a\s+lot\s+of|lot\s+of|hmm|hmmm|han|haan|ji|jee|ok|okay|theek|thik|bilkul|sahi)$/.test(nextCustomerText)
      )
    );

    const agentDateMention = /\b(kal|kl|parson|parso|tarikh|tareekh|tk|tak|april|may|june|july|august|september|october|november|december|\d{1,2})\b/.test(agentText);
    const agentPtpConfirmPhrase = /\b(theek\s+hai|main\s+yeh\s+note|main\s+is\s+ko\s+note|note\s+kar|confirm|main\s+.*\s+confirm\s+kar|main\s+record\s+update|payment\s+.*\s+tak|aap\s+.*\s+kar\s+sakte)\b/.test(agentText);
    const forcedChoicePrompt = /\b(kal|kl|parson|parso)\b.*\b(ya|or)\b.*\b(kal|kl|parson|parso)\b|\bdono\s+mein\s+se\b|\bjo\s+bhi\s+convenient\s+ho\b/.test(agentText);

    if (
      !customerHasHardRefusal &&
      !customerHasPaymentDone &&
      !customerCallbackIntent &&
      !customerLooksNonCustomer &&
      agentDateMention &&
      agentPtpConfirmPhrase &&
      (hasAffirmAfter || (customerLowSignal && hasNoisyAckAfter))
    ) {
      ptpConfirmedByAgent = true;
      ptpDateFromAgent = ptpDateFromAgent || smartExtractPtpDate(agentText) || smartExtractPtpDate(transcript);
    }

    if (forcedChoicePrompt && hasAffirmAfter) {
      const laterAgentConfirm = messages.slice(i + 1, i + 5)
        .filter((m) => m?.role === 'agent')
        .some((m) => {
          const laterText = normalize(m?.text || '');
          return /\b(theek\s+hai|main\s+.*\s+note|note\s+kar|confirm)\b/.test(laterText)
            && /\b(kal|kl|parson|parso|\d{1,2}|april|may|june|july|august|september|october|november|december)\b/.test(laterText);
        });
      if (!laterAgentConfirm && !ptpConfirmedByAgent) unresolvedForcedChoice = true;
    }

    if (/\b(baad\s+mein|dobara\s+call|later\s+call|busy\s+hain|busy\s+ho|call\s+later)\b/.test(agentText) && hasAffirmAfter) {
      callbackConfirmedByAgent = true;
    }

    if (/\b(galat\s+number|ghalat\s+number|wrong\s+number|aap\s+.*\s+customer\s+nahi|message\s+chhod)\b/.test(agentText) && hasAffirmAfter) {
      nonCustomerConfirmedByAgent = true;
    }
  }

  return {
    ptpFromAgent: ptpConfirmedByAgent,
    callbackFromAgent: callbackConfirmedByAgent,
    nonCustomerFromAgent: nonCustomerConfirmedByAgent,
    unresolvedForcedChoice,
    extractedPtpDate: ptpDateFromAgent,
    source: 'agent_semantic',
  };
}

function analyzeConversationWithSemanticModel(messages, conv, transcript = '') {
  const customerMessageCount = messages.filter((m) => m.role === 'customer' && String(m.text || '').trim()).length;
  if (customerMessageCount === 0) {
    return {
      final_response: null,
      ptp_date: null,
      ptp_status: null,
      notes: 'Customer ne call pick/engage nahi ki, outcome classify nahi kiya gaya',
      schedule_retry_hours: 2,
      semantic_winner: 'no_pickup_no_outcome',
      semantic_confidence: 98,
    };
  }

  const customerOnlyText = messages
    .filter((m) => m.role === 'customer')
    .map((m) => m.text)
    .join(' ');

  const normalizedCustomer = normalizeTranscriptToRomanUrdu(customerOnlyText).toLowerCase();
  const normalizedTranscript = normalizeTranscriptToRomanUrdu(transcript).toLowerCase();
  const identityConfirmed = hasIdentityConfirmation(customerOnlyText, conv?.customer_name || '');

  const signals = buildSemanticSignalMatrix(normalizedCustomer, normalizedTranscript, messages);
  const signalCount = [
    !!signals.ptp,
    signals.paymentDone,
    signals.callback,
    signals.barrier,
    signals.unreachable,
    signals.partial,
    signals.busyShort,
    signals.abuse.isAbusive,
    signals.refusal.isRefusal,
  ].filter(Boolean).length;

  const withConfidence = (result) => ({
    ...result,
    customerMessages: customerMessageCount,
    signalCount,
    semantic_confidence: calculateOutcomeConfidence({ ...result, customerMessages: customerMessageCount, signalCount }),
  });

  if (signals.abuse.isAbusive && signals.abuse.severity >= 2) {
    return withConfidence({
      final_response: 'abuse_detected',
      ptp_date: null,
      ptp_status: null,
      notes: 'Customer ne abusive language use ki',
      schedule_retry_hours: 24,
      semantic_winner: 'semantic_abuse_priority',
    });
  }

  if (signals.unreachable && customerMessageCount <= 1) {
    return withConfidence({
      final_response: 'switched_off',
      ptp_date: null,
      ptp_status: null,
      notes: 'Number switched off ya unreachable signal mila',
      schedule_retry_hours: 2,
      semantic_winner: 'semantic_switched_off_priority',
    });
  }

  const strictNonCustomer = detectNonCustomerSemantic(normalizedCustomer) && !identityConfirmed;
  if (strictNonCustomer) {
    return withConfidence({
      final_response: 'non_customer_pickup',
      ptp_date: null,
      ptp_status: null,
      notes: 'Customer account holder line par nahi tha',
      schedule_retry_hours: 5,
      semantic_winner: 'semantic_non_customer',
    });
  }

  if (signals.paymentDone) {
    return withConfidence({
      final_response: 'payment_done',
      ptp_date: null,
      ptp_status: null,
      notes: 'Customer ne payment already done confirm ki',
      schedule_retry_hours: 0,
      semantic_winner: 'semantic_payment_done',
    });
  }

  // Barrier before PTP only when no concrete near date exists.
  if (signals.barrier && !signals.hasSpecificNearDate) {
    return withConfidence({
      final_response: 'negotiation_barrier',
      ptp_date: null,
      ptp_status: null,
      notes: 'Customer ne extension/installment maanga, specific date nahi di',
      schedule_retry_hours: 5,
      semantic_winner: 'semantic_negotiation_barrier_early',
    });
  }

  if (signals.ptp) {
    const commitmentQuality = analyzePaymentCommitmentQuality(customerOnlyText);
    const result = withConfidence({
      ...signals.ptp,
      semantic_winner: 'semantic_ptp',
    });
    result.semantic_confidence = Math.max(result.semantic_confidence, Math.min(99, 72 + Math.floor(commitmentQuality / 3)));
    return result;
  }

  if (signals.refusal.isRefusal && signals.refusal.strength >= 2) {
    const result = withConfidence({
      final_response: 'refused',
      ptp_date: null,
      ptp_status: null,
      notes: 'Customer ne payment se inkaar kiya',
      schedule_retry_hours: 24,
      semantic_winner: 'semantic_refusal_nuanced',
    });
    result.semantic_confidence = Math.min(99, Math.max(result.semantic_confidence, 72 + (signals.refusal.strength * 8)));
    return result;
  }

  if (signals.partial) {
    return withConfidence({
      final_response: 'partial_payment',
      ptp_date: null,
      ptp_status: null,
      notes: 'Customer ne partial payment ka signal diya',
      schedule_retry_hours: 2,
      semantic_winner: 'semantic_partial_payment',
    });
  }

  if (signals.unresolvedForcedChoice) {
    return withConfidence({
      final_response: 'callback_requested',
      ptp_date: null,
      ptp_status: null,
      notes: 'Agent ne kal ya parson choice di lekin customer ne exact date lock nahi ki',
      schedule_retry_hours: 2,
      semantic_winner: 'semantic_forced_choice_unresolved',
    });
  }

  if (signals.callback) {
    return withConfidence({
      final_response: 'callback_requested',
      ptp_date: null,
      ptp_status: null,
      notes: 'Customer ne callback/later request diya',
      schedule_retry_hours: 2,
      semantic_winner: 'semantic_callback',
    });
  }

  if (signals.barrier) {
    return withConfidence({
      final_response: 'negotiation_barrier',
      ptp_date: null,
      ptp_status: null,
      notes: 'Customer ne installment/extension context mention kiya',
      schedule_retry_hours: 5,
      semantic_winner: 'semantic_negotiation_barrier',
    });
  }

  if (signals.busyShort && customerMessageCount <= 2) {
    return withConfidence({
      final_response: 'busy',
      ptp_date: null,
      ptp_status: null,
      notes: 'Customer busy tha, short interaction hui',
      schedule_retry_hours: 2,
      semantic_winner: 'semantic_busy',
    });
  }

  if (signals.refusal.isRefusal) {
    return withConfidence({
      final_response: 'refused',
      ptp_date: null,
      ptp_status: null,
      notes: 'Customer ne payment se inkaar kiya',
      schedule_retry_hours: 24,
      semantic_winner: 'semantic_refusal_soft',
    });
  }

  if (signals.unreachable) {
    return withConfidence({
      final_response: 'switched_off',
      ptp_date: null,
      ptp_status: null,
      notes: 'Number switched off ya unreachable signal mila',
      schedule_retry_hours: 2,
      semantic_winner: 'semantic_switched_off_fallback',
    });
  }

  return withConfidence({
    final_response: 'callback_requested',
    ptp_date: null,
    ptp_status: null,
    notes: 'Outcome ambiguous tha, safe callback classify kiya gaya',
    schedule_retry_hours: 2,
    semantic_winner: 'semantic_ambiguous_safe_callback',
  });
}
const SEMANTIC_VALID_OUTCOMES = new Set([
  'ptp_secured', 'non_customer_pickup', 'switched_off', 'negotiation_barrier', 'refused',
  'callback_requested', 'partial_payment', 'payment_done', 'busy', 'abuse_detected', 'no_answer',
]);

const SEMANTIC_RETRY_HOURS_DEFAULTS = {
  no_answer: 2,
  switched_off: 2,
  non_customer_pickup: 5,
  callback_requested: 2,
  busy: 2,
  refused: 24,
  negotiation_barrier: 5,
  abuse_detected: 24,
  partial_payment: 2,
  ptp_secured: 0,
  payment_done: 0,
};

/** Live lookup: merges hardcoded defaults with operator-configured retry hours */
function getSemanticRetryHours(outcome) {
  const cfg = getRetryConfig();
  const overrides = {
    no_answer: cfg.retryNoAnswerHours,
    switched_off: cfg.retryNoAnswerHours,
    non_customer_pickup: cfg.retryNonCustomerHours,
    callback_requested: cfg.retryCallbackHours,
    busy: cfg.retryCallbackHours,
    refused: cfg.retryRefusedHours,
    partial_payment: cfg.retryCallbackHours,
  };
  return overrides[outcome] ?? SEMANTIC_RETRY_HOURS_DEFAULTS[outcome] ?? 2;
}

// Keep a plain alias for backward-compatible reads (non-configured outcomes)
const SEMANTIC_RETRY_HOURS = SEMANTIC_RETRY_HOURS_DEFAULTS;

function normalizeRomanUrduOutputText(value = '') {
  const normalized = normalizeTranscriptToRomanUrdu(String(value || ''))
    .replace(/[^A-Za-z0-9\s.,!?':\-]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  return normalized || 'Roman Urdu note unavailable';
}

function finalizeSemanticOutcome(result = {}) {
  const normalized = {
    ...result,
    final_response: result?.final_response ?? null,
    ptp_date: result?.ptp_date ?? null,
    ptp_status: result?.ptp_status ?? null,
    notes: normalizeRomanUrduOutputText(result?.notes || ''),
  };

  if (normalized.final_response !== null && !SEMANTIC_VALID_OUTCOMES.has(normalized.final_response)) {
    normalized.final_response = null;
  }

  if (normalized.final_response !== 'ptp_secured') {
    normalized.ptp_date = null;
    normalized.ptp_status = null;
  } else {
    normalized.ptp_status = 'pending';
    if (!normalized.ptp_date) {
      normalized.ptp_date = smartExtractPtpDate(normalized.notes || '') || new Date(Date.now() + 2 * 86400000).toISOString().split('T')[0];
    }
  }

  if (normalized.final_response === null) {
    normalized.schedule_retry_hours = typeof normalized.schedule_retry_hours === 'number' ? normalized.schedule_retry_hours : 2;
    normalized.outcome_classified = false;
  } else {
    normalized.schedule_retry_hours = getSemanticRetryHours(normalized.final_response);
    normalized.outcome_classified = true;
  }

  if (typeof normalized.semantic_confidence !== 'number' || Number.isNaN(normalized.semantic_confidence)) {
    normalized.semantic_confidence = 60;
  }
  if (normalized.final_response === null) {
    normalized.semantic_confidence = Math.max(1, Math.min(99, Math.round(normalized.semantic_confidence)));
  } else {
    // Operational target: keep classified outcomes in high-confidence band.
    normalized.semantic_confidence = Math.max(75, Math.min(99, Math.round(normalized.semantic_confidence)));
  }

  return normalized;
}

// Analyze conversation (post-call)
export async function analyzeConversation(messages, conv, options = {}) {
  const forceGeminiOutcomeRules = options?.forceGeminiOutcomeRules === true;
  const transcript = messages.map((m) => `${m.role}: ${m.text}`).join(' \n');
  const customerOnlyText = messages.filter((m) => m.role === 'customer').map((m) => m.text || '').join(' ');
  const identityConfirmed = hasIdentityConfirmation(customerOnlyText, conv?.customer_name || '');
  const explicitNonCustomerEvidence = detectNonCustomerSemantic(customerOnlyText);
  const explicitCallbackEvidence = detectCallbackRequestedSemantic(customerOnlyText);
  const explicitPaymentDoneEvidence = detectPaymentDoneSemantic(customerOnlyText);
  const explicitRefusalEvidence = /\b(nahi\s+d(oonga|unga|oongi|ungi)|nahi\s+kar(unga|ungi)|refuse|no\s+payment)\b/.test(
    normalizeTranscriptToRomanUrdu(customerOnlyText).toLowerCase()
  );

  if (USE_SEMANTIC_OUTCOME_MODEL && !forceGeminiOutcomeRules) {
    const semanticResult = finalizeSemanticOutcome(analyzeConversationWithSemanticModel(messages, conv, transcript));
    if (explicitNonCustomerEvidence && !identityConfirmed && semanticResult.final_response !== 'non_customer_pickup') {
      semanticResult.final_response = 'non_customer_pickup';
      semanticResult.ptp_date = null;
      semanticResult.ptp_status = null;
      semanticResult.schedule_retry_hours = SEMANTIC_RETRY_HOURS.non_customer_pickup;
      semanticResult.notes = 'Customer account holder line par nahi tha (explicit non-customer evidence)';
      semanticResult.semantic_winner = 'semantic_non_customer_override';
      semanticResult.semantic_confidence = Math.max(semanticResult.semantic_confidence || 60, 88);
    }
    log.success('CALL', `[ANALYSIS-SEMANTIC] outcome=${semanticResult.final_response || 'null'}, date=${semanticResult.ptp_date || 'none'}, winner=${semanticResult.semantic_winner || 'semantic'}, conf=${semanticResult.semantic_confidence}`);
    return semanticResult;
  }

  if (forceGeminiOutcomeRules) {
    log.info('CALL', '[ANALYSIS] forceGeminiOutcomeRules enabled - using Gemini extraction rules for this prediction');
  }

  const geminiExtract = await extractDateAndOutcomeFromTranscript(transcript, messages);
  if (geminiExtract.ok && geminiExtract.data) {
    const normalized = finalizeSemanticOutcome(geminiExtract.data);
    const agentHints = deriveAgentSemanticHints(messages, transcript);
    const lowSignalCustomer = isLowSignalCustomerTranscript(customerOnlyText);

    if (explicitNonCustomerEvidence && !identityConfirmed && normalized.final_response !== 'non_customer_pickup') {
      normalized.final_response = 'non_customer_pickup';
      normalized.ptp_date = null;
      normalized.ptp_status = null;
      normalized.schedule_retry_hours = SEMANTIC_RETRY_HOURS.non_customer_pickup;
      normalized.notes = 'Customer account holder line par nahi tha (explicit non-customer evidence)';
      normalized.semantic_winner = 'gemini_non_customer_override';
      normalized.semantic_confidence = Math.max(normalized.semantic_confidence || 60, 86);
    }

    if (
      !explicitNonCustomerEvidence &&
      !explicitCallbackEvidence &&
      !explicitRefusalEvidence &&
      !explicitPaymentDoneEvidence &&
      lowSignalCustomer &&
      agentHints.ptpFromAgent &&
      ['callback_requested', 'busy', null].includes(normalized.final_response)
    ) {
      normalized.final_response = 'ptp_secured';
      normalized.ptp_date = agentHints.extractedPtpDate || smartExtractPtpDate(customerOnlyText || transcript);
      normalized.ptp_status = 'pending';
      normalized.schedule_retry_hours = getSemanticRetryHours('ptp_secured');
      normalized.notes = 'Customer transcript low-signal tha, agent-confirmed PTP date ko final maana gaya';
      normalized.semantic_winner = 'gemini_agent_confirmed_ptp_override';
      normalized.semantic_confidence = Math.max(normalized.semantic_confidence || 60, 82);
    }

    if (normalized.final_response === 'ptp_secured') {
      const extractedDate = smartExtractPtpDate(customerOnlyText || transcript);
      if (extractedDate) normalized.ptp_date = extractedDate;
      if (!normalized.ptp_date) {
        normalized.ptp_date = new Date(Date.now() + 2 * 86400000).toISOString().split('T')[0];
      }

      const today = new Date();
      const maxDate = new Date(Date.now() + 5 * 86400000);
      const ptpDateObj = new Date(normalized.ptp_date);
      if (ptpDateObj > maxDate) normalized.ptp_date = maxDate.toISOString().split('T')[0];
      if (ptpDateObj < today) normalized.ptp_date = new Date(Date.now() + 1 * 86400000).toISOString().split('T')[0];
      normalized.ptp_status = 'pending';
      normalized.semantic_confidence = Math.max(normalized.semantic_confidence || 60, 75);
    }

    log.info('TIMING', `[ANALYSIS-GEMINI] outcome=${normalized.final_response || 'null'}, date=${normalized.ptp_date || 'none'}, winner=gemini_extract, conf=${normalized.semantic_confidence}`);
    return normalized;
  }

  const fallback = finalizeSemanticOutcome(analyzeConversationWithSemanticModel(messages, conv, transcript));
  fallback.semantic_winner = 'gemini_failed_semantic_fallback';
  fallback.semantic_confidence = Math.max(fallback.semantic_confidence || 60, 62);
  log.warn('TIMING', `[ANALYSIS-FALLBACK] Gemini extraction failed (error: ${geminiExtract.error || 'unknown'}), using semantic fallback. outcome=${fallback.final_response || 'null'}`);
  return fallback;
}

function buildPredictionSnapshot(source, analysis = null, extra = {}) {
  if (!analysis) {
    return {
      source,
      available: false,
      outcome: null,
      ptp_date: null,
      ptp_status: null,
      notes: null,
      retry_in_hours: null,
      semantic_winner: null,
      semantic_confidence: null,
      ...extra,
    };
  }

  return {
    source,
    available: true,
    outcome: analysis.final_response || null,
    ptp_date: analysis.ptp_date || null,
    ptp_status: analysis.ptp_status || null,
    notes: sanitizeLogText(analysis.notes || '') || null,
    retry_in_hours: analysis.schedule_retry_hours || null,
    semantic_winner: analysis.semantic_winner || null,
    semantic_confidence: typeof analysis.semantic_confidence === 'number' ? analysis.semantic_confidence : null,
    ...extra,
  };
}

function formatPredictionSnapshotForLog(label, snapshot) {
  if (!snapshot || snapshot.available === false) {
    return `[${label}] unavailable | reason=${snapshot?.reason || 'unknown'} | source=${snapshot?.transcript_source || 'none'}`;
  }

  return [
    `[${label}]`,
    `outcome=${snapshot.outcome || 'null'}`,
    `ptp_date=${snapshot.ptp_date || 'none'}`,
    `ptp_status=${snapshot.ptp_status || 'none'}`,
    `retry=${snapshot.retry_in_hours ?? 'none'}h`,
    `winner=${snapshot.semantic_winner || 'n/a'}`,
    `confidence=${snapshot.semantic_confidence ?? 'n/a'}`,
    `source=${snapshot.transcript_source || snapshot.source || 'unknown'}`,
    `messages=${snapshot.message_count ?? 'n/a'}`,
    `transcript_length=${snapshot.transcript_length ?? 'n/a'}`,
    `semantic_commitment_score=${snapshot.semantic_commitment_score ?? 'n/a'}`,
    `semantic_date_hint=${snapshot.semantic_date_hint || 'none'}`,
    `notes=${sanitizeLogText(snapshot.notes || '') || 'none'}`,
  ].join(' | ');
}

function analyzeConversationWithSemanticPrediction(messages, conv, transcript = '') {
  if (!Array.isArray(messages) || messages.length === 0) return null;

  const customerOnlyText = messages.filter((m) => m.role === 'customer').map((m) => m.text || '').join(' ');
  const identityConfirmed = hasIdentityConfirmation(customerOnlyText, conv?.customer_name || '');
  const explicitNonCustomerEvidence = detectNonCustomerSemantic(customerOnlyText);
  const semanticResult = finalizeSemanticOutcome(analyzeConversationWithSemanticModel(messages, conv, transcript));

  if (explicitNonCustomerEvidence && !identityConfirmed && semanticResult.final_response !== 'non_customer_pickup') {
    semanticResult.final_response = 'non_customer_pickup';
    semanticResult.ptp_date = null;
    semanticResult.ptp_status = null;
    semanticResult.schedule_retry_hours = getSemanticRetryHours('non_customer_pickup');
    semanticResult.notes = 'Customer account holder line par nahi tha (explicit non-customer evidence)';
    semanticResult.semantic_winner = 'semantic_non_customer_override';
    semanticResult.semantic_confidence = Math.max(semanticResult.semantic_confidence || 60, 88);
  }

  semanticResult.notes = sanitizeLogText(semanticResult.notes || '') || null;
  return semanticResult;
}

async function predictOutcomeFromRecording({ recordingUrl = null, callSid = null, conv = null }) {
  const customerName = conv?.customer_name || 'Customer';
  const callerName = conv?.caller_name || 'Agent';

  let transcript = '';
  let parsedMessages = [];
  let transcriptSource = null;

  const localRecordingPath = getLocalRecordingPath(recordingUrl || '');
  if (localRecordingPath && existsSync(localRecordingPath)) {
    try {
      const buffer = readFileSync(localRecordingPath);
      const mimeType = getMimeTypeFromRecordingExt(getRecordingExtFromUrl(localRecordingPath));
      const tx = await transcribeRecordingWithGemini(buffer, mimeType, { customerName, callerName });
      if (tx.ok && tx.transcript) {
        transcript = tx.transcript;
        parsedMessages = parseTranscriptToMessages(transcript, callerName, customerName);
        transcriptSource = 'local_recording';
      }
    } catch (err) {
      log.warn('TIMING', `[PREDICT-RECORDING] Local recording analysis failed: ${err.message}`);
    }
  }

  if (!transcript && callSid) {
    const rescue = await fetchTranscriptFromTwilioRecordingNow({
      callSid,
      customerName,
      callerName,
      maxAttempts: 2,
    });
    if (rescue.ok && rescue.transcript) {
      transcript = rescue.transcript;
      parsedMessages = parseTranscriptToMessages(transcript, callerName, customerName);
      transcriptSource = 'twilio_recording_fetch';
    } else {
      return buildPredictionSnapshot('recording_based', null, {
        reason: rescue.reason || 'recording_unavailable',
        transcript_source: null,
        transcript_length: 0,
      });
    }
  }

  if (!transcript || parsedMessages.length === 0) {
    return buildPredictionSnapshot('recording_based', null, {
      reason: 'recording_transcript_parse_failed',
      transcript_source: transcriptSource,
      transcript_length: transcript.length,
    });
  }

  const analysis = finalizeSemanticOutcome(await analyzeConversation(parsedMessages, conv));
  analysis.notes = sanitizeLogText(analysis.notes || '') || null;
  return buildPredictionSnapshot('recording_based', analysis, {
    transcript_source: transcriptSource,
    transcript_length: transcript.length,
    message_count: parsedMessages.length,
  });
}

function smartExtractPtpDate(textLower) {
  const today = new Date();
  const maxDays = 5;
  const clampDate = (d) => {
    const max = new Date(Date.now() + maxDays * 86400000);
    return (d > max ? max : d).toISOString().split('T')[0];
  };

  const normalizedDateText = normalizeTranscriptToRomanUrdu(String(textLower || '').toLowerCase())
    // Join split day digits only in date-like contexts: "1 7 april", "1 7 ko", "1 9 tk", "1 9 nahi 1 7"
    .replace(/\b(\d)\s+(\d)(?=\s*(?:ko|tk|tak|tarikh|tarik|taarikh|tareekh|april|may|june|july|march|february|january|august|september|october|november|december|nahi)\b)/g, '$1$2')
    .replace(/\b(\d)\s+(\d)\s+(\d)(?=\s*(?:ko|tk|tak|tarikh|tarik|taarikh|tareekh|april|may|june|july|march|february|january|august|september|october|november|december|nahi)\b)/g, '$2$3')
    .replace(/\b(\d{1,2})\s+nahi\s+(\d)\s+(\d)\b/g, '$1 nahi $2$3')
    .replace(/\b(\d)\s+(\d)\s+nahi\s+(\d{1,2})\b/g, '$1$2 nahi $3');

  const correctedNumericDateMatch = normalizedDateText.match(/\b(\d{1,2})\s+nahi\s+(\d{1,2})\b/);
  if (correctedNumericDateMatch) {
    const correctedDay = parseInt(correctedNumericDateMatch[2], 10);
    if (correctedDay >= 1 && correctedDay <= 31) {
      const correctedTarget = new Date(today.getFullYear(), today.getMonth(), correctedDay);
      if (correctedTarget <= today) correctedTarget.setMonth(correctedTarget.getMonth() + 1);
      return clampDate(correctedTarget);
    }
  }

  // 1. Relative day words ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â with comprehensive fuzzy/misspelling matching
  // Exact matches first
  const relativeDaysExact = { 'kal': 1, 'kl': 1, 'tomorrow': 1, 'day after': 2, 'day after tomorrow': 2 };
  for (const [word, days] of Object.entries(relativeDaysExact)) {
    if (normalizedDateText.includes(word)) return clampDate(new Date(Date.now() + days * 86400000));
  }

  // Fuzzy match for "parson/parso" (2 days) ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â all possible misspellings & phonetic variants
  const parsonVariants = [
    'parson', 'parso', 'parsoon', 'parsson', 'parsun', 'pursun', 'purson', 'purso',
    'person', 'perso', 'persoon', 'prson', 'prso', 'parsan', 'parsn', 'prsn',
    'pasron', 'pason', 'paso', 'parzon', 'parzo', 'parrson', 'parrso',
    'parosn', 'paron', 'paro', 'parsoo', 'parsou', 'parsoan', 'parsoen',
    'paarson', 'paarso', 'paarsoon', 'paerson', 'pareson', 'parsn',
    'parsone', 'parsoni', 'parsoni', 'prsoon', 'pirson', 'pirso',
    'parsin', 'parsein', 'parsen', 'parsonn', 'parsun', 'persn',
    'par son', 'par so', 'per son', 'per so',
  ];

  // Fuzzy match for "tarson/narson" (3 days) ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â all possible misspellings & phonetic variants
  const tarsonVariants = [
    'tarson', 'tarso', 'tarsoon', 'tarsson', 'tarsun', 'turson', 'turso',
    'tarsn', 'trson', 'trso', 'tarsan', 'trsn', 'tasron', 'tason', 'taso',
    'tarzon', 'tarzo', 'tarrson', 'tarrso', 'tarosn', 'taron', 'taro',
    'tarsoo', 'tarsou', 'tarsoan', 'tarsoen', 'taarson', 'taarso',
    'taarsoon', 'tareson', 'tarsone', 'tarsoni', 'trsoon', 'tirson', 'tirso',
    'tarsin', 'tarsein', 'tarsen', 'tarsonn', 'tersn', 'tersen',
    'tar son', 'tar so', 'ter son',
    'narson', 'narso', 'narsoon', 'narsson', 'narsun', 'nurson', 'nurso',
    'narsn', 'nrson', 'nrso', 'narsan', 'nrsn', 'nasron', 'nason', 'naso',
    'narzon', 'narzo', 'narrson', 'narrso', 'narosn', 'naron', 'naro',
    'narsoo', 'narsou', 'narsoan', 'narsoen', 'naarson', 'naarso',
    'naarsoon', 'nareson', 'narsone', 'narsoni', 'nrsoon', 'nirson', 'nirso',
    'narsin', 'narsein', 'narsen', 'narsonn', 'nersn', 'nersen',
    'nar son', 'nar so', 'ner son',
  ];

  // Helper: check if any variant appears in text
  const matchesAny = (text, variants) => variants.some(v => text.includes(v));

  // Also do Levenshtein-based fuzzy match for words in transcript against base forms
  const fuzzyMatchRelative = (text) => {
    const words = text.split(/\s+/);
    const levenshtein = (a, b) => {
      const m = a.length, n = b.length;
      const dp = Array.from({length: m+1}, (_, i) => {
        const row = new Array(n+1);
        row[0] = i;
        return row;
      });
      for (let j = 0; j <= n; j++) dp[0][j] = j;
      for (let i = 1; i <= m; i++)
        for (let j = 1; j <= n; j++)
          dp[i][j] = Math.min(dp[i-1][j]+1, dp[i][j-1]+1, dp[i-1][j-1] + (a[i-1]!==b[j-1]?1:0));

      return dp[m][n];
    };
    // Check individual words and bigrams
    for (let i = 0; i < words.length; i++) {
      const w = words[i];
      const bigram = i < words.length - 1 ? w + words[i+1] : '';
      // parson/parso ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ 2 days (allow edit distance ÃƒÂ¢Ã¢â‚¬Â°Ã‚Â¤ 2 for words 4+ chars)
      for (const base of ['parson', 'parso', 'parsoon']) {
        if (w.length >= 4 && levenshtein(w, base) <= 2) return 2;
        if (bigram.length >= 4 && levenshtein(bigram, base) <= 2) return 2;
      }
      // tarson/narson ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ 3 days
      for (const base of ['tarson', 'tarso', 'tarsoon', 'narson', 'narso', 'narsoon']) {
        if (w.length >= 4 && levenshtein(w, base) <= 2) return 3;
        if (bigram.length >= 4 && levenshtein(bigram, base) <= 2) return 3;
      }
    }
    return null;
  };

  if (matchesAny(normalizedDateText, parsonVariants)) return clampDate(new Date(Date.now() + 2 * 86400000));
  if (matchesAny(normalizedDateText, tarsonVariants)) return clampDate(new Date(Date.now() + 3 * 86400000));
  const fuzzyDays = fuzzyMatchRelative(normalizedDateText);
  if (fuzzyDays) return clampDate(new Date(Date.now() + fuzzyDays * 86400000));

  // 2. "N din" patterns
  const dinPatterns = { 'ek din': 1, '1 din': 1, 'do din': 2, '2 din': 2, 'teen din': 3, '3 din': 3, 'tin din': 3, 'char din': 4, '4 din': 4, 'panch din': 5, '5 din': 5, 'do teen din': 3, '2 3 din': 3, 'kuch din': 3 };
  for (const [phrase, days] of Object.entries(dinPatterns)) {
    if (normalizedDateText.includes(phrase)) return clampDate(new Date(Date.now() + days * 86400000));
  }

  // 3. Week references
  if (normalizedDateText.includes('agle hafte') || normalizedDateText.includes('next week')) return clampDate(new Date(Date.now() + 7 * 86400000));
  if (normalizedDateText.includes('is hafte') || normalizedDateText.includes('this week')) return clampDate(new Date(Date.now() + 3 * 86400000));

  // 4. Day names (English + Urdu)
  const dayNames = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
  const urduDayNames = ['itwar', 'somwar', 'mangal', 'budh', 'jumerat', 'juma', 'hafta'];
  for (let i = 0; i < 7; i++) {
    if (normalizedDateText.includes(dayNames[i]) || normalizedDateText.includes(urduDayNames[i])) {
      const diff = ((i - today.getDay() + 7) % 7) || 7;
      return clampDate(new Date(Date.now() + diff * 86400000));
    }
  }

  // 5. Numeric date: "12 ko", "15 tarikh", "15 april", "2 ko", "1 tarikh"
  const numMatch = normalizedDateText.match(/(\d{1,2})\s*(ko|tk|tak|tarikh|tarik|taarikh|april|may|june|july|march|february|january|august|september|october|november|december)/);
  if (numMatch) {
    const dayNum = parseInt(numMatch[1]);
    const monthMap = { january: 0, february: 1, march: 2, april: 3, may: 4, june: 5, july: 6, august: 7, september: 8, october: 9, november: 10, december: 11 };
    let month = monthMap[numMatch[2]] ?? today.getMonth();
    let target = new Date(today.getFullYear(), month, dayNum);
    if (target <= today) target.setMonth(target.getMonth() + 1);
    return clampDate(target);
  }

  // 6. Urdu ordinal/cardinal numbers: "chaudah ko", "pandrahvi tarikh", "bees ko", "do pehli", "ek tarikh"
  const urduNumbers = {
    'pehli': 1, 'doosri': 2, 'teesri': 3, 'chauthi': 4, 'paanchvi': 5,
    'chhai': 6, 'saatvi': 7, 'aathvi': 8, 'nauvi': 9, 'dasvi': 10,
    'gyaarvi': 11, 'baarvi': 12, 'tervi': 13, 'chaudahvi': 14, 'pandrahvi': 15,
    'solahvi': 16, 'satrahvi': 17, 'atharahvi': 18, 'unneesvi': 19, 'beesvi': 20,
    'ikkisvi': 21, 'baisvi': 22, 'teisvi': 23, 'chaubisvi': 24, 'pacchisvi': 25,
    'chaudah': 14, 'pandrah': 15, 'solah': 16, 'satrah': 17, 'atharah': 18,
    'unnees': 19, 'bees': 20, 'ikkees': 21, 'baais': 22, 'teis': 23,
    'chaubis': 24, 'pacchis': 25, 'chhabbis': 26, 'sattais': 27, 'atthais': 28,
    'untees': 29, 'tees': 30, 'ikattis': 31,
    'das': 10, 'gyarah': 11, 'barah': 12, 'terah': 13,
    // Common spoken forms: "do pehli" = 2nd, "ek tarikh" = 1st
    'ek ko': 1, 'do ko': 2, 'teen ko': 3, 'char ko': 4, 'paanch ko': 5,
  };
  
  // Check multi-word patterns first (e.g., "do pehli" ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ 2nd)
  const spokenDateMatch = normalizedDateText.match(/\b(ek|do|teen|char|paanch|chhe|saat|aath|nau|das)\s+(pehli|tarikh|tarik|taarikh|ko)\b/);
  if (spokenDateMatch) {
    const spokenNumMap = { 'ek': 1, 'do': 2, 'teen': 3, 'char': 4, 'paanch': 5, 'chhe': 6, 'saat': 7, 'aath': 8, 'nau': 9, 'das': 10 };
    const dayNum = spokenNumMap[spokenDateMatch[1]] || 1;
    let target = new Date(today.getFullYear(), today.getMonth(), dayNum);
    if (target <= today) target.setMonth(target.getMonth() + 1);
    return clampDate(target);
  }
  
  for (const [word, dayNum] of Object.entries(urduNumbers)) {
    if (normalizedDateText.includes(word)) {
      // For standalone words like "pehli", "doosri" etc. ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ treat as date of month
      let target = new Date(today.getFullYear(), today.getMonth(), dayNum);
      if (target <= today) target.setMonth(target.getMonth() + 1);
      return clampDate(target);
    }
  }

  // 7. Vague commitment without specific date ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ default 2 days
  const vagueCommitPatterns = [
    'kar dunga', 'kar dungi', 'de dunga', 'de dungi', 'bhej dunga', 'bhej dungi',
    'ho jayega', 'ho jayegi', 'pay karunga', 'pay karungi',
    'payment karunga', 'payment karungi', 'daal dunga', 'daal dungi',
    'abhi karta', 'abhi karti', 'jaldi', 'jald',
  ];
  for (const p of vagueCommitPatterns) {
    if (normalizedDateText.includes(p)) return clampDate(new Date(Date.now() + 2 * 86400000));
  }

  // 8. Default: 3 days
  return clampDate(new Date(Date.now() + 3 * 86400000));
}

// Intelligent fallback based on transcript content and duration
function buildDurationFallback(messages, conv) {
  const transcript = messages.map(m => m.text).join(' ').toLowerCase();
  const customerMsgs = messages.filter(m => m.role === 'customer');
  const customerTranscript = customerMsgs.map(m => m.text).join(' ').toLowerCase();
  const msgCount = messages.length;

  // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ PRIORITY 0: Negotiation barrier BEFORE PTP to prevent false ptp_secured on extension/installment speech ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
  if (detectNegotiationBarrierSemantic(customerTranscript)) {
    const hasSpecificNearDate =
      /\b\d{1,2}\s*(ko|tk|tak|tarikh|tarik)\b/.test(customerTranscript) ||
      /\b(kal|kl|parson|parso|parsoon|tarson|narson)\b/.test(customerTranscript);
    if (!hasSpecificNearDate) {
      return { final_response: 'negotiation_barrier', ptp_date: null, ptp_status: null, notes: 'Customer ne extension/installment maanga, date commit nahi diya', schedule_retry_hours: 5, semantic_winner: 'fallback_negotiation_barrier_early' };
    }
  }
  // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ PRIORITY 0.5: Use reusable PTP safety net ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
  const ptpFromTranscript = checkTranscriptForPtp(transcript, messages);
  if (ptpFromTranscript) return ptpFromTranscript;

  // Check transcript content for explicit non-customer clues from CUSTOMER speech only
  const nonCustPatterns = [
    'ghalat number', 'wrong number', 'woh nahi hain', 'wo nahi hain',
    'unka phone', 'un ka phone', 'main nahi hoon', 'koi aur', 'unhe baat',
    'main salah nahi hoon', 'bhai salah nahi hoon', 'salah nahi hoon',
    'wrong number dial kiya', 'wrong number dial kiya hai', 'wrong number hai',
    'main keh raha hoon wrong number', 'main keh rha hoon wrong number',
    'ye unka number', 'ye un ka number', 'mera beta', 'meri beti', 'mera bhai',
    'meri behan', 'mera shohar', 'meri biwi', 'wo bahar gaye', 'wo bahar gayi',
    'abhi nahi hain', 'abhi available nahi', 'sim change', 'ye purana number',
    'ghar pe nahi', 'office mein hain', 'bahar gaye hain', 'hum bata denge',
    'main bata doongi', 'main bata doonga', 'ye number kisi aur', 'wrong person',
    'main inka', 'main unka', 'main unki',
  ];
  if (nonCustPatterns.some(p => customerTranscript.includes(p))) {
    return { final_response: 'non_customer_pickup', ptp_date: null, ptp_status: null, notes: 'Kisi aur ne call receive ki ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â customer available nahi tha', schedule_retry_hours: 5 };
  }

  // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ PRIORITY 0.5: Strong semantic outcomes on noisy transcript ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
  if (detectNonCustomerSemantic(customerTranscript)) {
    return { final_response: 'non_customer_pickup', ptp_date: null, ptp_status: null, notes: 'Call par account holder mojood nahi tha', schedule_retry_hours: 5, semantic_winner: 'fallback_non_customer_semantic' };
  }
  if (detectPaymentDoneSemantic(transcript)) {
    return { final_response: 'payment_done', ptp_date: null, ptp_status: null, notes: 'Customer ne payment already done claim kiya', schedule_retry_hours: 0, semantic_winner: 'fallback_payment_done_semantic' };
  }
  if (detectCallbackRequestedSemantic(customerTranscript)) {
    return { final_response: 'callback_requested', ptp_date: null, ptp_status: null, notes: 'Customer ne callback/later request diya', schedule_retry_hours: 2, semantic_winner: 'fallback_callback_semantic' };
  }
  if (detectNegotiationBarrierSemantic(customerTranscript)) {
    return { final_response: 'negotiation_barrier', ptp_date: null, ptp_status: null, notes: 'Customer ne extension/installment maanga, date commit nahi diya', schedule_retry_hours: 5, semantic_winner: 'fallback_negotiation_barrier_semantic' };
  }

  // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ PRIORITY 1: Payment commitment detection (MUST check before negotiation_barrier) ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
  const paymentCommitPhrases = [
    'karunga', 'karungi', 'kar doon', 'kar dunga', 'kar dungi',
    'de dunga', 'de dungi', 'kr doonga', 'kr doongi', 'kr dunga', 'kr dungi',
    'bhej dunga', 'bhej dungi', 'kar deta', 'kar deti',
    'haan theek hai', 'ok theek hai', 'theek hai kar', 'ho jayegi',
    'daal dunga', 'daal dungi', 'pay kar', 'payment kar',
    'kr doo nga', 'kr doo', 'kar doonga', 'de doonga',
    'main ne kr', 'maine kr', 'main kr', 'haan haan',
    'kroonga', 'krungi', 'kronga', 'bilkul', 'sahi hai',
  ];
  const hasPaymentCommit = paymentCommitPhrases.some(p => transcript.includes(p));
  
  if (hasPaymentCommit) {
    const ptpDate = smartExtractPtpDate(transcript);
    return { final_response: 'ptp_secured', ptp_date: ptpDate, ptp_status: 'pending', notes: 'Customer ne payment ka wada kiya', schedule_retry_hours: 0 };
  }

  // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ PRIORITY 2: Negotiation / extension outcomes ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
  if (customerTranscript.includes('installment') || customerTranscript.includes('extension') || customerTranscript.includes('waqt chahiye') || customerTranscript.includes('mahina') || customerTranscript.includes('maheena') || customerTranscript.includes('one month') || customerTranscript.includes('ek mahina') || customerTranscript.includes('1 mahina') || customerTranscript.includes('bad men') || customerTranscript.includes('baad men') || customerTranscript.includes('baad mein')) {
    const futureCommitPhrases = ['dunga', 'duga', 'doonga', 'doongi', 'karunga', 'karungi', 'dega', 'degi', 'de dunga', 'de dungi', 'doonga', 'doongi', 'maalum', 'later', 'later se', 'bad men'];
    const hasFuturePayment = futureCommitPhrases.some(p => customerTranscript.includes(p));
    if (hasFuturePayment) {
      return { final_response: 'negotiation_barrier', ptp_date: null, ptp_status: null, notes: 'Customer ne extension ya baad mein payment ka wada kiya, lekin koi date nahi di', schedule_retry_hours: 5 };
    }
  }
  if (customerTranscript.includes('nahi karunga') || customerTranscript.includes('nahi karungi') || customerTranscript.includes('refuse') || customerTranscript.includes('nahi doongi') || customerTranscript.includes('nahi doonga')) {
    return { final_response: 'refused', ptp_date: null, ptp_status: null, notes: 'Customer ne payment se inkaar kiya', schedule_retry_hours: 24 };
  }
  if (detectCallbackRequestedSemantic(customerTranscript)) {
    return { final_response: 'callback_requested', ptp_date: null, ptp_status: null, notes: 'Customer ne baad mein call karne ko kaha', schedule_retry_hours: 2 };
  }
  if (transcript.includes('payment ho chuki') || transcript.includes('paid already') || transcript.includes('pay kar diya') || transcript.includes('payment kar di') || transcript.includes('already paid') || transcript.includes('ho gayi payment')) {
    return { final_response: 'payment_done', ptp_date: null, ptp_status: null, notes: 'Customer ne kaha payment ho chuki hai', schedule_retry_hours: 0 };
  }
  
  // Fall back to message count
  if (msgCount === 0) {
    return { final_response: 'no_answer', ptp_date: null, ptp_status: null, notes: 'Koi jawab nahi mila', schedule_retry_hours: 2 };
  }
  if (customerMsgs.length === 0) {
    return { final_response: 'no_answer', ptp_date: null, ptp_status: null, notes: 'Customer ki koi baat record nahi hui', schedule_retry_hours: 2 };
  }
  if (msgCount <= 4) {
    return { final_response: 'callback_requested', ptp_date: null, ptp_status: null, notes: 'Mukhtasar call ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â dobara try zaroori', schedule_retry_hours: 2 };
  }
  return { final_response: 'callback_requested', ptp_date: null, ptp_status: null, notes: 'Call ka natija wazeh nahi ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â dobara contact zaroori', schedule_retry_hours: 2 };
}

// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
// WAV File Creator
// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â

function createWav(pcmData, sampleRate) {
  const dataSize = pcmData.length * 2;
  const buffer = Buffer.alloc(44 + dataSize);
  buffer.write('RIFF', 0); buffer.writeUInt32LE(36 + dataSize, 4);
  buffer.write('WAVE', 8); buffer.write('fmt ', 12);
  buffer.writeUInt32LE(16, 16); buffer.writeUInt16LE(1, 20); buffer.writeUInt16LE(1, 22);
  buffer.writeUInt32LE(sampleRate, 24); buffer.writeUInt32LE(sampleRate * 2, 28);
  buffer.writeUInt16LE(2, 32); buffer.writeUInt16LE(16, 34);
  buffer.write('data', 36); buffer.writeUInt32LE(dataSize, 40);
  for (let i = 0; i < pcmData.length; i++) {
    buffer.writeInt16LE(pcmData[i], 44 + i * 2);
  }
  return buffer;
}

// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
// MEDIA STREAM HANDLER ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â Direct WebSocket bridge (bridge-server-7 pattern)
// Twilio ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬Â This Server ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬Â Gemini Native Audio API (v1alpha)
// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â

export function handleMediaStream(twilioWs, initialConversationId) {
  const T0 = Date.now(); // ÃƒÂ¢Ã‚ÂÃ‚Â± TIMING: stream connect
  let activeConversationId = initialConversationId || '';
  log.info('TIMING', `[T+0ms] Twilio stream connected, convId: ${activeConversationId}, activeCalls: ${ACTIVE_CALLS.size}`);
  broadcast({ type: 'LOG', message: `TWILIO: Stream connected (active: ${ACTIVE_CALLS.size})`, source: 'twil' });

  // Register in active call registry
  const callEntry = {
    convId: activeConversationId,
    callSid: null,
    customerName: '',
    startedAt: T0,
    twilioWs,
    geminiWs: null,
  };
  if (activeConversationId) ACTIVE_CALLS.set(activeConversationId, callEntry);

  let streamSid = null;
  let callSid = null;
  let wsGemini = null;
  let mediaEventCount = 0;
  let conv = null;
  let callAssets = activeConversationId ? CALL_PREPARATION_CACHE.get(activeConversationId) || null : null;
  let geminiReconnectAttempts = 0;
  let geminiRecovering = false;
  const MAX_GEMINI_RECONNECTS = 4;

  function getSessionVoiceName(metadata = {}, assetMeta = {}) {
    const callerGender = resolveCallerGender(
      metadata.callerGender || assetMeta.caller_gender || conv?.caller_gender || conv?.callerGender,
      metadata.agentName || metadata.callerName || assetMeta.caller_name || conv?.caller_name || conv?.callerName || 'Omar',
      'male'
    );
    const requestedVoice = assetMeta.voice || metadata.voice || conv?.voice || conv?.callerVoice || '';
    return normalizeVoiceName(requestedVoice, callerGender);
  }

  // Helper: update registry whenever geminiWs changes
  function updateRegistryGemini() {
    if (activeConversationId && ACTIVE_CALLS.has(activeConversationId)) {
      ACTIVE_CALLS.get(activeConversationId).geminiWs = wsGemini;
    }
  }

  // ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â EARLY GEMINI CONNECTION ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â start connecting IMMEDIATELY, don't wait for Twilio start event ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
  // This saves 3-8 seconds of latency since Gemini WS handshake + setup happens during Twilio stream negotiation
  let earlyGeminiStarted = false;
  function closeGeminiSocket(targetWs = wsGemini, reason = 'local_close') {
    if (!targetWs) return;
    targetWs._expectedClose = true;
    targetWs._closeReason = reason;
    if (targetWs === wsGemini) { wsGemini = null; updateRegistryGemini(); }
    try {
      if (targetWs.readyState === WebSocket.OPEN || targetWs.readyState === WebSocket.CONNECTING) {
        targetWs.close();
      }
    } catch {}
  }

  function startEarlyGeminiConnection(options = {}) {
    const { forceFresh = false, reason = 'initial' } = options;
    if (!forceFresh && (earlyGeminiStarted || wsGemini)) return;
    earlyGeminiStarted = true;

    if (forceFresh && wsGemini && (wsGemini.readyState === WebSocket.OPEN || wsGemini.readyState === WebSocket.CONNECTING)) {
      closeGeminiSocket(wsGemini, `replace_${reason}`);
    }

    // ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â CHECK FOR PRE-WARMED GEMINI WS (created during RINGING phase) ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
    const preWarmed = !forceFresh && activeConversationId ? PREWARMED_GEMINI_WS.get(activeConversationId) : null;
    if (preWarmed && (preWarmed.readyState === WebSocket.OPEN || preWarmed.readyState === WebSocket.CONNECTING)) {
      const prewarmAgeMs = Date.now() - (preWarmed._prewarmCreatedAt || Date.now());
      const hasCompletedSetup = preWarmed._setupComplete === true;
      if (!hasCompletedSetup && prewarmAgeMs > PREWARM_REUSE_STALE_MS) {
        log.warn('TIMING', `[T+${Date.now()-T0}ms] Discarding stale pre-warmed Gemini WS (age=${prewarmAgeMs}ms, setupComplete=false) ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â opening fresh socket`);
        cleanupPrewarmed(activeConversationId);
      } else {
      PREWARMED_GEMINI_WS.delete(activeConversationId);
      // Remove old listeners from pre-warm phase
      preWarmed.removeAllListeners();
      preWarmed._expectedClose = false;
      wsGemini = preWarmed;
      log.info('TIMING', `[T+${Date.now()-T0}ms] ÃƒÂ¢Ã¢â€žÂ¢Ã‚Â»ÃƒÂ¯Ã‚Â¸Ã‚Â Reusing pre-warmed Gemini WS (age: ${prewarmAgeMs}ms, setupComplete=${hasCompletedSetup})`);
      setupGeminiHandlers(wsGemini);
      // If already open, manually trigger the open handler logic
      if (wsGemini.readyState === WebSocket.OPEN) {
        wsGemini.emit('open');
      }
      return;
      }
    }

    // Fallback: create fresh connection
    const apiKey = getGeminiApiKey();
    if (!apiKey) {
      log.warn('MEDIA', 'No Gemini API key ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â cannot pre-connect');
      return;
    }
    const geminiURL = `wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1alpha.GenerativeService.BidiGenerateContent?key=${apiKey}`;
    log.info('TIMING', `[T+${Date.now()-T0}ms] Gemini WS connecting (${forceFresh ? `reconnect:${reason}` : 'fresh'})...`);
    broadcast({ type: 'LOG', message: 'GEMINI: Pre-connecting...', source: 'gem' });
    wsGemini = new WebSocket(geminiURL);
    wsGemini._expectedClose = false;
    updateRegistryGemini();
    setupGeminiHandlers(wsGemini);
  }

  // Pre-connect immediately if we have a conversation ID
  if (activeConversationId) {
    // Load conversation data first so we have metadata for Gemini setup
    conv = db.prepare('SELECT * FROM call_conversations WHERE id = ?').get(activeConversationId);
    if (conv) {
      callAssets = CALL_PREPARATION_CACHE.get(activeConversationId) || callAssets || null;
    }
    startEarlyGeminiConnection();
  }

  let startEventMetadata = {};

  // Recording buffers
  const inboundChunks = [];
  const outboundChunks = [];
  const messages = [];
  const callStartTime = Date.now();
  const what_customer_said = [];   // customer speech turns only
  const what_agent_said = [];      // agent (Gemini) speech turns only
  let useTwilioRecording = false; // Will be set from call assets

  // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Noise Gate & Network Monitor (per-call instances) ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
  const noiseGate = createNoiseGateState();
  const networkMonitor = createNetworkMonitor();

  // State tracking (bridge pattern)
  let currentConversationState = 'GREETING';
  let conversationEnded = false;
  let abuseAttempts = 0;
  let geminiSetupFailed = false;
  let geminiFailureReason = '';
  let customerTurnsHeard = 0;
  let callAdvancedPastGreeting = false;
  let geminiSetupComplete = false;
  let greetingRequestSent = false;
  let openingInstructionSent = false;
  let preparedGreetingPlayed = false;
  let postGreetingListenGate = false;
  let lastGreetingSource = 'none';
  let audioPacketsBufferedBeforeSetup = 0;
  let pickupRecoveryAttempts = 0;
  let lastCustomerText = '';
  let nonCustomerConfirmPending = false;
  let nonCustomerIntentLocked = false;
  let lastNonCustomerPhase = 'generic';  // Track non-customer type: 'wrong_number', 'name_mismatch', 'family_member', 'relay_message', 'unavailable', 'generic'
  let lastNonCustomerDeclaredName = '';  // Track name explicitly stated by non-customer (e.g., "main Aslam hoon")
  let offTopicRedirects = 0;
  let lastAgentText = '';
  let recentAgentClosingText = '';
  let closingSignalDetectedAt = 0;
  let finalClosingPlaybackLock = false;
  let suppressCustomerInputDuringEnding = false;
  let finalClosingPromptSent = false;
  let greetingPlaybackLock = false;  // LOCK: Prevent customer interruption during greeting
  let greetingPlaybackStartedAt = 0; // Timestamp when greeting started
  let greetingMinPlaybackMs = 1700;  // Keep brief lock, but release sooner for faster interaction
  let endCause = { source: 'unknown', reason: 'not_set', details: null };
  let customerSpeechDetectedAt = 0;
  let forceEndingCall = false;
  let firstAudioSentToTwilio = false;
  let closingRequested = false;
  let gracefulHangupTimer = null;
  let initialCustomerResponseTimer = null;
  let lastAgentAudioSentAt = 0;
  let agentPlaybackActiveUntil = 0;
  let lastNoiseActivityAt = 0;
  let twilioInboundGapAlertLevel = 0;
  const preSetupAudioBuffer = [];
  const MAX_PRE_SETUP_AUDIO_PACKETS = 150;
  const INITIAL_CUSTOMER_RESPONSE_TIMEOUT_MS = 9000;
  const CONNECTIVITY_FOLLOWUP_INTERVAL_MS = 9000;
  const MAX_CONNECTIVITY_FOLLOWUPS = 5;
  let connectivityFollowupCount = 0;
  const GREETING_CHUNK_INTERVAL_MS = 20;
  const GREETING_START_DELAY_MS = 20;

  // ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â PTP NEGOTIATION LOCK ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â blocks hangup during active beyond-PTP-window negotiation ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
  let ptpNegotiationAttempts = 0;
  let ptpNegotiationActive = false;
  const MAX_PTP_NEGOTIATION_ATTEMPTS = 10;

  // ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â GEMINI KEEPALIVE ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â prevent idle WebSocket timeout ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
  let geminiKeepaliveInterval = null;
  let lastGeminiSendAt = Date.now();
  const GEMINI_KEEPALIVE_MS = 15000; // 15s keepalive interval

  function startGeminiKeepalive() {
    stopGeminiKeepalive();
    geminiKeepaliveInterval = setInterval(() => {
      if (!wsGemini || wsGemini.readyState !== WebSocket.OPEN || conversationEnded || forceEndingCall) {
        stopGeminiKeepalive();
        return;
      }
      const elapsed = Date.now() - lastGeminiSendAt;
      if (elapsed >= GEMINI_KEEPALIVE_MS - 2000) {
        const silentPcm = Buffer.alloc(480).toString('base64');
        try {
          wsGemini.send(JSON.stringify({
            realtime_input: {
              media_chunks: [{ mime_type: 'audio/pcm;rate=24000', data: silentPcm }]
            }
          }));
          lastGeminiSendAt = Date.now();
        } catch (e) {
          log.warn('MEDIA', `Keepalive send failed: ${e.message}`);
        }
      }
    }, GEMINI_KEEPALIVE_MS);
  }

  function stopGeminiKeepalive() {
    if (geminiKeepaliveInterval) {
      clearInterval(geminiKeepaliveInterval);
      geminiKeepaliveInterval = null;
    }
  }

  // ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â AGENT RESPONSE WATCHDOG ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â nudge Gemini if no audio after customer speech ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
  let agentResponseWatchdog = null;
  const AGENT_RESPONSE_TIMEOUT_MS = 8000;

  function startAgentWatchdog() {
    clearAgentWatchdog();
    if (conversationEnded || forceEndingCall || closingRequested) return;
    agentResponseWatchdog = setTimeout(() => {
      agentResponseWatchdog = null;
      if (!wsGemini || wsGemini.readyState !== WebSocket.OPEN || !geminiSetupComplete) return;
      if (conversationEnded || forceEndingCall || closingRequested) return;
      log.warn('MEDIA', `[WATCHDOG] No agent audio for ${AGENT_RESPONSE_TIMEOUT_MS}ms after customer speech ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â nudging Gemini`);
      broadcast({ type: 'LOG', message: 'ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â Agent response delayed ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â nudging AI', source: 'sys' });
      try {
        wsGemini.send(JSON.stringify({
          client_content: {
            turns: [{ role: 'user', parts: [{ text: 'RESPOND NOW. Customer is waiting. Speak immediately in Roman Urdu ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â give a brief, natural response. Do NOT think internally.' }] }],
            turn_complete: true,
          },
        }));
        lastGeminiSendAt = Date.now();
      } catch (e) {
        log.warn('MEDIA', `Watchdog nudge failed: ${e.message}`);
      }
    }, AGENT_RESPONSE_TIMEOUT_MS);
  }

  function clearAgentWatchdog() {
    if (agentResponseWatchdog) {
      clearTimeout(agentResponseWatchdog);
      agentResponseWatchdog = null;
    }
  }

  // ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â TRANSCRIPT AGGREGATION ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â debounce word-by-word fragments into full sentences ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
  // Gemini sends transcripts word-by-word. Processing each word through detectState()
  // causes false keyword matches and fragmented messages. Buffer and flush after pause.
  let customerTranscriptBuffer = '';
  let customerTranscriptRawBuffer = '';
  let customerTranscriptTimer = null;
  const CUSTOMER_TRANSCRIPT_DEBOUNCE_MS = 1000; // Faster flush so explicit non-customer cues are acted on before next agent turn
  let customerFirstWordLogged = false;

  let agentTranscriptBuffer = '';
  let agentTranscriptTimer = null;
  const AGENT_TRANSCRIPT_DEBOUNCE_MS = 800; // 0.8s after last chunk = sentence done

  function flushCustomerTranscript() {
    customerTranscriptTimer = null;
    const fullText = customerTranscriptBuffer.trim();
    customerTranscriptBuffer = '';
    customerTranscriptRawBuffer = '';
    // Ã¢â€â‚¬Ã¢â€â‚¬ SUPPRESS DURING: ending message, final closing lock, OR greeting playback lock Ã¢â€â‚¬Ã¢â€â‚¬
    if (suppressCustomerInputDuringEnding || closingRequested || finalClosingPlaybackLock || greetingPlaybackLock) {
      if (greetingPlaybackLock) {
        log.info('GREETING-LOCK-SUPPRESS', `Ignoring customer speech during greeting: "${fullText.substring(0, 50)}..."`);
        broadcast({ type: 'LOG', message: `[GREETING LOCK] Suppressing customer input`, source: 'system' });
      }
      return;
    }
    if (!fullText) return;

    const normalizedText = normalizeTranscriptToRomanUrdu(fullText);
    const cleanText = pushMessage('customer', normalizedText);
    if (cleanText) {
      lastCustomerText = normalizedText;
      if (!conversationEnded && !forceEndingCall) {
        detectState(normalizedText, 'customer');
      }
      log.info('MEDIA', `ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â Customer (aggregated): raw="${cleanText}" normalized="${normalizedText}"`);
    }
  }

  function flushAgentTranscript() {
    agentTranscriptTimer = null;
    const fullText = agentTranscriptBuffer.trim();
    agentTranscriptBuffer = '';
    if (!fullText) return;

    const cleanText = pushMessage('agent', fullText);
    if (cleanText) {
      lastAgentText = cleanText;
      if (!conversationEnded && !forceEndingCall) {
        detectState(cleanText, 'agent');
      }
    }
  }

  function appendCustomerTranscript(text, rawText = text) {
    customerTranscriptBuffer = (customerTranscriptBuffer + ' ' + text).trim();
    customerTranscriptRawBuffer = (customerTranscriptRawBuffer + ' ' + rawText).trim();
    if (customerTranscriptTimer) clearTimeout(customerTranscriptTimer);
    customerTranscriptTimer = setTimeout(flushCustomerTranscript, CUSTOMER_TRANSCRIPT_DEBOUNCE_MS);
  }

  function appendAgentTranscript(text) {
    agentTranscriptBuffer = (agentTranscriptBuffer + ' ' + text).trim();
    if (agentTranscriptTimer) clearTimeout(agentTranscriptTimer);

    // Fast-path close: detect [END_CALL] markers AND explicit goodbye phrases.
    // Goodbye phrases are included here so we don't rely on the 800ms debounce ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â
    // closing Gemini immediately prevents a second "Allah Hafiz" from being generated.
    const chunkNorm = String(text || '');
    const chunkHasEndDirective = hasEndCallDirective(chunkNorm);
    const chunkHasGoodbye = hasGoodbyeClosingPhrase(chunkNorm);
    const chunkHasExplicitEndToken =
      chunkHasEndDirective ||
      chunkHasGoodbye;

    if (chunkHasExplicitEndToken && !conversationEnded && !forceEndingCall && !closingRequested) {
      finalClosingPlaybackLock = true;
      suppressCustomerInputDuringEnding = true;
      if (!closingSignalDetectedAt) closingSignalDetectedAt = Date.now();
      // Flush immediately so transcript/state are consistent before ending.
      agentTranscriptTimer = setTimeout(() => {
        try { flushAgentTranscript(); } catch {}
      }, 20);

      const fastReason = nonCustomerIntentLocked ? 'non_customer' : 'agent_closing';
      const ensuredClosing = chunkHasEndDirective && !chunkHasGoodbye
        ? requestFinalClosingLine('append_transcript_end_call')
        : false;
      const fastDelayMs = nonCustomerIntentLocked ? 7000 : (ensuredClosing ? 7000 : 6200);
      scheduleGracefulHangup(fastReason, fastDelayMs);

      // Immediately close Gemini so it cannot generate any further response.
      // This prevents the agent from saying "Allah Hafiz" a second time if the
      // customer speaks after the first goodbye.
      if (!ensuredClosing) {
        const geminiToClose = wsGemini;
        if (geminiToClose && geminiToClose.readyState === WebSocket.OPEN) {
          setTimeout(() => closeGeminiSocket(geminiToClose, 'closing_phrase_detected'), 900);
        }
      }
      return;
    }

    agentTranscriptTimer = setTimeout(flushAgentTranscript, AGENT_TRANSCRIPT_DEBOUNCE_MS);
  }

  function bufferInboundAudio(muLawBuffer, trigger = 'bridge_wait') {
    if (!muLawBuffer?.length) return;
    audioPacketsBufferedBeforeSetup += 1;
    preSetupAudioBuffer.push(Buffer.from(muLawBuffer));
    if (preSetupAudioBuffer.length > MAX_PRE_SETUP_AUDIO_PACKETS) preSetupAudioBuffer.shift();
    if (audioPacketsBufferedBeforeSetup === 1) {
      log.info('TIMING', `[T+${Date.now()-T0}ms] Buffering audio ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â ${trigger}...`);
    }
  }

  // ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â GREETING CONTROL ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â opening greeting plays once; follow-ups are handled by Gemini checks ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
  let greetingRetryCount = 0;
  let greetingRetryTimer = null;
  const GREETING_RETRY_INTERVAL_MS = 6500;
  const MAX_GREETING_RETRIES = 0;

  function startGreetingRetryTimer() {
    if (greetingRetryTimer) clearTimeout(greetingRetryTimer);
    if (MAX_GREETING_RETRIES <= 0) return;
    if (conversationEnded || forceEndingCall || closingRequested || customerTurnsHeard > 0) return;

    greetingRetryTimer = setTimeout(() => {
      if (conversationEnded || forceEndingCall || closingRequested || customerTurnsHeard > 0) return;

      greetingRetryCount++;
      if (greetingRetryCount >= MAX_GREETING_RETRIES) return;
    }, GREETING_RETRY_INTERVAL_MS);
  }

  function cancelGreetingRetryTimer() {
    if (greetingRetryTimer) {
      clearTimeout(greetingRetryTimer);
      greetingRetryTimer = null;
    }
  }

  // Replay only the audio chunks to Twilio without pushing duplicate transcript/detectState
  function replayGreetingAudioOnly() {
    if (customerTurnsHeard > 0 || conversationEnded || forceEndingCall) return;
    const voiceName = getSessionVoiceName(startEventMetadata || {}, callAssets || {});
    const retryGreetingOptions = USE_NATIVE_AUDIO_FOR_OPENING_GREETING
      ? { allowToneFallback: true, forceConnectionTone: true }
      : { allowToneFallback: true };
    const { payloadsToPlay } = resolveGreetingPayloads(callAssets, voiceName, retryGreetingOptions);
    if (!streamSid || !payloadsToPlay.length || twilioWs.readyState !== WebSocket.OPEN) return;
    sendTwilioPayloadSequence(payloadsToPlay, {
      intervalMs: GREETING_CHUNK_INTERVAL_MS,
      initialDelayMs: GREETING_START_DELAY_MS,
      stopIf: () => customerTurnsHeard > 0 || conversationEnded || forceEndingCall || closingRequested,
    });
    log.info('MEDIA', `[GREETING] Replayed audio-only (${payloadsToPlay.length} chunks)`);
  }

  function sendTwilioPayloadSequence(payloads = [], options = {}) {
    const intervalMs = Number(options.intervalMs || 20);
    const initialDelayMs = Number(options.initialDelayMs || 0);
    const stopIf = typeof options.stopIf === 'function' ? options.stopIf : () => false;

    if (!Array.isArray(payloads) || payloads.length === 0) return 0;
    if (!streamSid || twilioWs.readyState !== WebSocket.OPEN) return 0;

    const estimatedPlaybackMs = initialDelayMs + (payloads.length * intervalMs) + 1400;
    agentPlaybackActiveUntil = Math.max(agentPlaybackActiveUntil, Date.now() + estimatedPlaybackMs);

    payloads.forEach((payload, idx) => {
      const when = initialDelayMs + idx * intervalMs;
      setTimeout(() => {
        if (stopIf()) return;
        if (!streamSid || twilioWs.readyState !== WebSocket.OPEN) return;
        try {
          twilioWs.send(JSON.stringify({ event: 'media', streamSid, media: { payload } }));
          lastAgentAudioSentAt = Date.now();
          agentPlaybackActiveUntil = Math.max(agentPlaybackActiveUntil, Date.now() + 1200);
          try {
            const chunk = Buffer.from(payload, 'base64');
            outboundChunks.push(chunk);
            appendFileSync(debugFiles.twilioMulaw, chunk);
          } catch {}
        } catch (e) {}
      }, when);
    });

    return payloads.length;
  }

  // ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â SILENCE TIMEOUT ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â 3 nudges (30s each) with context resume, then polite goodbye ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
  let silenceTimer = null;
  let silenceNudgeCount = 0;
  const SILENCE_NUDGE_MS = 30000;       // 30s between each nudge
  const MAX_SILENCE_NUDGES = 3;         // 3 nudges before final goodbye
  const SILENCE_FINAL_WAIT_MS = 15000;  // 15s after last nudge ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ hangup

  function getSilenceNudgePrompt(nudgeNum, nameTag, state) {
    // Each nudge is different ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â escalating from gentle check-in to context reminder to final attempt
    const contextHint = state === 'COLLECTION' ? 'payment date'
      : state === 'DISCLOSURE' ? 'pending amount'
      : state === 'CONSENT' ? 'baat karne'
      : 'baat';

    const nudges = [
      // Nudge 1 ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â Gentle check-in
      `Customer has been silent for 30 seconds. They may be distracted or checking something. Do NOT end the call. Say ONLY: "${nameTag}, kya aap line par hain? Main yahan hoon, aap jab chaahein baat karein." ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â then WAIT silently. Do NOT say Allah Hafiz.`,
      // Nudge 2 ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â Context reminder, re-engage
      `Customer is still silent after 60 seconds total. They might need a reminder of what you were discussing. Do NOT end the call. Say ONLY: "${nameTag}, hum abhi ${contextHint} ke baare mein baat kar rahe the. Agar aap tayyar hain toh batayein, main sun rahi hoon." ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â then WAIT. Do NOT say Allah Hafiz.`,
      // Nudge 3 ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â Final gentle attempt with soft urgency
      `Customer has been silent for 90 seconds total despite 2 previous nudges. This is your LAST attempt before ending. Do NOT end the call yet. Say ONLY: "${nameTag}, lagta hai shayad line mein masla hai. Agar aap sun rahe hain toh bas ek lafz bol dein ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â main yahan hoon aapki madad ke liye." ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â then WAIT. Do NOT say Allah Hafiz.`,
    ];
    return nudges[Math.min(nudgeNum, nudges.length - 1)];
  }

  function scheduleSilenceNudge() {
    if (silenceTimer) clearTimeout(silenceTimer);
    if (conversationEnded || forceEndingCall || closingRequested) return;

    silenceTimer = setTimeout(() => {
      if (conversationEnded || forceEndingCall || closingRequested) return;

      const custName = conv?.customer_name || '';
      const custGender = normalizeGender(conv?.customer_gender || callAssets?.customer_gender || 'male', 'male');
      const honor = custGender === 'female' ? 'Sahiba' : 'Sahab';
      const nameTag = custName ? `${custName} ${honor}` : honor;

      silenceNudgeCount++;

      if (silenceNudgeCount <= MAX_SILENCE_NUDGES) {
        // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Send nudge with variation ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
        log.info('TIMING', `[SILENCE] Nudge ${silenceNudgeCount}/${MAX_SILENCE_NUDGES} ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â ${silenceNudgeCount * 30}s total silence`);
        broadcast({ type: 'LOG', message: `ÃƒÂ¢Ã‚ÂÃ‚Â±ÃƒÂ¯Ã‚Â¸Ã‚Â Silence nudge ${silenceNudgeCount}/${MAX_SILENCE_NUDGES}`, source: 'sys' });

        if (wsGemini && wsGemini.readyState === WebSocket.OPEN && geminiSetupComplete) {
          const prompt = getSilenceNudgePrompt(silenceNudgeCount - 1, nameTag, currentConversationState);
          wsGemini.send(JSON.stringify({
            client_content: {
              turns: [{ role: 'user', parts: [{ text: prompt }] }],
              turn_complete: true,
            },
          }));
        }

        // Schedule next nudge or final timeout
        if (silenceNudgeCount < MAX_SILENCE_NUDGES) {
          scheduleSilenceNudge(); // next 30s nudge
        } else {
          // After 3rd nudge, wait 15s more then end
          silenceTimer = setTimeout(() => {
            if (conversationEnded || forceEndingCall || closingRequested) return;
            log.info('TIMING', `[SILENCE] ${MAX_SILENCE_NUDGES} nudges exhausted + 15s ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â ending call`);
            broadcast({ type: 'LOG', message: 'ÃƒÂ¢Ã‚ÂÃ‚Â±ÃƒÂ¯Ã‚Â¸Ã‚Â All nudges exhausted ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â ending call', source: 'sys' });

            if (wsGemini && wsGemini.readyState === WebSocket.OPEN && geminiSetupComplete) {
              wsGemini.send(JSON.stringify({
                client_content: {
                  turns: [{ role: 'user', parts: [{ text: `Customer did not respond after ${MAX_SILENCE_NUDGES} attempts over ${MAX_SILENCE_NUDGES * 30 + 15} seconds. End the call politely. Say ONLY: "${nameTag}, lagta hai abhi aap busy hain. Koi baat nahi, hum dobara rabta karenge. Shukriya. Allah Hafiz. [END_CALL]"` }] }],
                  turn_complete: true,
                },
              }));
            }
            scheduleGracefulHangup('silence_timeout', 5200);
          }, SILENCE_FINAL_WAIT_MS);
        }
      }
    }, SILENCE_NUDGE_MS);
  }

  function resetSilenceTimer() {
    if (silenceTimer) clearTimeout(silenceTimer);
    if (customerTurnsHeard === 0) return;
    silenceNudgeCount = 0; // Customer spoke ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â reset nudge counter
    if (conversationEnded || forceEndingCall || closingRequested) return;
    scheduleSilenceNudge();
  }

  // Debug files
  const debugDir = getDebugSessionDir(activeConversationId || 'unknown');
  const debugFiles = {
    geminiRaw: path.join(debugDir, 'gemini_raw.pcm'),
    twilioMulaw: path.join(debugDir, 'twilio_outbound.mulaw'),
    transcript: path.join(debugDir, 'transcript.txt'),
    summary: path.join(debugDir, 'summary.json'),
  };
  writeFileSync(debugFiles.geminiRaw, Buffer.alloc(0));
  writeFileSync(debugFiles.twilioMulaw, Buffer.alloc(0));
  writeFileSync(debugFiles.transcript, '', 'utf8');

  function appendTranscriptLine(speaker, text) {
    try { appendFileSync(debugFiles.transcript, `${speaker}: ${text}\n`, 'utf8'); } catch (e) {}
  }

  function normalizeSafetyText(value = '') {
    return String(value || '')
      .toLowerCase()
      .replace(/[^a-z0-9\s?]/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
  }

  function isShortPickupReply(value = '') {
    const normalized = normalizeSafetyText(value);
    if (!normalized || normalized.length > 32) return false;

    return [
      /^(hello|helo)(?:\s+\w+){0,2}$/,
      /^(assalam|assalamualaikum|assalam o alaikum)(?:\s+\w+){0,2}$/,
      /^(ji|jee|haan|han|hmm|hmmm|kon|kaun|boliye)$/,
      /^(ji|jee)\s+(boliye|haan|han)$/,
      /^(kon|kaun)\s+hai$/,
    ].some((pattern) => pattern.test(normalized));
  }

  function setEndCause(source, reason, details = null, force = false) {
    if (!force && endCause.reason !== 'not_set') return;
    endCause = { source, reason, details };
  }

  function clearGracefulHangupTimer() {
    if (gracefulHangupTimer) {
      clearTimeout(gracefulHangupTimer);
      gracefulHangupTimer = null;
    }
  }

  function clearInitialCustomerResponseTimer() {
    if (initialCustomerResponseTimer) {
      clearTimeout(initialCustomerResponseTimer);
      initialCustomerResponseTimer = null;
    }
  }

  function requestFinalClosingLine(trigger = 'end_call_marker') {
    if (finalClosingPromptSent) return false;
    if (!wsGemini || wsGemini.readyState !== WebSocket.OPEN || !geminiSetupComplete) return false;
    if (conversationEnded || forceEndingCall) return false;

    const custGender = normalizeGender(conv?.customer_gender || callAssets?.customer_gender || 'male', 'male');
    const honor = custGender === 'female' ? 'Sahiba' : 'Sahab';
    const custName = conv?.customer_name || callAssets?.customer_name || '';
    const nameTag = custName ? `${custName} ${honor}` : honor;

    try {
      finalClosingPromptSent = true;
      suppressCustomerInputDuringEnding = true;
      finalClosingPlaybackLock = true;
      wsGemini.send(JSON.stringify({
        client_content: {
          turns: [{
            role: 'user',
            parts: [{
              text: `You requested call end (${trigger}). Complete a polished final line now and include [END_CALL]. Say ONLY: "Theek hai ${nameTag}. Aaj ke liye itna hi. Shukriya. Allah Hafiz. [END_CALL]"`,
            }],
          }],
          turn_complete: true,
        },
      }));
      lastGeminiSendAt = Date.now();

      // Safety release in case close marker is never produced.
      setTimeout(() => {
        if (conversationEnded || forceEndingCall) return;
        finalClosingPlaybackLock = false;
        suppressCustomerInputDuringEnding = false;
      }, 12000);

      return true;
    } catch (err) {
      // Roll back locks if prompt cannot be sent.
      finalClosingPromptSent = false;
      suppressCustomerInputDuringEnding = false;
      finalClosingPlaybackLock = false;
      log.warn('MEDIA', `Final closing prompt failed: ${err.message}`);
      return false;
    }
  }

  function requestConnectivityFollowup(attemptNumber = 1) {
    const custGender = normalizeGender(conv?.customer_gender || callAssets?.customer_gender || 'male', 'male');
    const honor = custGender === 'female' ? 'Sahiba' : 'Sahab';
    const custName = conv?.customer_name || callAssets?.customer_name || '';
    const nameTag = custName ? `${custName} ${honor}` : honor;
    const isLastAttempt = attemptNumber >= MAX_CONNECTIVITY_FOLLOWUPS;

    if (wsGemini && wsGemini.readyState === WebSocket.OPEN && geminiSetupComplete) {
      const prompt = isLastAttempt
        ? `Customer voice stayed unclear after ${MAX_CONNECTIVITY_FOLLOWUPS} connectivity checks. End politely. Say ONLY: "${nameTag}, lagta hai line clear nahi aa rahi. Koi baat nahi, hum thori der baad dobara call karenge. Shukriya. Allah Hafiz. [END_CALL]"`
        : `Customer voice is unclear or not heard after pickup (attempt ${attemptNumber}/${MAX_CONNECTIVITY_FOLLOWUPS}). Do NOT end the call. Say ONLY one short connectivity follow-up in Roman Urdu like: "${nameTag}, awaaz clear nahi aa rahi. Kya aap dobara bol sakte hain?" Then wait silently.`;

      try {
        wsGemini.send(JSON.stringify({
          client_content: {
            turns: [{ role: 'user', parts: [{ text: prompt }] }],
            turn_complete: true,
          },
        }));
        lastGeminiSendAt = Date.now();
        return true;
      } catch (err) {
        log.warn('MEDIA', `Connectivity follow-up send failed: ${err.message}`);
      }
    }

    // Do not replay the opening greeting. A delayed reconnect can still resume live handling.
    log.warn('MEDIA', `Connectivity follow-up skipped ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â Gemini not ready (attempt ${attemptNumber}/${MAX_CONNECTIVITY_FOLLOWUPS})`);
    return false;
  }

  function startInitialCustomerResponseTimer(timeoutMs = INITIAL_CUSTOMER_RESPONSE_TIMEOUT_MS) {
    clearInitialCustomerResponseTimer();
    if (conversationEnded || forceEndingCall || closingRequested || customerTurnsHeard > 0) return;

    initialCustomerResponseTimer = setTimeout(() => {
      initialCustomerResponseTimer = null;
      if (conversationEnded || forceEndingCall || closingRequested || customerTurnsHeard > 0) return;

      const playbackStillActive =
        greetingPlaybackLock ||
        Boolean(agentTranscriptTimer) ||
        Date.now() < agentPlaybackActiveUntil ||
        (lastAgentAudioSentAt > 0 && Date.now() - lastAgentAudioSentAt < 1800);

      if (playbackStillActive) {
        log.info('TIMING', `[PICKUP-VERIFY] Deferring connectivity follow-up - agent playback still active`);
        startInitialCustomerResponseTimer(1800);
        return;
      }

      connectivityFollowupCount += 1;
      const attempt = connectivityFollowupCount;
      const lastAttempt = attempt >= MAX_CONNECTIVITY_FOLLOWUPS;

      log.warn('TIMING', `[PICKUP-VERIFY] No clear customer speech (${timeoutMs}ms). Connectivity follow-up ${attempt}/${MAX_CONNECTIVITY_FOLLOWUPS}`);
      broadcast({
        type: 'LOG',
        message: `Connectivity check ${attempt}/${MAX_CONNECTIVITY_FOLLOWUPS}: customer voice unclear or not heard`,
        source: 'sys',
      });

      const promptSent = requestConnectivityFollowup(attempt);

      if (lastAttempt) {
        log.warn('TIMING', `[PICKUP-VERIFY] Connectivity follow-ups exhausted (${MAX_CONNECTIVITY_FOLLOWUPS}) ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â ending call gracefully`);
        broadcast({ type: 'LOG', message: 'Connectivity retries exhausted ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â ending call gracefully', source: 'sys' });
        scheduleGracefulHangup('no_customer_audio_after_pickup', promptSent ? 5200 : 2800);
        return;
      }

      startInitialCustomerResponseTimer(CONNECTIVITY_FOLLOWUP_INTERVAL_MS);
    }, timeoutMs);
  }

  function scheduleGracefulHangup(reason = 'agent_close', delayMs = 3500) {
    if (forceEndingCall) return;
    // Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬ ENDING MESSAGE COMPLETION: Ensure full playback before hangup Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
    // Spoken final lines (e.g., "Allah Hafiz...") require minimum 7500ms for safe complete delivery
    const requiresSpokenFinalLine =
      reason === 'customer_goodbye' ||
      reason === 'agent_closing' ||
      reason === 'no_customer_audio_after_pickup' ||
      reason === 'silence_timeout' ||
      reason === 'off_topic_timeout' ||
      reason.startsWith('non_customer');
    if (requiresSpokenFinalLine && delayMs < 7800) delayMs = 7800;
    if (requiresSpokenFinalLine && !finalClosingPromptSent) {
      requestFinalClosingLine(`schedule_${reason}`);
    }
    const terminalEndReasons = new Set([
      'agent_closing',
      'customer_goodbye',
      'non_customer',
      'non_customer_wrong_number',
      'non_customer_name_mismatch',
      'non_customer_unavailable',
      'non_customer_relay_msg',
      'abuse_detected',
      'off_topic_timeout',
      'silence_timeout',
      'no_customer_audio_after_pickup',
      'negotiation_exhausted',
      'gemini_disconnected',
      'payment_done',
      'manual_end_requested',
      'stream_closed',
    ]);
    const shouldBypassNegotiationLock = terminalEndReasons.has(reason);
    // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ NEGOTIATION LOCK: Block hangup during active PTP negotiation ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
    if (!shouldBypassNegotiationLock && ptpNegotiationActive && ptpNegotiationAttempts < MAX_PTP_NEGOTIATION_ATTEMPTS) {
      log.warn('MEDIA', `[NEGOTIATION-LOCK] Blocked hangup (reason=${reason}) ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â negotiation active (attempt ${ptpNegotiationAttempts}/${MAX_PTP_NEGOTIATION_ATTEMPTS})`);
      broadcast({ type: 'LOG', message: `ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬â„¢ Hangup blocked ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â negotiation in progress (${ptpNegotiationAttempts}/${MAX_PTP_NEGOTIATION_ATTEMPTS})`, source: 'sys' });
      return; // Do NOT hang up ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â customer is still arguing
    }
    closingRequested = true;
    cancelGreetingRetryTimer();
    clearAgentWatchdog();
    if (silenceTimer) {
      clearTimeout(silenceTimer);
      silenceTimer = null;
    }
    if (currentConversationState !== 'CLOSING') {
      currentConversationState = 'CLOSING';
      broadcast({ type: 'STATE_CHANGE', state: 'CLOSING' });
    }
    // Reflect ending state in DB immediately so frontend timer stops quickly.
    try {
      if (activeConversationId) {
        db.prepare("UPDATE call_conversations SET status='ending', updated_at=datetime('now') WHERE id=?").run(activeConversationId);
      }
    } catch (e) {
      log.warn('TIMING', `Failed to mark conversation as ending: ${e.message}`);
    }
    setEndCause('system', reason, { callSid, streamSid }, false);
    clearGracefulHangupTimer();
    
    // For non-customer flows, add a hard timeout safety net (1s longer than graceful delay)
    const isNonCustomerReason = reason.startsWith('non_customer');
    const hardTimeoutMs = isNonCustomerReason ? delayMs + 1000 : delayMs + 500;
    
    let geminiClosingAttempts = 0;
    const maxGeminiCloseAttempts = isNonCustomerReason ? 3 : 1;
    
    // Function to safely close Gemini WebSocket with retry logic
    const closeGeminiWithFallback = () => {
      geminiClosingAttempts++;
      if (geminiClosingAttempts <= maxGeminiCloseAttempts) {
        if (wsGemini && wsGemini.readyState === WebSocket.OPEN) {
          try {
            wsGemini.close();
            log.info('MEDIA', `[GEMINI-CLOSE] Closed Gemini WS (attempt ${geminiClosingAttempts}/${maxGeminiCloseAttempts}, reason=${reason})`);
          } catch (err) {
            log.warn('MEDIA', `[GEMINI-CLOSE-ERROR] Failed to close: ${err.message} (attempt ${geminiClosingAttempts})`);
            if (geminiClosingAttempts < maxGeminiCloseAttempts) {
              // Retry in 200ms
              setTimeout(closeGeminiWithFallback, 200);
            }
          }
        }
      }
    };
    
    // Requirement: close Gemini exactly 1 second after graceful ending begins.
    const geminiCloseDelayMs = Math.max(1800, Math.min(3200, delayMs - 1800));
    setTimeout(() => {
      if (conversationEnded || forceEndingCall) return;
      closeGeminiWithFallback();
    }, geminiCloseDelayMs);
    
    // Schedule graceful hangup with audio-drain enforcement.
    // If recent TTS audio is still flowing, extend briefly so customer hears complete goodbye.
    let drainExtensions = 0;
    const maxDrainExtensions = 3;  // Allow up to 3 drain extensions for longer messages
    const runGracefulFinalize = () => {
      gracefulHangupTimer = null;
      const msSinceLastAudio = Date.now() - lastGeminiSendAt;
      // Extend if audio arrived within last 1200ms (was 900ms) to give TTS extra time
      if (msSinceLastAudio < 1200 && drainExtensions < maxDrainExtensions && !conversationEnded && !forceEndingCall) {
        const extraMs = 1200 - msSinceLastAudio + 300;  // More generous drain extension
        drainExtensions += 1;
        log.info('TIMING', `[GRACEFUL] Extending hangup for audio drain (+${extraMs}ms, ext ${drainExtensions}/${maxDrainExtensions})`);
        broadcast({ type: 'LOG', message: `[AUDIO-DRAIN] Waiting for TTS to complete (ext ${drainExtensions}/${maxDrainExtensions})`, source: 'system' });
        gracefulHangupTimer = setTimeout(runGracefulFinalize, extraMs);
        return;
      }

      log.info('TIMING', `[GRACEFUL] Hangup timer expired (reason=${reason}, delayMs=${delayMs}, drainExt=${drainExtensions}, timeSinceLastAudio=${msSinceLastAudio}ms)`);
      forceCompleteLiveCall(reason).catch(() => {});
    };

    gracefulHangupTimer = setTimeout(runGracefulFinalize, delayMs);
    
    // Hard timeout to force hangup if graceful doesn't work
    setTimeout(() => {
      if (!conversationEnded && !forceEndingCall) {
        log.warn('TIMING', `[HARD-TIMEOUT] Graceful hangup exceeded ${hardTimeoutMs}ms ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â forcing immediate close (reason=${reason})`);
        forceCompleteLiveCall(`${reason}_force_timeout`).catch(() => {});
      }
    }, hardTimeoutMs);
  }

  function sendAudioPacketToGemini(muLawBuffer) {
    // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ CRITICAL: Stop feeding audio to Gemini once call is ending ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
    if (conversationEnded || forceEndingCall || closingRequested) return false;
    // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ EXTRA SAFETY: If non-customer flow is progressing to close, stop audio asap ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
    if (nonCustomerIntentLocked && (currentConversationState === 'NON_CUSTOMER' || closingRequested)) return false;
    if (!muLawBuffer?.length || !wsGemini || wsGemini.readyState !== WebSocket.OPEN) return false;

    const pcmBuffer = decodeTwilioToGemini(muLawBuffer);
    wsGemini.send(JSON.stringify({
      realtime_input: {
        media_chunks: [{
          mime_type: 'audio/pcm;rate=24000',
          data: pcmBuffer.toString('base64')
        }]
      }
    }));
    lastGeminiSendAt = Date.now();
    return true;
  }

  function flushBufferedInboundAudio(trigger = 'setup_complete') {
    if (!geminiSetupComplete || !wsGemini || wsGemini.readyState !== WebSocket.OPEN || !preSetupAudioBuffer.length) {
      return 0;
    }

    const packets = preSetupAudioBuffer.splice(0, preSetupAudioBuffer.length);
    let flushed = 0;

    for (const packet of packets) {
      try {
        if (sendAudioPacketToGemini(packet)) flushed += 1;
      } catch (err) {
        log.warn('MEDIA', `Buffered audio flush failed: ${err.message}`);
        break;
      }
    }

    if (flushed > 0) {
      log.info('MEDIA', `Flushed ${flushed} buffered inbound audio packets (${trigger})`);
      broadcast({ type: 'LOG', message: `GEMINI: flushed ${flushed} buffered audio packets`, source: 'gem' });
    }

    return flushed;
  }

  function shouldReconnectGemini() {
    if (conversationEnded || forceEndingCall || closingRequested) return false;
    if (!twilioWs || twilioWs.readyState !== WebSocket.OPEN) return false;
    if (geminiReconnectAttempts >= MAX_GEMINI_RECONNECTS) return false;
    // Always allow reconnect if call is still active
    return true;
  }

  function scheduleGeminiReconnect(reason = 'bridge_reconnect') {
    if (!shouldReconnectGemini()) return false;
    geminiReconnectAttempts += 1;
    geminiRecovering = true;
    geminiSetupComplete = false;

    const waitMs = Math.min(700, 150 * geminiReconnectAttempts);
    log.warn('MEDIA', `Gemini reconnect scheduled (${geminiReconnectAttempts}/${MAX_GEMINI_RECONNECTS}) in ${waitMs}ms ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â ${reason}`);
    broadcast({ type: 'LOG', message: `AI reconnecting (${geminiReconnectAttempts}/${MAX_GEMINI_RECONNECTS})...`, source: 'sys' });

    setTimeout(() => {
      if (conversationEnded || forceEndingCall || closingRequested || twilioWs.readyState !== WebSocket.OPEN) return;
      startEarlyGeminiConnection({ forceFresh: true, reason });
    }, waitMs);

    return true;
  }

  function canSafelyAutoHangup(nextState = currentConversationState) {
    return nextState === 'NON_CUSTOMER' || callAdvancedPastGreeting || customerTurnsHeard >= 1;
  }

  async function forceCompleteLiveCall(reason = 'agent_close') {
    forceEndingCall = true;
    closingRequested = true;
    conversationEnded = true;

    try {
      if (activeConversationId) {
        db.prepare("UPDATE call_conversations SET status='ending', updated_at=datetime('now') WHERE id=?").run(activeConversationId);
      }
    } catch (e) {
      log.warn('TIMING', `forceCompleteLiveCall could not set ending status: ${e.message}`);
    }
    cancelGreetingRetryTimer();
    if (silenceTimer) clearTimeout(silenceTimer);
    clearGracefulHangupTimer();
    setEndCause('system', reason, { callSid, streamSid }, true);
    
    // Enhanced logging for non-customer flows
    const callEndLog = `[END] Force-completing call ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â reason: ${reason}, convId: ${activeConversationId}, callSid: ${callSid}, elapsed: ${Date.now()-T0}ms, customerTurns: ${customerTurnsHeard}, state: ${currentConversationState}`;
    const nonCustomerLog = nonCustomerIntentLocked 
      ? `, [NON-CUSTOMER] phase: ${lastNonCustomerPhase}, declaredName: ${lastNonCustomerDeclaredName || 'none'}`
      : '';
    log.info('TIMING', callEndLog + nonCustomerLog);

    // 1) Clear Twilio audio buffer immediately (stop any queued audio)
    try {
      if (streamSid && twilioWs.readyState === WebSocket.OPEN) {
        twilioWs.send(JSON.stringify({ event: 'clear', streamSid }));
      }
    } catch {}

    // 2) Hang up Twilio call FIRST ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â customer hears nothing more
    const accountSid = process.env.TWILIO_ACCOUNT_SID;
    const authToken = process.env.TWILIO_AUTH_TOKEN;
    if (callSid && accountSid && authToken && !callSid.startsWith('MOCK_')) {
      const MAX_HANGUP_RETRIES = 3;
      for (let attempt = 1; attempt <= MAX_HANGUP_RETRIES; attempt++) {
        try {
          const controller = new AbortController();
          const timeout = setTimeout(() => controller.abort(), 5000);
          const twilioRes = await fetch(`https://api.twilio.com/2010-04-01/Accounts/${accountSid}/Calls/${callSid}.json`, {
            method: 'POST',
            headers: {
              'Authorization': 'Basic ' + Buffer.from(`${accountSid}:${authToken}`).toString('base64'),
              'Content-Type': 'application/x-www-form-urlencoded',
            },
            body: new URLSearchParams({ Status: 'completed' }).toString(),
            signal: controller.signal,
          });
          clearTimeout(timeout);
          const twilioBody = await twilioRes.text();
          if (twilioRes.ok || twilioRes.status === 404) {
            log.info('TIMING', `[END] ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ Twilio call completed (attempt ${attempt}): ${callSid} status=${twilioRes.status}`);
            break;
          } else {
            log.warn('MEDIA', `Force-complete Twilio API failed attempt ${attempt}/${MAX_HANGUP_RETRIES} (${twilioRes.status}) body=${twilioBody.slice(0, 200)}`);
            if (attempt < MAX_HANGUP_RETRIES) await new Promise(r => setTimeout(r, 500 * attempt));
          }
        } catch (err) {
          log.warn('MEDIA', `Force-complete exception attempt ${attempt}/${MAX_HANGUP_RETRIES}: ${err.message}`);
          if (attempt < MAX_HANGUP_RETRIES) await new Promise(r => setTimeout(r, 500 * attempt));
        }
      }
    } else {
      log.warn('MEDIA', `ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â Cannot hangup ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â missing TWILIO_ACCOUNT_SID or TWILIO_AUTH_TOKEN`);
    }

    // 3) Run analysis + DB updates BEFORE closing Gemini
    log.info('TIMING', '[END] Running post-call analysis before closing Gemini...');
    await handleCallEnd();

    // 4) Gemini socket is already closed by handleCallEnd's finally block
    log.info('TIMING', '[END] Post-call analysis + Gemini cleanup done');

    // 5) Close Twilio WebSocket
    setTimeout(() => {
      try { if (twilioWs.readyState === WebSocket.OPEN) twilioWs.close(); } catch {}
    }, 600);
  }

  function requestPickupRecovery(trigger) {
    if (!wsGemini || wsGemini.readyState !== WebSocket.OPEN || !geminiSetupComplete) return false;
    if (pickupRecoveryAttempts >= 2) return false;

    pickupRecoveryAttempts += 1;
    const custText = lastCustomerText || customerTranscriptBuffer.trim() || '';
    const prompt = isShortPickupReply(custText)
      ? `Customer already greeted you by saying "${custText}". Do NOT repeat full greeting. Say EXACTLY ONE natural Roman Urdu sentence: "Walaikum Assalam ${conv?.customer_name || 'Sahab'}, kya ye munasib waqt hai aapke credit card dues par baat karne ke liye?" Do NOT say "Ji theek hai". Do not add a second sentence. Do not end the call. Do not say Allah Hafiz.`
      : `Customer spoke but may not have heard you clearly. Say your name and bank in ONE short sentence, then go straight to consent. Do NOT repeat the full greeting. Do not end the call. Do not say Allah Hafiz.`;

    wsGemini.send(JSON.stringify({
      client_content: {
        turns: [{
          role: 'user',
          parts: [{ text: prompt }],
        }],
        turn_complete: true,
      },
    }));

    log.warn('MEDIA', `[FLOW-GUARD] Forced re-engagement (${trigger})`, {
      pickupRecoveryAttempts,
      customerTurnsHeard,
      lastCustomerText,
      currentConversationState,
    });
    broadcast({ type: 'LOG', message: `FLOW GUARD: re-engagement ${pickupRecoveryAttempts} (${trigger})`, source: 'sys' });
    return true;
  }

  // Filter out Gemini's internal thinking/reasoning text from transcripts
  function isThinkingText(text) {
    const t = text.trim();
    // Quick length check ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â genuine Roman Urdu agent speech is short; thinking blocks are long English
    const englishWordCount = (t.match(/\b[A-Za-z]{4,}\b/g) || []).length;
    const totalWordCount = t.split(/\s+/).length;
    // If >80% of words are 4+ letter English words and text is long, it's thinking
    if (totalWordCount > 12 && englishWordCount / totalWordCount > 0.7) return true;

    const thinkingPatterns = [
      /^(Initiating|Refining|Confirming|Analyzing|Acknowledging|Concluding|Clarifying|Planning|Preparing|Processing|Crafting|Finalizing|Registering|Working|Setting|Evaluating|Assessing|Reviewing|Formulating|Structuring|Considering|Determining|Calculating|Translating|Detailing|Acknowledge|Outlining|Summarizing|Moving|Noting|Switching|Proceeding|Re-[Ee]ngaging|Addressing|Steering|Implementing|Observing|Redirecting|Handling|Detecting|Securing|Extracting|Transitioning|Wrapping)/i,
      /I've (crafted|finalized|registered|noted|translated|grasped|completed|acknowledged|understood|recognized|calculated|checked|determined|noticed|decided|identified)/i,
      /My (focus|emphasis|next move|priority|current priority|plan|intent|intention|approach|goal|strategy) (now |has |is |shifts|will )/i,
      /I'm (mentally|also|now|currently|preparing|ready|mindful|working|planning|setting|focusing|aiming|going to|about to)/i,
      /I am also (prepared|planning|ready)/i,
      /Now I'm (preparing|setting|structuring)/i,
      /Therefore, I/i,
      /I will (now |ensure |offer |employ |end |steer |redirect |attempt )/i,
      /Per the flow/i,
      /My next step/i,
      /I've outlined/i,
      /the desired tone/i,
      /remembering the \d+-day/i,
      /I'll (ask|move|proceed|continue|now)/i,
      /I'm aiming for/i,
      /moving on to the/i,
      /(consent|disclosure|collection|negotiation|closing) phase/i,
      /I (noticed|intend|must|need to|should|have to) (steer|redirect|get|secure|extract|ask|remind|push|move|proceed|ensure|verify|confirm)/i,
      /linguistic protocol/i,
      /mandatory rules/i,
      /Approach [A-Z] (or |will )/i,
      /To get a firm commitment/i,
      /audio quality is/i,
      /speech unintelligible/i,
      /I've determined/i,
      /rendering their speech/i,
    ];
    return thinkingPatterns.some(p => p.test(t));
  }

  function pushMessage(role, text, options = {}) {
    const { preserveTranscriptText = false } = options;
    let cleanText = '';
    try {
      cleanText = preserveTranscriptText
        ? String(text || '').replace(/\s+/g, ' ').trim()
        : normalizeTranscriptToRomanUrdu(text);
    } catch (normErr) {
      cleanText = String(text || '').trim();
      log.warn('MEDIA', `Transcript normalization fallback used: ${normErr?.message || normErr}`);
    }
    if (!cleanText) return null;

    // Filter out Gemini thinking/reasoning leaks from agent messages
    if (role === 'agent' && isThinkingText(cleanText)) {
      log.info('MEDIA', `ÃƒÂ°Ã…Â¸Ã‚Â§Ã‚Â  Filtered thinking text: "${cleanText.substring(0, 60)}..."`);
      return null;
    }

    // Filter very short noise fragments from customer (e.g. "Did you", single words)
    if (role === 'customer') {
      const wordCount = cleanText.split(/\s+/).length;
      // Fragments ÃƒÂ¢Ã¢â‚¬Â°Ã‚Â¤2 words that are NOT valid short replies get merged into previous message
      if (wordCount <= 2 && !isShortPickupReply(cleanText)) {
        const last = messages[messages.length - 1];
        if (last?.role === 'customer') {
          // Merge into previous customer message
          const mergedAt = new Date(last.timestamp);
          const now = new Date();
          // Only merge if within 8 seconds of last customer message
          if (now.getTime() - mergedAt.getTime() < 8000) {
            last.text = `${last.text} ${cleanText}`.trim();
            last.timestamp = now.toISOString();
            appendTranscriptLine('Customer', cleanText);
            broadcast({ type: 'TRANSCRIPTION', role: 'user', text: last.text });
            log.info('MEDIA', `ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â Merged fragment into previous: "${last.text}"`);
            return cleanText;
          }
        }
      }
    }

    // Merge consecutive same-role messages within 5 seconds
    const last = messages[messages.length - 1];
    if (last?.role === role && last?.text === cleanText) return null;
    if (last?.role === role) {
      const lastAt = new Date(last.timestamp);
      const now = new Date();
      if (now.getTime() - lastAt.getTime() < 5000) {
        const lastNorm = fuzzy(last.text);
        const currNorm = fuzzy(cleanText);
        // Drop near-duplicate echo chunks instead of merging repeated greeting/transcript text.
        if (lastNorm === currNorm || lastNorm.includes(currNorm) || currNorm.includes(lastNorm)) {
          return cleanText;
        }
        // Merge into previous message (same speaker, within 5s)
        last.text = `${last.text} ${cleanText}`.trim();
        last.text = normalizeTranscriptToRomanUrdu(last.text);
        last.timestamp = now.toISOString();
        appendTranscriptLine(role === 'agent' ? (conv?.caller_name || 'Agent') : 'Customer', cleanText);
        broadcast({ type: 'TRANSCRIPTION', role: role === 'agent' ? 'agent' : 'user', text: last.text });
        return cleanText;
      }
    }
    messages.push({ role, text: cleanText, timestamp: new Date().toISOString() });
    if (role === 'customer') what_customer_said.push(cleanText);
    if (role === 'agent') what_agent_said.push(cleanText);
    appendTranscriptLine(role === 'agent' ? (conv?.caller_name || 'Agent') : 'Customer', cleanText);
    broadcast({ type: 'TRANSCRIPTION', role: role === 'agent' ? 'agent' : 'user', text: cleanText });
    return cleanText;
  }

  // ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
  // SMART DETECTION ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â fuzzy keyword matching (handles millions of spelling combos)
  // ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â

  // Normalize text: strip diacritics, collapse whitespace, unify common letter swaps
  function fuzzy(t) {
    return String(t || '').toLowerCase()
      .replace(/[^\w\s]/g, '')          // remove punctuation
      .replace(/\s+/g, ' ').trim()
      // Common Pakistani Roman Urdu spelling normalization
      .replace(/aa/g, 'a')
      .replace(/ee/g, 'i')
      .replace(/oo/g, 'u')
      .replace(/ph/g, 'f')
      .replace(/gh/g, 'g')
      .replace(/kh/g, 'k')
      .replace(/sh/g, 's')
      .replace(/th/g, 't')
      .replace(/dh/g, 'd');
  }

  // Check if ANY keyword combo matches (word-level)
  function hasAnyKeywords(text, keywordSets) {
    const joined = fuzzy(text);
    return keywordSets.some(set => {
      if (typeof set === 'string') {
        const kw = fuzzy(set);
        // Use word-boundary matching to prevent partial matches (e.g. "salaam" matching "sala")
        const regex = new RegExp(`(?:^|\\s|[^a-z])${kw.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}(?:$|\\s|[^a-z])`, 'i');
        return regex.test(` ${joined} `);
      }
      // Array = ALL keywords must be present (each with word boundary)
      return set.every(kw => {
        const fkw = fuzzy(kw);
        const regex = new RegExp(`(?:^|\\s|[^a-z])${fkw.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}(?:$|\\s|[^a-z])`, 'i');
        return regex.test(` ${joined} `);
      });
    });
  }

  function tokenizeName(raw) {
    return fuzzy(raw)
      .split(' ')
      .map(s => s.trim())
      .filter(s => s.length >= 3);
  }

  function hasExplicitNameMismatch(text, expectedFullName) {
    const normalized = fuzzy(text);
    if (!normalized) return false;

    const expectedTokens = tokenizeName(expectedFullName);
    if (expectedTokens.length === 0) return false;
    const expectedFirst = expectedTokens[0];

    const directNotExpected = new RegExp(`\\b(i am not|im not|main nahi|mai nahi|mein nahi|main nhi|mai nhi|mein nhi)\\s+${expectedFirst}\\b`, 'i');
    if (directNotExpected.test(normalized)) return true;

    const declaredNameMatch = normalized.match(/\\b(my name is|mera naam|i am|im|main hun|main hoon|mai hun|mai hoon|mein hun|mein hoon)\\s+([a-z]{3,})\\b/i);
    if (!declaredNameMatch) return false;

    const declaredName = (declaredNameMatch[2] || '').trim();
    if (!declaredName) return false;

    const ignoreWords = new Set(['not', 'nahi', 'nhi', 'hun', 'hoon', 'main', 'mai', 'mein', 'haan', 'han']);
    if (ignoreWords.has(declaredName)) return false;

    return !expectedTokens.includes(declaredName);
  }

  function hasIdentityConfirmation(text, expectedFullName) {
    const normalized = fuzzy(text);
    if (!normalized) return false;

    const expectedTokens = tokenizeName(expectedFullName);
    const expectedFirst = expectedTokens[0] || '';

    const selfConfirmPatterns = [
      /\b(ji|jee|haan|han)?\s*(main|mai|mein)\s+(hi\s+)?(bol\s+raha|bol\s+rahi|baat\s+kar\s+raha|baat\s+kar\s+rahi|hoon|hun)\b/i,
      /\b(ji|jee|haan|han)?\s*(main|mai|mein)\s+bolta\s+hoon\b/i,
      /\b(ji|jee|haan|han)?\s*(main|mai|mein)\s+bol\s+raha\s+hoon\b/i,
      /\b(ji|jee|haan|han)?\s*(main|mai|mein)\s+bol\s+rahi\s+hoon\b/i,
      /\b(yes|yeah)\s+i\s+am\b/i,
    ];

    if (expectedFirst) {
      selfConfirmPatterns.push(new RegExp(`\\b(ji|jee|haan|han)?\\s*(main|mai|mein)\\s+${expectedFirst}\\b`, 'i'));
      selfConfirmPatterns.push(new RegExp(`\\b(yes|yeah)\\s+i\\s+am\\s+${expectedFirst}\\b`, 'i'));
    }

    return selfConfirmPatterns.some((pattern) => pattern.test(normalized));
  }

  function hasPtpOrPaymentIntent(text) {
    const normalized = fuzzy(text);
    if (!normalized) return false;

    const intentSignals = [
      /\b(ptp|payment|pay)\b/i,
      /\b(kar\s+dunga|kar\s+dungi|de\s+dunga|de\s+dungi|bhej\s+dunga|bhej\s+dungi|kar\s+doonga|kar\s+doongi)\b/i,
      /\b(kal|parson|aaj|aj|tarikh|tareekh|date|din|haft[ea]|mahina|month)\b/i,
      /\b(april|may|june|july|august|september|october|november|december)\b/i,
      /\b\d{1,2}\b/i,
    ];
    return intentSignals.some((p) => p.test(normalized));
  }

  function detectState(text, role = 'agent') {
    const lower = String(text || '').toLowerCase();
    const fz = fuzzy(text);
    let newState = currentConversationState;

    // ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â NON-CUSTOMER DETECTION (customer role only) ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
    // Uses keyword combos so "main nahi hoon", "mn na ehun", "mai nhi hun" ALL match
    const nonCustomerSignals = [
      // Wrong number variants (any spelling)
      ['wrong', 'number'], ['rong', 'number'], ['rong', 'nmbr'], ['wrong', 'nmbr'],
      ['galat', 'number'], ['ghalat', 'number'], ['glt', 'number'], ['glt', 'nmbr'],
      'wrong person', 'wrong call',
      // "Main nahi hoon" ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â keep stronger patterns only (avoid false hits in normal payment talk)
      ['main', 'nahi', 'hun'], ['main', 'nahi', 'hoon'],
      ['mai', 'nahi', 'hun'], ['mai', 'nahi', 'hoon'],
      ['mein', 'nahi', 'hun'], ['mein', 'nahi', 'hoon'],
      ['mn', 'na', 'ehun'],
      ['main', 'na', 'hun'], ['mai', 'na', 'hun'], ['mn', 'na', 'ehun'],
      'main to nahi', 'mai to nahi', 'mein to nahi',
      // "Ye mera number/phone nahi" 
      ['mera', 'number', 'nahi'], ['mera', 'nmbr', 'nahi'], ['mera', 'phone', 'nahi'],
      ['mera', 'number', 'nhi'], ['mera', 'nhi'],
      // "Wo/woh nahi hain"
      ['wo', 'nahi', 'hain'], ['woh', 'nahi', 'hain'], ['wo', 'nhi', 'hain'], ['woh', 'nhi', 'hain'],
      ['unka', 'phone'], ['un', 'ka', 'phone'], ['unka', 'number'], ['un', 'ka', 'number'],
      'unka nahi', 'ye unka nhi', 'ye un ka nahi',
      // Family member pickup
      'bhai hun', 'behan hun', 'beta hun', 'beti hun', 'biwi hun', 'shohar hun',
      'bhai bol', 'behan bol', 'beta bol', 'beti bol',
      'main inka', 'main unka', 'main unki', 'main inki',
      'mera beta', 'meri beti', 'mera shohar', 'meri biwi', 'mera bhai', 'meri behan',
      // Person unavailable
      'wo bahar', 'woh bahar', 'bahar gaye', 'ghar pe nahi', 'ghar par nahi',
      'abhi nahi hain', 'abhi available nahi', 'abhi nahi hai',
      'office mein', 'office gaye', 'office gayi', 'kaam par',
      'wo abhi nahi', 'woh abhi nahi',
      // SIM/number changed
      'sim change', 'number badal', 'purana number', 'number band',
      'ye number ab kisi', 'kisi aur ka', 'kisi aur ki sim',
      // Will relay message
      ['bata', 'denge'], ['bata', 'dungi'], ['bata', 'dunga'], ['message', 'de'],
      ['message', 'nahi', 'de'], ['message', 'nahi', 'de', 'sakta'], ['message', 'nahi', 'de', 'sakti'],
      ['msg', 'nahi', 'de'], ['mai', 'janta', 'nahi'], ['main', 'janta', 'nahi'], ['mein', 'jaanta', 'nahi'],
      'unhe message', 'un ko message', 'hum bata', 'main bata doon',
      // "I don't know"
      'mujhe nahi pata', 'nahi jaanta', 'nahi jaanti', 'pata nahi', 'main janta hee nahi hoon',
      // English fallbacks
      'who is this for', 'not available', 'he is not here', 'she is not here',
    ];

    // ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â CUSTOMER GOODBYE DETECTION ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
    const goodbyeSignals = [
      'allah hafiz', 'alah hafiz', 'ala hafiz', 'allahafiz',
      'khuda hafiz', 'kudahafiz', 'kuda hafiz',
      'goodbye', 'good bye', 'bye bye', 'byebye', 'bye',
      'ok bye', 'theek hai bye', 'achha bye', 'acha bye',
      'shukriya bye', 'shukria bye',
      'rakhti hoon', 'rakhta hoon', 'rakh rahi hoon', 'rakh raha hoon',
      'phone rakh', 'call rakh', 'call band', 'call khatam',
      'mat karo call', 'dobara mat call',
      'hang up', 'cut karo', 'disconnect',
    ];

    // ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â ABUSE/GAALI DETECTION ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
    const abuseSignals = [
      // Common Pakistani/Urdu abuse words (romanized variants)
      'bhenchod', 'bhen chod', 'bc', 'bhenchd', 'bhnchod',
      'madarchod', 'madar chod', 'mc', 'madarchd',
      'chutiya', 'chutia', 'chtiya', 'chtyia',
      'gandu', 'gando', 'gandy',
      'haramkhor', 'haram khor', 'harami', 'haramy',
      'kuttay', 'kutay', 'kutta', 'kutti', 'kutiya', 'kutia',
      'kamina', 'kamini', 'kameena', 'kameeni',
      'sala', 'saala', 'saali', 'sali',
      'ullu', 'ullu ka', 'bewakoof', 'bewaqoof',
      'pagal', 'paagal', 'pagla',
      'bakwas', 'bakwass', 'bakvas',
      'badtameez', 'bad tameez', 'badtamiz',
      'chor', 'daku', 'fraud',
      'nikal', 'dur ho', 'door ho', 'hat ja', 'hatja',
      'kaminey', 'kameenay',
      'fuck', 'fuck off', 'shit', 'asshole', 'bastard', 'idiot',
      'shut up', 'shutup',
    ];

    // ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â AGENT CLOSING PHRASES ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
    const explicitClosingPhrases = [
      'allah hafiz', 'khuda hafiz', 'goodbye', 'good bye',
    ];

    // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Run detection (customer messages only for non-customer/goodbye/abuse) ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬

    if (role === 'customer') {
      if (suppressCustomerInputDuringEnding) {
        log.info('MEDIA', `[SKIP] Customer input ignored during ending playback: "${text}"`);
        return;
      }
      // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ GUARD: If call is already ending, skip all detection ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
      if (conversationEnded || forceEndingCall || (closingRequested && !ptpNegotiationActive)) {
        log.info('MEDIA', `[SKIP] Ignoring customer text ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â call already ending: "${text}"`);
        return;
      }
      // Helper vars for negotiation prompts
      const callerGenderLocal = resolveCallerGender(
        callAssets?.caller_gender || conv?.caller_gender || conv?.callerGender,
        callAssets?.caller_name || conv?.caller_name || conv?.callerName || 'Omar',
        'male'
      );
      const maxPtpDateLocal = dateToNaturalUrdu(new Date(Date.now() + (callAssets?.maxPtpDays || 5) * 86400000));

      // 1. ABUSE ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â don't disconnect, try to calm and redirect to PTP (up to 3 attempts)
      if (hasAnyKeywords(text, abuseSignals)) {
        if (!abuseAttempts) abuseAttempts = 0;
        abuseAttempts++;
        log.info('TIMING', `[ABUSE] Detected (attempt ${abuseAttempts}/3): "${text}"`);
        broadcast({ type: 'LOG', message: `ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â Abusive language detected (attempt ${abuseAttempts}/3) ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â calming customer`, source: 'sys' });

        if (abuseAttempts >= 3) {
          // After 3 attempts, end the call
          newState = 'CLOSING';
          closingRequested = true;
          log.info('TIMING', `[ABUSE] 3 attempts exhausted ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â ending call`);
          broadcast({ type: 'LOG', message: 'ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â Customer persistently abusive after 3 attempts ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â ending call', source: 'sys' });
          if (wsGemini && wsGemini.readyState === WebSocket.OPEN && geminiSetupComplete) {
            wsGemini.send(JSON.stringify({
              client_content: {
                turns: [{ role: 'user', parts: [{ text: 'Customer is very upset after 3 attempts. End call professionally. Say ONLY: "Sir/Madam, main samajhta/samajhti hoon aap pareshaan hain. Hum aapko baad mein call karenge. Allah Hafiz. [END_CALL]"' }] }],
                turn_complete: true,
              },
            }));
          }
          scheduleGracefulHangup('abuse_detected', 4200);
        } else {
          // Calm and redirect ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â DO NOT end call
          if (wsGemini && wsGemini.readyState === WebSocket.OPEN && geminiSetupComplete) {
            const calmPrompts = [
              'Customer is upset and used harsh words. Stay calm and professional. Say something like: "Sir/Madam, main samajhta/samajhti hoon aap pareshaan hain. Main aapki madad karne ke liye call kar raha/rahi hoon. Agar aap sirf [amount] rupees ka payment kar dein to aapka masla hal ho jayega. Kya aap kal ya parson payment kar sakte hain?" Keep trying to secure a PTP. Do NOT end the call. Do NOT mention their language.',
              'Customer is still upset. Be empathetic but persistent. Say: "Sir/Madam, main aapki baat samajh raha/rahi hoon. Bas aapko ek chhoti si payment karni hai. Agar aap 2-3 din mein payment kar dein to koi aur action nahi hoga. Batayein kab convenient hoga?" Stay professional, secure PTP.',
            ];
            wsGemini.send(JSON.stringify({
              client_content: {
                turns: [{ role: 'user', parts: [{ text: calmPrompts[Math.min(abuseAttempts - 1, calmPrompts.length - 1)] }] }],
                turn_complete: true,
              },
            }));
          }
        }
      }
      // 2. NON-CUSTOMER
      else {
        const nonCustomerKeywordHit = hasAnyKeywords(text, nonCustomerSignals);
        const explicitNameMismatch = hasExplicitNameMismatch(text, conv?.customer_name || '');
        const identityConfirmed = hasIdentityConfirmation(text, conv?.customer_name || '');
        const paymentIntentDetected = hasPtpOrPaymentIntent(text);
        const callbackIntentDetected = detectCallbackRequestedSemantic(text);
        const refusalIntentDetected = /\b(nahi\s+doonga|nahi\s+dunga|nahi\s+doongi|nahi\s+dungi|nahi\s+karunga|nahi\s+karungi|refuse)\b/i.test(lower);

        if (nonCustomerConfirmPending && identityConfirmed) {
          nonCustomerConfirmPending = false;
          nonCustomerIntentLocked = false;
          lastNonCustomerPhase = 'generic';
          lastNonCustomerDeclaredName = '';
          log.info('TIMING', `[NON-CUSTOMER] Customer confirmed identity ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â returning to live flow: "${text}"`);
          broadcast({ type: 'LOG', message: 'Identity confirmed ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â continuing normal call flow', source: 'sys' });
        }

        // If customer gives payment/date intent, never treat this turn as non-customer unless there is explicit mismatch.
        const shouldTreatAsNonCustomer =
          !identityConfirmed &&
          !callbackIntentDetected &&
          !refusalIntentDetected &&
          (explicitNameMismatch || (nonCustomerKeywordHit && !paymentIntentDetected));

        if (shouldTreatAsNonCustomer) {
          nonCustomerIntentLocked = true;
        const nonCustomerContextText = fuzzy(`${lastCustomerText || ''} ${text || ''}`);
        const explicitWrongNumber = /\b(wrong\s*number|rong\s*number|wrng\s*number|galat\s*number|ghalat\s*number|dial\s*ho\s*gaya|dial\s*kr\s*diya)\b/i.test(nonCustomerContextText);
        const familyMember = /\b(bhai|behan|beta|beti|biwi|shohar|husband|wife|son|daughter|father|mother)\b/i.test(lower);
        const messagingOffer = /\b(bata|message|msg|sambha|doon|dega|degi|denge|dungi|dunga)\b/i.test(lower);
        const unavailable = /\b(bahar|office|kaam|travel|dusri|nahi\s+hain|nahi\s+hai|available\s+nahi)\b/i.test(lower);
        
        // Track non-customer phase for targeted closing
        const nonCustomerPhase = explicitWrongNumber ? 'wrong_number' : explicitNameMismatch ? 'name_mismatch' : familyMember ? 'family_member' : messagingOffer ? 'relay_message' : unavailable ? 'unavailable' : 'generic';
        lastNonCustomerPhase = nonCustomerPhase;
        
        // Try to extract explicitly declared name (e.g., "main Aslam hoon")
        const nameMatch = text.match(/\b(?:main|mai|mein|i\s+am|im)\s+([a-z]+)\s+(?:hoon|hoon|hun|am)\b/i);
        if (nameMatch) {
          lastNonCustomerDeclaredName = nameMatch[1];
        }
        
        const skipConfirmation = explicitWrongNumber || explicitNameMismatch || familyMember;

        if (!nonCustomerConfirmPending && !skipConfirmation) {
          nonCustomerConfirmPending = true;
          log.info('TIMING', `[NON-CUSTOMER] Candidate (phase=${nonCustomerPhase}) detected, asking one-step confirmation: "${text}"`);
          broadcast({ type: 'LOG', message: `ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â Non-customer candidate (${nonCustomerPhase}) ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â confirming once before ending`, source: 'sys' });

          if (wsGemini && wsGemini.readyState === WebSocket.OPEN && geminiSetupComplete) {
            wsGemini.send(JSON.stringify({
              client_content: {
                turns: [{ role: 'user', parts: [{ text: `Customer MAY be non-account-holder. Ask ONE short confirmation only: "Maazrat, kya aap ${conv?.customer_name || 'Sahab'} bol rahe hain?" If they again indicate they are not the customer, then leave message and end call politely with [END_CALL]. Do NOT disclose financial details.` }] }],
                turn_complete: true,
              },
            }));
          }
        } else {
          newState = 'NON_CUSTOMER';
          log.info('TIMING', `[NON-CUSTOMER] CONFIRMED (phase=${nonCustomerPhase}, declaredName=${lastNonCustomerDeclaredName || 'none'}) ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â delivering close script`);
          closingRequested = true;
          nonCustomerConfirmPending = false;
          broadcast({ type: 'LOG', message: `ÃƒÂ¢Ã‚ÂÃ…â€™ Non-customer (${nonCustomerPhase}) confirmed ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â leaving graceful closing message`, source: 'sys' });

          if (wsGemini && wsGemini.readyState === WebSocket.OPEN && geminiSetupComplete) {
            // Dynamically select closing script based on non-customer phase
            let closingScript = '';
            if (nonCustomerPhase === 'wrong_number') {
              closingScript = `Deliver non-customer wrong number closing: "Maazrat, lagta hai galat number mil gaya. Main koshish karunga/karungi dobara. Shukriya aur kshama chahta/chahti hoon. Allah Hafiz. [END_CALL]"`;
            } else if (nonCustomerPhase === 'name_mismatch') {
              const declaredName = lastNonCustomerDeclaredName || 'wo';
              closingScript = `Deliver non-customer name mismatch closing: "Main samajh gaya/gai ke aap ${declaredName} Sahab/Bibi hain na. Theek hai, main ${conv?.customer_name || 'Sahab'} ke liye message chhod raha/rahi hoon. Agar woh reply dena chahein to main dobara call kar sakta/sakti hoon. Shukriya. Allah Hafiz. [END_CALL]"`;
            } else if (nonCustomerPhase === 'family_member') {
              closingScript = `Deliver family member message: "Theek hai, koi baat nahi. Main ${conv?.customer_name || 'Sahab'} ko zaroor message de dunga/dungi ke JS Bank se call aayi hai. Agar woh contact karna chahein to hum ready hain. Shukriya. Allah Hafiz. [END_CALL]"`;
            } else if (nonCustomerPhase === 'relay_message') {
              closingScript = `Deliver message relay closing: "Bilkul, main ${conv?.customer_name || 'Sahab'} ko zaroor bata dunga/dungi. Aapka message ho gaya. Agar woh kuch kehna chahein to hum dobara call kar denge. Shukriya. Allah Hafiz. [END_CALL]"`;
            } else if (nonCustomerPhase === 'unavailable') {
              closingScript = `Deliver unavailable person closing: "Theek hai, ${conv?.customer_name || 'Sahab'} unavailable hain right now. Main dobara try kar lunga/lungi later. Shukriya. Allah Hafiz. [END_CALL]"`;
            } else {
              closingScript = `Deliver standard non-customer closing: "Theek hai, koi baat nahi. Main ${conv?.customer_name || 'Sahab'} ke liye message chhod raha/rahi hoon ke JS Bank se important call aayi thi. Shukriya aur kshama chahta/chahti hoon. Allah Hafiz. [END_CALL]"`;
            }
            wsGemini.send(JSON.stringify({
              client_content: {
                turns: [{ role: 'user', parts: [{ text: closingScript }] }],
                turn_complete: true,
              },
            }));
          }
          // Vary timeout based on phase (more time for message relay confirmation)
          const nonCustomerCloseDelay = nonCustomerPhase === 'relay_message' ? 9000 : nonCustomerPhase === 'name_mismatch' ? 8400 : 7800;
          scheduleGracefulHangup(`non_customer_${nonCustomerPhase}`, nonCustomerCloseDelay);
        }
        }
        // 3. GOODBYE ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â but NOT during active PTP negotiation
        else if (hasAnyKeywords(text, goodbyeSignals)) {
        const strongGoodbyeSignal = /\b(allah\s+hafiz|khuda\s+hafiz|good\s*bye|bye\s+bye|ok\s+bye|phone\s+rakh|call\s+band|hang\s*up|disconnect)\b/.test(lower);
        const weakSingleBye = !strongGoodbyeSignal && /\bbye\b/.test(lower);
        if (weakSingleBye && !callAdvancedPastGreeting && customerTurnsHeard < 2) {
          log.info('MEDIA', `[GOODBYE-GUARD] Ignoring weak early goodbye-like token during pickup/noise: "${text}"`);
          return;
        }
        // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ NEGOTIATION LOCK: If negotiation is active and under limit, suppress goodbye ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
        if (ptpNegotiationActive && ptpNegotiationAttempts < MAX_PTP_NEGOTIATION_ATTEMPTS) {
          log.warn('MEDIA', `[NEGOTIATION-LOCK] Suppressed customer goodbye during active negotiation (attempt ${ptpNegotiationAttempts}/${MAX_PTP_NEGOTIATION_ATTEMPTS}): "${text}"`);
          broadcast({ type: 'LOG', message: `ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬â„¢ Customer goodbye blocked ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â negotiation active`, source: 'sys' });
          // Send Gemini instruction to keep negotiating instead of ending
          if (wsGemini && wsGemini.readyState === WebSocket.OPEN && geminiSetupComplete) {
            wsGemini.send(JSON.stringify({
              client_content: {
                turns: [{ role: 'user', parts: [{ text: `Customer is trying to end the call without giving a payment date within 5 days. Do NOT say goodbye. Do NOT say Allah Hafiz. Do NOT use [END_CALL]. Instead, use one of these approaches to keep them engaged:
- "Ek second ${conv?.customer_name || 'Sahab'}, sirf date confirm kar dein phir main phone rakh ${callerGenderLocal === 'female' ? 'dungi' : 'dunga'}. Kal ya parson?"
- "Bas ek baat ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â payment date de dein phir aapka time nahi ${callerGenderLocal === 'female' ? 'lungi' : 'lunga'}."
- "Ruk jayein please ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â ye important hai aapke liye. Agar payment ${maxPtpDateLocal} tak ho jaye to koi charges nahi."
Stay on the call. Keep negotiating. This is attempt ${ptpNegotiationAttempts}/${MAX_PTP_NEGOTIATION_ATTEMPTS}.` }] }],
                turn_complete: true,
              },
            }));
          }
        } else {
          newState = 'CLOSING';
          closingRequested = true;
          log.info('TIMING', `[GOODBYE] Customer said: "${text}" ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â ending call`);
          broadcast({ type: 'LOG', message: `ÃƒÂ°Ã…Â¸Ã¢â‚¬ËœÃ¢â‚¬Â¹ Customer goodbye: "${text}"`, source: 'sys' });

          if (streamSid && twilioWs.readyState === WebSocket.OPEN) {
            try { twilioWs.send(JSON.stringify({ event: 'clear', streamSid })); } catch {}
          }
          if (wsGemini && wsGemini.readyState === WebSocket.OPEN && geminiSetupComplete) {
            wsGemini.send(JSON.stringify({
              client_content: {
                turns: [{ role: 'user', parts: [{ text: 'Customer just said goodbye. Respond with ONLY: "Shukriya. Allah Hafiz. [END_CALL]". No extra words.' }] }],
                turn_complete: true,
              },
            }));
          }
          scheduleGracefulHangup('customer_goodbye', 5200);
        }
        }
        // 4. BEYOND-PTP DATE DETECTION + OFF-TOPIC / TIME-WASTING detection
        else {
        if (nonCustomerConfirmPending) nonCustomerConfirmPending = false;
        // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Check if customer is requesting a date beyond 5-day PTP window ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
        const beyondPtpPatterns = [
          'next week', 'agle hafte', 'agle mahine', 'next month', 'month end', 'end of month',
          'ek mahina', '1 mahina', 'one month', 'mahina', 'maheena', 'next mahina',
          'das din', '10 din', 'pandrah din', '15 din', 'bees din', '20 din',
          'do hafte', '2 hafte', 'teen hafte', '3 hafte',
          'mahine ki pehli', 'mahine ki aakhir',
        ];
        // Check for specific dates > 5 days from now
        const numericBeyondMatch = lower.match(/(\d{1,2})\s*(ko|tarikh|tarik|april|may|june|july|march|february|january|august|september|october|november|december)/);
        let isBeyondPtpRequest = beyondPtpPatterns.some(p => lower.includes(p));
        
        if (!isBeyondPtpRequest && numericBeyondMatch) {
          const dayNum = parseInt(numericBeyondMatch[1]);
          const monthNames = { january: 0, february: 1, march: 2, april: 3, may: 4, june: 5, july: 6, august: 7, september: 8, october: 9, november: 10, december: 11 };
          const mo = monthNames[numericBeyondMatch[2]] ?? new Date().getMonth();
          const proposedDate = new Date(new Date().getFullYear(), mo, dayNum);
          if (proposedDate <= new Date()) proposedDate.setMonth(proposedDate.getMonth() + 1);
          const maxAllowed = new Date(Date.now() + 5 * 86400000);
          if (proposedDate > maxAllowed) isBeyondPtpRequest = true;
        }
        
        // Also check Urdu number-based dates (bees ko, pacchees ko etc ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â typically > 5 days)
        const urduBeyondPatterns = ['bees', 'ikkees', 'baais', 'teis', 'chaubees', 'pacchees', 'chabbees', 'sattais', 'atthais', 'untees', 'tees', 'ikattees'];
        if (!isBeyondPtpRequest) {
          for (const up of urduBeyondPatterns) {
            if (lower.includes(up)) {
              // Map to date number and check if beyond 5 days
              const urduToNum = { bees: 20, ikkees: 21, baais: 22, teis: 23, chaubees: 24, pacchees: 25, chabbees: 26, sattais: 27, atthais: 28, untees: 29, tees: 30, ikattees: 31 };
              const dayVal = urduToNum[up] || 20;
              const proposed = new Date(new Date().getFullYear(), new Date().getMonth(), dayVal);
              if (proposed <= new Date()) proposed.setMonth(proposed.getMonth() + 1);
              if (proposed > new Date(Date.now() + 5 * 86400000)) { isBeyondPtpRequest = true; break; }
            }
          }
        }
        
        if (isBeyondPtpRequest && callAdvancedPastGreeting) {
          ptpNegotiationActive = true;
          ptpNegotiationAttempts++;
          // Clear any pending hangup timers ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â we're in negotiation mode
          clearGracefulHangupTimer();
          closingRequested = false;
          
          log.info('MEDIA', `[NEGOTIATION] Beyond-PTP date detected (attempt ${ptpNegotiationAttempts}/${MAX_PTP_NEGOTIATION_ATTEMPTS}): "${text}"`);
          broadcast({ type: 'LOG', message: `ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬â„¢ Negotiation attempt ${ptpNegotiationAttempts}/${MAX_PTP_NEGOTIATION_ATTEMPTS}: "${text}"`, source: 'sys' });
          
          if (ptpNegotiationAttempts >= MAX_PTP_NEGOTIATION_ATTEMPTS) {
            // Exhausted all attempts ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â let the agent end with a strong final close
            ptpNegotiationActive = false;
            log.info('TIMING', `[NEGOTIATION] ${MAX_PTP_NEGOTIATION_ATTEMPTS} attempts exhausted ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â allowing call to close`);
            broadcast({ type: 'LOG', message: `ÃƒÂ¢Ã‚ÂÃ‚Â±ÃƒÂ¯Ã‚Â¸Ã‚Â Negotiation exhausted ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â closing professionally`, source: 'sys' });
            
            const toneLabel = conv?.tone || 'polite';
            const agentVerb = callerGender === 'female' ? 'rahi' : 'raha';
            const finalClose = toneLabel === 'assertive'
              ? `Be VERY firm: "${conv?.customer_name || 'Sahab'}, aap ka account ${conv?.dpd || 0} din overdue hai. Bank policy ke mutabiq ab hum is call ko yahan band kar dete hain. Aap se phir baad mein contact kiya jayega. Shukriya. Allah Hafiz. [END_CALL]"`
              : `Close professionally: "${conv?.customer_name || 'Sahab'}, main ne bohot koshish ki hai. Ab main is call ko yahan band kar ${agentVerb} hoon. Shukriya. Allah Hafiz. [END_CALL]"`;
            if (wsGemini && wsGemini.readyState === WebSocket.OPEN && geminiSetupComplete) {
              wsGemini.send(JSON.stringify({
                client_content: {
                  turns: [{ role: 'user', parts: [{ text: `You have tried ${MAX_PTP_NEGOTIATION_ATTEMPTS} times to negotiate a payment date within 5 days but the customer refuses. This is the FINAL attempt. ${finalClose}` }] }],
                  turn_complete: true,
                },
              }));
            }
            scheduleGracefulHangup('negotiation_exhausted', 5500);
          } else {
            // Send negotiation strategy based on attempt number
            if (wsGemini && wsGemini.readyState === WebSocket.OPEN && geminiSetupComplete) {
              const maxDateLocal = dateToNaturalUrdu(new Date(Date.now() + (callAssets?.maxPtpDays || 5) * 86400000));
              const strategies = [
                `Customer wants more than 5 days. Use INCENTIVE approach: "${conv?.customer_name || 'Sahab'}, agar ${maxDateLocal} tak kar dein to bilkul koi extra charges nahi lagenge. Us ke baad teen percent penalty shuru ho jayegi. Aapke liye yahi sabse acha hai."`,
                `Customer STILL wants beyond 5 days. Use POLICY approach: "Bank policy ke mutabiq maximum 5 din ka time de sakte hain. Us se zyada possible nahi hai. Kal ya parson ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â kab convenient hoga?"`,
                `Customer insists on more time. Use URGENCY: "Account already ${conv?.dpd || 0} din overdue hai. Mazeed delay se CIBIL report mein naam aa jayega aur future loans block ho jayenge. ${maxDateLocal} tak karna zaroori hai."`,
                `Use EITHER-OR CHOICE: "Dekhiye, do option hain: kal ya parson. Dono mein se jo convenient ho, bata dein. Main date note kar loon."`,
                `Use ASSUMPTIVE CLOSE: "Main parson ki date note kar raha/rahi hoon ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â theek hai? Bas confirm kar dein."`,
                `Use BENEFIT LOCK: "Agar parson tak ho jaye to main aapka case positive close kar dunga/dungi. Phir koi aur follow-up nahi hoga. Ye aapke liye ek chance hai."`,
                `Use SOCIAL PROOF: "Zyada tar customers 2-3 din mein manage kar lete hain. Aap bhi parson tak kar sakte hain? Main madad karna chahta/chahti hoon."`,
                `Use EMPATHY + FIRMNESS: "Main samajh sakta/sakti hoon mushkil hai. Lekin agar aap kuch amount bhi kal bhej dein to case mein improvement show hogi. Kya partial payment ho sakti hai?"`,
                `Use DIRECT FINAL ASK: "Last time pooch raha/rahi hoon ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â ${maxDateLocal} tak payment ho jayegi? Sirf haan ya nahi batayein."`,
                `FINAL ATTEMPT: "Dekhiye ${conv?.customer_name || 'Sahab'}, main ne bohot koshish ki. Aapke liye yahi best hai ke ${maxDateLocal} tak payment karein. Main ye date system mein daal raha/rahi hoon. Confirm?"`,
              ];
              const strategyIndex = Math.min(ptpNegotiationAttempts - 1, strategies.length - 1);
              wsGemini.send(JSON.stringify({
                client_content: {
                  turns: [{ role: 'user', parts: [{ text: `${strategies[strategyIndex]} Do NOT end the call. Do NOT say Allah Hafiz. Do NOT say goodbye. Do NOT use [END_CALL]. Keep negotiating for a date within 5 days.` }] }],
                  turn_complete: true,
                },
              }));
            }
          }
        }
        // Track off-topic redirects
        if (!offTopicRedirects) offTopicRedirects = 0;
        
        const offTopicSignals = [
          'tumhara naam', 'aapka naam kya', 'kahan se bol', 'kaun bol', 'kya tum robot', 'ai ho', 'machine ho',
          'mausam', 'weather', 'cricket', 'match', 'politics', 'election', 'movie', 'film', 'song', 'gaana',
          'shaadi', 'biryani', 'khana', 'chai', 'dosti', 'pyar', 'love', 'friend', 'girlfriend', 'boyfriend',
          'pagal', 'mazak', 'joke', 'haha', 'lol', 'bakwas', 'faltu', 'faaltu', 'bewakoof', 'ullu',
          'court', 'lawyer', 'vakeel', 'case karo', 'complaint', 'shikayat', 'consumer forum',
        ];
        
        const isOffTopic = offTopicSignals.some(s => lower.includes(s));
        // Also detect if customer hasn't mentioned anything payment-related in 3+ consecutive turns
        const paymentKeywords = ['payment', 'paisa', 'paise', 'rupee', 'pay', 'haan', 'kal', 'parson', 'kar dunga', 'kar dungi', 'de dunga', 'bhej', 'account', 'bank', 'installment'];
        const isPaymentRelated = paymentKeywords.some(k => lower.includes(k));
        
        if (isOffTopic && !isPaymentRelated) {
          offTopicRedirects++;
          log.info('MEDIA', `[OFF-TOPIC] Redirect #${offTopicRedirects}: "${text}"`);
          broadcast({ type: 'LOG', message: `ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Å¾ Off-topic redirect #${offTopicRedirects}: "${text}"`, source: 'sys' });
          
          if (offTopicRedirects >= 3) {
            log.info('TIMING', `[OFF-TOPIC] 3 redirects exhausted ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â concluding call`);
            broadcast({ type: 'LOG', message: 'ÃƒÂ¢Ã‚ÂÃ‚Â±ÃƒÂ¯Ã‚Â¸Ã‚Â Off-topic limit reached ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â ending call', source: 'sys' });
            
            if (wsGemini && wsGemini.readyState === WebSocket.OPEN && geminiSetupComplete) {
              wsGemini.send(JSON.stringify({
                client_content: {
                  turns: [{ role: 'user', parts: [{ text: `Customer has been off-topic for ${offTopicRedirects} turns. They are not discussing payment at all. End the call professionally. Say: "${conv?.customer_name || 'Sahab'}, lagta hai abhi munasib waqt nahi hai. Main baad mein dobara call karungi. Shukriya. Allah Hafiz. [END_CALL]"` }] }],
                  turn_complete: true,
                },
              }));
            }
            closingRequested = true;
            scheduleGracefulHangup('off_topic_timeout', 4800);
          }
        } else if (isPaymentRelated) {
          // Reset counter if customer returns to topic
          offTopicRedirects = 0;
        }
        
        // Normal greeting detection
        if (lower.includes('assalam') || lower.includes('kaise hain')) {
          newState = 'GREETING';
        }
        }
      }
    }

    // Agent-side state detection
    if (role === 'agent') {
      if (lower.includes('munasib waqt') || lower.includes('baat karna')) newState = 'CONSENT';
      else if (lower.includes('pending') || lower.includes('due date')) newState = 'DISCLOSURE';
      else if (lower.includes('payment') && (lower.includes('kar sakte') || lower.includes('tareeqa'))) newState = 'COLLECTION';
    }

    if (['CONSENT', 'DISCLOSURE', 'COLLECTION', 'NON_CUSTOMER'].includes(newState)) {
      callAdvancedPastGreeting = true;
    }

    // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ AGENT says closing phrase ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ force end call IMMEDIATELY ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
    if (role === 'agent' && text) {
      recentAgentClosingText = `${recentAgentClosingText} ${text}`.trim().slice(-220);
    }
    const agentClosingContext = role === 'agent'
      ? `${recentAgentClosingText} ${lastAgentText} ${text}`.trim().toLowerCase()
      : '';
    const normalizedAgentClosing = fuzzy(agentClosingContext);
    const closingContextHasEndDirective = hasEndCallDirective(agentClosingContext);
    const closingContextHasGoodbye = hasGoodbyeClosingPhrase(agentClosingContext);

    const hasClosingMarkers = role === 'agent' && (
      explicitClosingPhrases.some(phrase => normalizedAgentClosing.includes(fuzzy(phrase))) ||
      closingContextHasEndDirective ||
      closingContextHasGoodbye ||
      normalizedAgentClosing.includes('shukriya allah hafiz') ||
      normalizedAgentClosing.includes('shukria allah hafiz') ||
      normalizedAgentClosing.includes('end call')
    );

    const hasExplicitClosing = role === 'agent' && !conversationEnded && !forceEndingCall && !closingRequested && hasClosingMarkers;

    if (hasExplicitClosing) {
      finalClosingPlaybackLock = true;
      suppressCustomerInputDuringEnding = true;
      if (!closingSignalDetectedAt) closingSignalDetectedAt = Date.now();
      newState = 'CLOSING';
      closingRequested = true;
      recentAgentClosingText = '';
      const ensuredClosing = closingContextHasEndDirective && !closingContextHasGoodbye
        ? requestFinalClosingLine('detect_state_end_call')
        : false;
      log.info('TIMING', `[END_CALL] Agent closing detected ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â graceful hangup queued`);
      broadcast({ type: 'LOG', message: 'ÃƒÂ¢Ã‚ÂÃ‚Â±ÃƒÂ¯Ã‚Â¸Ã‚Â Ending call now...', source: 'sys' });

      const nonCustomerClosureContext =
        nonCustomerIntentLocked ||
        nonCustomerConfirmPending ||
        currentConversationState === 'NON_CUSTOMER' ||
        /\b(wrong\s+number|galat\s+number|ghalat\s+number|main\s+nahi\s+hoon|i\s+am\s+not|im\s+not|not\s+the\s+customer)\b/i.test(`${lastCustomerText} ${text}`);

      if (nonCustomerClosureContext) {
        scheduleGracefulHangup('non_customer', 8200);
      } else {
        scheduleGracefulHangup('agent_closing', ensuredClosing ? 7000 : 6200);
      }
    } else if (role === 'agent' && !conversationEnded && !forceEndingCall && closingRequested && hasClosingMarkers && !gracefulHangupTimer) {
      // Re-arm graceful hangup if we are in closing mode but timer is missing.
      const nonCustomerClosureContext =
        nonCustomerIntentLocked ||
        nonCustomerConfirmPending ||
        currentConversationState === 'NON_CUSTOMER';
      const rearmDelay = nonCustomerClosureContext ? 8200 : 6200;
      scheduleGracefulHangup(nonCustomerClosureContext ? 'non_customer' : 'agent_closing', rearmDelay);
      log.warn('TIMING', '[END_CALL] Re-armed graceful hangup from explicit closing marker');
    } else if (role === 'agent' && (conversationEnded || forceEndingCall || closingRequested)) {
      log.info('MEDIA', `[SKIP] Ignoring agent closing ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â call already ending`);
    }

    if (newState !== currentConversationState) {
      currentConversationState = newState;
      broadcast({ type: 'STATE_CHANGE', state: newState });
    }
  }

  // Load conversation from DB
  function loadConversation() {
    if (!activeConversationId) return;
    conv = db.prepare('SELECT * FROM call_conversations WHERE id = ?').get(activeConversationId);
    if (conv) {
      callAssets = CALL_PREPARATION_CACHE.get(activeConversationId) || callAssets || null;
      useTwilioRecording = callAssets?.fetchTwilioRecording !== false;
      log.info('MEDIA', `Loaded: ${conv.customer_name} (twilioRecording: ${useTwilioRecording}, warmGreeting: ${callAssets?.openingGreetingPayloads?.length || 0} chunks)`);
      broadcast({ type: 'LOG', message: `Customer: ${conv.customer_name}`, source: 'sys' });
    }
  }

  function playPreparedGreeting(options = {}) {
    // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ SMART GREETING: If customer already spoke (salam/hello), skip replay ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
    if (customerTurnsHeard > 0 && preparedGreetingPlayed) {
      log.info('MEDIA', `[SMART-GREET] Customer already spoke (turns=${customerTurnsHeard}) ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â skipping greeting replay`);
      return true;
    }
    if (preparedGreetingPlayed) return true;
    const voiceName = getSessionVoiceName(startEventMetadata || {}, callAssets || {});
    const greetingOptions = USE_NATIVE_AUDIO_FOR_OPENING_GREETING
      ? { ...options, allowToneFallback: true, forceConnectionTone: true }
      : options;
    const { payloadsToPlay, greetingSource } = resolveGreetingPayloads(callAssets, voiceName, greetingOptions);

    if (!streamSid || !payloadsToPlay.length || twilioWs.readyState !== WebSocket.OPEN) return false;

    preparedGreetingPlayed = true;
    greetingRequestSent = true;
    lastGreetingSource = greetingSource;
    postGreetingListenGate = greetingSource !== 'connection-tone';
    if (postGreetingListenGate) {
      log.info('TIMING', `[GREETING-GATE] Enabled: waiting for first customer response before any further agent speech`);
    }

    // Ã¢â€â‚¬Ã¢â€â‚¬ GREETING LOCK: Prevent customer interruption during minimum playback window Ã¢â€â‚¬Ã¢â€â‚¬
    greetingPlaybackLock = true;
    greetingPlaybackStartedAt = Date.now();
    log.info('GREETING-LOCK', `[T+${Date.now()-T0}ms] Greeting lock ENGAGED`);

    sendTwilioPayloadSequence(payloadsToPlay, {
      intervalMs: GREETING_CHUNK_INTERVAL_MS,
      initialDelayMs: GREETING_START_DELAY_MS,
      stopIf: () => customerTurnsHeard > 0 || conversationEnded || forceEndingCall || closingRequested,
    });

    if (greetingSource !== 'connection-tone') {
      const cleanGreeting = pushMessage('agent', callAssets?.openingGreeting || '');
      if (cleanGreeting) {
        lastAgentText = cleanGreeting;
        detectState(cleanGreeting, 'agent');
      }
    }

    log.info('TIMING', `[T+${Date.now()-T0}ms] Greeting played [${greetingSource}] (${payloadsToPlay.length} chunks)`);
    broadcast({ type: 'LOG', message: `Greeting played [${greetingSource}]`, source: 'gem' });
    // Silence nudges are armed only after first verified customer speech.
    resetSilenceTimer();
    // Opening greeting is one-shot only. Start connectivity timer after playback window.
    const greetingPlaybackMs = Math.max(1200, payloadsToPlay.length * GREETING_CHUNK_INTERVAL_MS + GREETING_START_DELAY_MS);
    agentPlaybackActiveUntil = Math.max(agentPlaybackActiveUntil, Date.now() + greetingPlaybackMs + 300);
    
    // Ã¢â€â‚¬Ã¢â€â‚¬ GREETING LOCK RELEASE: Unlock after minimum playback window OR when customer speaks early Ã¢â€â‚¬Ã¢â€â‚¬
    const lockReleaseDelay = Math.max(greetingPlaybackMs, greetingMinPlaybackMs);
    setTimeout(() => {
      greetingPlaybackLock = false;
      log.info('GREETING-LOCK', `[T+${Date.now()-T0}ms] Greeting lock RELEASED (after ${lockReleaseDelay}ms)`);
      
      if (conversationEnded || forceEndingCall || closingRequested || customerTurnsHeard > 0) return;
      startInitialCustomerResponseTimer();
    }, lockReleaseDelay + 300);
    return true;
  }
  // loadConversation already called above for early Gemini connection
  if (!conv) loadConversation();

  // ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â GEMINI WEBSOCKET HANDLERS ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â extracted for early connection ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
  // Smart greeting instruction ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â adapts based on whether customer already spoke
  function buildGeminiGreetingInstruction(greetingText, greetingWasFallback) {
    const custText = lastCustomerText || customerTranscriptBuffer.trim() || '';
    const customerAlreadySpoke = customerTurnsHeard > 0 && custText.length > 0;
    const customerGreeted = customerAlreadySpoke && isShortPickupReply(custText);

    if (customerGreeted) {
      // Customer already said salam/hello ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â DO NOT repeat greeting, acknowledge and continue
      return `The customer has ALREADY greeted you by saying "${custText}". Your pre-recorded greeting "${greetingText}" was already played. The customer's speech has priority over any scripted opening. Do NOT repeat greeting. Reply with EXACTLY ONE sentence in Roman Urdu: "Walaikum Assalam ${conv?.customer_name || 'Sahab'}, kya ye munasib waqt hai aapke credit card dues par baat karne ke liye?" Do NOT say "Ji theek hai". No filler words. No second sentence.`;
    }
    if (customerAlreadySpoke) {
      // Customer spoke something (not a greeting) ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â skip greeting, respond contextually
      return `Customer has already spoken: "${custText}". Your pre-recorded greeting was already played. The customer's words have priority. Do NOT repeat the greeting. Respond to what the customer said naturally and continue the conversation from the appropriate step. If they seem confused, briefly re-introduce yourself in ONE sentence and move to consent. Do NOT say the full greeting again.`;
    }
    if (greetingWasFallback) {
      // No pre-recorded greeting played ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â Gemini must greet
      return `You are on a live phone call. The customer just picked up. Greet them once with EXACTLY this line: "${greetingText}" and then STOP. If the customer starts speaking before or during that greeting, stop greeting immediately, listen first, and answer their latest words instead of finishing the scripted greeting. Ignore background noise. Do NOT greet twice. Do NOT go to consent until after the customer response.`;
    }
    // Pre-recorded greeting was played ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â tell Gemini not to repeat
    return `The opening greeting "${greetingText}" has ALREADY been spoken to the customer via pre-recorded audio. DO NOT repeat it. Wait silently for the customer to respond. If any buffered or overlapping customer speech arrives, treat that speech as priority and respond to it first. When they respond, continue from TURN 2 (consent step). Do NOT say the greeting again.`;
  }

  // Send Gemini instruction to trigger greeting or consent step, but only if we haven't already sent it for this call (avoid duplicates during pre
  function sendOpeningInstructionOnce(geminiWs, instructionText, trigger = 'unknown') {
    if (!geminiWs || geminiWs.readyState !== WebSocket.OPEN || !instructionText) return false;
    if (openingInstructionSent) {
      log.info('MEDIA', `[GREETING] Opening instruction already sent ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â skipping duplicate (${trigger})`);
      return false;
    }

    geminiWs.send(JSON.stringify({
      client_content: {
        turns: [{ role: 'user', parts: [{ text: instructionText }] }],
        turn_complete: true,
      },
    }));
    openingInstructionSent = true;
    return true;
  }

  function setupGeminiHandlers(geminiWs) {
    const isCurrentGeminiSocket = () => wsGemini === geminiWs;

    geminiWs.on('open', () => {
      if (!isCurrentGeminiSocket()) return;
      geminiWs._expectedClose = false;
      geminiSetupFailed = false;
      geminiFailureReason = '';
      log.info('TIMING', `[T+${Date.now()-T0}ms] Gemini WebSocket OPEN`);
      broadcast({ type: 'LOG', message: 'GEMINI: Connected', source: 'gem' });

      // ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â SKIP SETUP IF PRE-WARMED WS ALREADY SENT IT ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
      // During PHASE 1 (pre-dial), we send the setup config so Gemini processes it during ringing.
      // If setupComplete already arrived, we skip the entire setup build ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â saving 3-5 seconds.
      if (geminiWs._setupSent) {
        if (geminiWs._setupComplete) {
          geminiSetupComplete = true;
          log.info('TIMING', `[T+${Date.now()-T0}ms] ÃƒÂ¢Ã…Â¡Ã‚Â¡ INSTANT READY ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â setup was completed during RINGING (0ms post-pickup delay!)`);
          broadcast({ type: 'LOG', message: 'GEMINI: Pre-warmed + pre-setup ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â INSTANT READY', source: 'gem' });
          broadcast({ type: 'STATUS_UPDATE', status: 'ACTIVE' });
          if (activeConversationId) {
            db.prepare("UPDATE call_conversations SET status='active', updated_at=datetime('now') WHERE id=?").run(activeConversationId);
          }
          // Trigger greeting + flush just like setupComplete handler would
          const usedPreparedGreeting = preparedGreetingPlayed || playPreparedGreeting();
          if (usedPreparedGreeting) {
            const greetingWasFallback = lastGreetingSource === 'connection-tone' || lastGreetingSource === 'none';
            const metadata = startEventMetadata || {};
            const exactOpeningGreetingForSetup = sanitizePreparedText(callAssets?.openingGreeting) || buildExactGreeting({
              customer_name: metadata.customerName || conv?.customer_name || 'customer',
              caller_name: metadata.agentName || conv?.caller_name || 'Omar',
              customer_gender: normalizeGender(metadata.customerGender || callAssets?.customer_gender || conv?.customer_gender || 'male', 'male'),
              caller_gender: resolveCallerGender(
                metadata.callerGender || callAssets?.caller_gender || conv?.caller_gender || conv?.callerGender,
                metadata.agentName || metadata.callerName || callAssets?.caller_name || conv?.caller_name || conv?.callerName || 'Omar',
                'male'
              ),
            });
            if (greetingWasFallback && geminiWs.readyState === WebSocket.OPEN) {
              // Send smart greeting instruction BEFORE flushing audio
              const smartInstruction = buildGeminiGreetingInstruction(exactOpeningGreetingForSetup, true);
              sendOpeningInstructionOnce(geminiWs, smartInstruction, 'prewarm_instant_fallback');
              setTimeout(() => {
                flushBufferedInboundAudio('prewarm_setup_complete_after_greeting');
              }, 300);
            } else if (geminiWs.readyState === WebSocket.OPEN) {
              const smartInstruction = buildGeminiGreetingInstruction(exactOpeningGreetingForSetup, false);
              sendOpeningInstructionOnce(geminiWs, smartInstruction, 'prewarm_instant_prepared');
              setTimeout(() => {
                flushBufferedInboundAudio('prewarm_setup_complete');
              }, 120);
            } else {
              flushBufferedInboundAudio('prewarm_setup_complete');
            }
            log.info('MEDIA', 'ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ Gemini INSTANT READY ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â prepared greeting played, processing customer reply');
          }
          return;
        } else {
          log.info('TIMING', `[T+${Date.now()-T0}ms] ÃƒÂ¢Ã¢â€žÂ¢Ã‚Â»ÃƒÂ¯Ã‚Â¸Ã‚Â Setup config was pre-sent ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â waiting for setupComplete...`);
          broadcast({ type: 'LOG', message: 'GEMINI: Setup pre-sent ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â awaiting ready', source: 'gem' });
          setTimeout(() => {
            if (!isCurrentGeminiSocket()) return;
            if (geminiSetupComplete || conversationEnded || forceEndingCall || closingRequested) return;
            log.warn('TIMING', `[T+${Date.now()-T0}ms] Pre-warmed Gemini setup still pending after pickup grace (${PREWARM_SETUP_GRACE_MS}ms) ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â forcing fresh reconnect`);
            startEarlyGeminiConnection({ forceFresh: true, reason: 'prewarmed_setup_stalled' });
          }, PREWARM_SETUP_GRACE_MS);
          return; // Don't re-send setup ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â just wait for setupComplete message
        }
      }

      // Build system instruction from conversation data + metadata
      const metadata = startEventMetadata || {};
      const assetMeta = callAssets || {};
      const agentName = metadata.agentName || metadata.callerName || assetMeta.caller_name || conv?.caller_name || 'Omar';
      const custName = metadata.customerName || assetMeta.customer_name || conv?.customer_name || 'customer';
      const amount = metadata.amount || String(assetMeta.balance ?? conv?.balance ?? 0);
      const dueDate = metadata.dueDate || new Date(Date.now() + 5 * 86400000).toISOString().split('T')[0];
      const custGender = normalizeGender(metadata.customerGender || assetMeta.customer_gender || conv?.customer_gender || 'male', 'male');
      const callerGender = resolveCallerGender(
        metadata.callerGender || assetMeta.caller_gender || conv?.caller_gender || conv?.callerGender,
        agentName,
        'male'
      );
      const voiceName = getSessionVoiceName(metadata, assetMeta);
      const dpdValue = Number(assetMeta.dpd ?? conv?.dpd ?? 0);
      const followUpValue = Number(assetMeta.follow_up_count ?? conv?.follow_up_count ?? 0);
      const ptpStatusValue = assetMeta.ptp_status || conv?.ptp_status || 'None';
      const agentTypeValue = conv?.agent_type || assetMeta.agent_type || 'fresh_call';
      const honor = custGender === 'female' ? 'Sahiba' : 'Sahab';
      const sirMadam = custGender === 'female' ? 'Madam' : 'Sir';
      const bolVerb = callerGender === 'female' ? 'bol rahi' : 'bol raha';
      const chahVerb = callerGender === 'female' ? 'chahti' : 'chahta';
      const samajhVerb = callerGender === 'female' ? 'samajh gayi' : 'samajh gaya';
      const karunVerb = callerGender === 'female' ? 'karungi' : 'karunga';
      const kartiVerb = callerGender === 'female' ? 'karti' : 'karta';
      const saktiVerb = callerGender === 'female' ? 'sakti' : 'sakta';
      const custSakteVerb = custGender === 'female' ? 'sakti' : 'sakte';
      const custKarVerb = custGender === 'female' ? 'kar sakti' : 'kar sakte';
      const exactOpeningGreeting = sanitizePreparedText(callAssets?.openingGreeting) || buildExactGreeting({
        customer_name: custName,
        caller_name: agentName,
        customer_gender: custGender,
        caller_gender: callerGender,
      });
      const exactConsentPrompt = `${sirMadam}, main aapse aapke credit card dues ke baare mein baat karna ${chahVerb} hoon, kya ye munasib waqt hai?`;

      const toneKey = conv?.tone || metadata.tone || 'polite';
      let toneBlock = '';
      if (toneKey === 'assertive') {
        toneBlock = `
TONE: ASSERTIVE ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â BOLD, STRICT, POWERFUL. You are NOT a friendly reminder caller. You are a SERIOUS recovery agent.
- Your voice must sound FIRM, URGENT, and COMMANDING. No softness.
- Use STRONG words: "Aap ka account ab bohot critical stage par hai", "Mazeed delay BILKUL bardasht nahi hoga", "Legal notice ka process shuru ho chuka hai"
- Mention CONSEQUENCES clearly and repeatedly:
  * "Aap ka naam CIBIL credit bureau mein report ho jayega ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â future mein koi bhi loan ya card approve nahi hoga"
  * "Account freeze ka process already initiated ho chuka hai"
  * "Legal recovery proceedings start ho chuki hain ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â court notice aap ke ghar aa sakta hai"
  * "3% monthly penalty interest lagatar barh raha hai ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â har din aapka burden barh raha hai"
- INSIST on IMMEDIATE payment date ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â maximum ${new Date(Date.now() + 5 * 86400000).toISOString().split('T')[0]} tak
- If customer asks for extension: "Extension ka option ab available NAHI hai. Aap ka account already ${dpdValue} din overdue hai. Ab sirf immediate payment hi aap ko in consequences se bacha sakti hai."
- If customer says "baad mein": "Sahab/Sahiba, 'baad mein' ka option ab KHATAM ho chuka hai. Pehle bhi chances diye gaye ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â ab bank ko action lena zaroori hai."
- Do NOT be polite or soft. Be DIRECT, BOLD, and make the customer feel URGENCY.
- Short, punchy sentences. No long explanations. Hit hard with consequences.`;
      } else if (toneKey === 'empathetic') {
        toneBlock = `
TONE: EMPATHETIC ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â Understanding but guiding toward payment.
- Show genuine concern for customer's situation
- Use phrases like "Main samajh sakti/sakta hoon", "Mushkil waqt hai lekin..."
- Still guide firmly toward payment commitment`;
      } else {
        toneBlock = `
TONE: POLITE ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â Professional, warm, respectful.
- Courteous and friendly approach
- Use "please", "shukriya", "agar aap ki marzi ho"
- Gentle reminders about payment`;
      }

      const speechPacingBlock = `SPEECH PACING (CRITICAL ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â READ THIS FIRST):
- You are on a REAL PHONE CALL. Speak FAST and NATURAL ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â like a real Pakistani call center agent.
- Keep EVERY response SHORT: 1 sentence MAX per turn. Do NOT give speeches.
- Respond INSTANTLY after customer speaks ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â ZERO thinking delay. Start speaking within 200ms.
- Say "Theek hai" not "Yeh bilkul theek hai aur main samajh gayi hoon".
- NEVER give long paragraphs. One sentence, then STOP AND WAIT.
- FIRST-REPLY RULE: If customer says "hello", "ji", "haan", "theek" ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â acknowledge in 3 words max, then consent step.
- FILLER SOUNDS: Say "Jee..." or "Acha..." INSTANTLY if you need a moment.
- SPEED: Match a real call center ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â brisk, professional, no pauses longer than 0.5 seconds.`;

      const maxPtpDaysVal = callAssets?.maxPtpDays || 5;
      const maxPtpDateForPrompt = dateToNaturalUrdu(new Date(Date.now() + maxPtpDaysVal * 86400000));

      const systemText = `IDENTITY: Your name is ${agentName}, a virtual recovery agent for JS Bank Karachi. You are professional, human-like, and advisory.
STRICT SCOPE: You ONLY discuss pending credit card dues.

FINANCIAL CONTEXT:
- Base Dues: ${amount} PKR
- Days Past Due: ${dpdValue}
- Due Date: ${maxPtpDateForPrompt}
- Penalty Interest: 3% (Say "three percent" or "teen percent")
- Interest Rule: No interest if paid by ${maxPtpDateForPrompt}
- PTP RULE (CRITICAL): Customer ki payment date MAXIMUM ${maxPtpDaysVal} din tak hi ho sakti hai (${maxPtpDateForPrompt} tak). Agar customer zyada din maange to:
  * PEHLE incentive do: "Agar ${maxPtpDateForPrompt} tak kar dein to koi charges nahi lagenge."
  * PHIR policy batao: "Bank policy ke mutabiq maximum ${maxPtpDaysVal} din ka time de sakte hain, us se zyada possible nahi."
  * LAST mein urgency: "Account already ${dpdValue} din overdue hai, mazeed delay se credit report affect hoga."
  * NEVER accept any date beyond ${maxPtpDaysVal} days. Always redirect to within 5 days.
- PTP Status: ${ptpStatusValue}
- Follow-ups: ${followUpValue}

LANGUAGE: EXCLUSIVELY PAKISTANI ROMAN URDU with specific English words.

STRICT LINGUISTIC RULES:
1. ROMAN-ONLY OUTPUT (HIGHEST PRIORITY): NEVER output Devanagari, Arabic Urdu, Thai, or any non-Latin script anywhere in speech, transcription, notes, summaries, or replies. If ASR produces non-Roman script like "ÃƒÂ Ã‚Â¸Ã‚Â®ÃƒÂ Ã‚Â¸Ã‚Â±ÃƒÂ Ã‚Â¸Ã‚Â¥ÃƒÂ Ã‚Â¹Ã¢â‚¬Å¡ÃƒÂ Ã‚Â¸Ã‚Â«ÃƒÂ Ã‚Â¸Ã‚Â¥", "Ãƒâ€ºÃ‚ÂÃƒâ€ºÃ…â€™Ãƒâ„¢Ã¢â‚¬Å¾Ãƒâ„¢Ã‹â€ ", or "ÃƒÂ Ã‚Â¤Ã‚Â¹ÃƒÂ Ã‚Â¥Ã‹â€ ÃƒÂ Ã‚Â¤Ã‚Â²ÃƒÂ Ã‚Â¥Ã¢â‚¬Â¹", immediately normalize it to Roman Urdu/ASCII like "hello" before continuing.
2. DATES: Always use full month names in natural Urdu. Say dates like "Pandra January" or "Bees February". Never use numeric format.
   ROMAN URDU DATE TERMS (CRITICAL ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â memorize these):
   - "kal" = tomorrow (1 day from today)
   - "parson" / "parso" = day after tomorrow (2 days from today)  
   - "tarsofn" / "tarson" / "narson" = 3 days from today
   - "Monday ko" / "Somwar ko" = next Monday ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â compute the actual date
   - "agle hafte" = next week ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â compute 7 days from today
   - "is hafte" = this week ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â compute remaining days this week
   - "mahine ki pehli" = 1st of next month
   - TODAY'S DATE: ${new Date().toISOString().split('T')[0]}
   - When customer says ANY relative term above, IMMEDIATELY convert to a SPECIFIC date and confirm: "Matlab [computed date with full month name], theek hai? Main ye note kar ${callerGender === 'female' ? 'leti' : 'leta'} hoon."
   - NEVER ask "kab?" or "konsi date?" ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â instead propose: "Main kal ki date note kar loon? Matlab [tomorrow's full date]"
3. NUMBERS & DECIMALS: Use "point" for decimals in English (e.g., "two point five percent"). For Urdu, say "teen percent" for 3%.
4. ROUNDING: Always round off amounts in speech. Say "Atharah hazaar" instead of exact decimals.
5. KEYWORDS: Inject these English words naturally: point, ${sirMadam.toLowerCase()}, interest, fees, bank, loan, cash, ensure, payment, credit card.
6. REPETITION: If the customer asks you to repeat or says "kia kaha?", respond with the full sentence in clear Urdu.
7. SCOPE LIMIT: If asked for a loan, account, or other services, respond: "Main sirf aapke pending dues ke baare mein assist kar ${saktiVerb} hoon. Baqi cheezon ke liye aap hamesha JS Bank customer support ko call ${custKarVerb} hain ya apni qareebi branch visit ${custKarVerb} hain. JS Bank hamesha aapki khidmat ke liye hazir hai."
8. ACCENT: Speak with natural Pakistani Karachi-style accent. Soft, friendly, conversational.
9. FLOW: Use natural fillers like "acha", "theek hai", "bas", "thora sa". Keep sentences short and flowing. Do NOT call the customer with standalone "Sahab/Sahiba" or standalone "Sir/Madam" in mid-conversation.
10. AVOID: Do not use "Jee" excessively. Use "Ji haan" or "Ji" sparingly.
11. GENDER: Customer is ${custGender}. Addressing style must be natural: use customer's first name (preferred) or full form "${custName} ${honor}" when needed, but never standalone "${honor}" / "${sirMadam}" in mid-conversation. When talking about what the CUSTOMER can do, use "${custSakteVerb}" (e.g., "aap ${custKarVerb} hain"). When talking about what YOU (agent) can do, use "${saktiVerb}" (e.g., "main kar ${saktiVerb} hoon"). You (agent) are ${callerGender} ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â use "${bolVerb}", "${chahVerb}", "${samajhVerb}", "${karunVerb}", "${kartiVerb}", "${saktiVerb}" for YOUR OWN actions consistently.
12. AGENT IDENTITY (CRITICAL ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â HIGHEST PRIORITY): You are a ${callerGender} agent named ${agentName}. ${callerGender === 'male' ? 'You are MALE. Your entire personality, voice, and language MUST be masculine. Use ONLY masculine Urdu verb forms: "main bol raha hoon", "main ne kaha", "main samajh gaya", "main karunga", "main karta hoon", "main leta hoon", "main chalta hoon". ABSOLUTELY NEVER use feminine forms like "rahi", "gayi", "karungi", "karti", "leti", "chalti". If you accidentally use a feminine form, you are BROKEN. Think of yourself as a confident Pakistani man from Karachi.' : 'You are FEMALE. Your entire personality, voice, and language MUST be feminine. Use ONLY feminine Urdu verb forms: "main bol rahi hoon", "main ne kaha", "main samajh gayi", "main karungi", "main karti hoon", "main leti hoon", "main chalti hoon". ABSOLUTELY NEVER use masculine forms like "raha", "gaya", "karunga", "karta", "leta", "chalta". Think of yourself as a professional Pakistani woman from Karachi.'}
12. HONORIFIC LIMIT (CRITICAL): Use "Sahab"/"Sahiba" MAXIMUM 2-3 times in the ENTIRE call ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â once in greeting, once in closing, maybe once mid-conversation. DO NOT say it every sentence ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â it sounds robotic and fake. Most of the time just use the customer's first name "${conv?.customer_name?.split(' ')[0] || 'customer'}" without any title.

${speechPacingBlock}

${toneBlock}

Agent Type: ${agentTypeValue}
${agentTypeValue === 'ptp_reminder' ? `
PTP REMINDER RULES (CRITICAL ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â YOU ARE CALLING TO REMIND ABOUT TOMORROW'S PAYMENT):
- This customer ALREADY gave a payment date. Your job is to REMIND them, not negotiate a new date.
- If customer asks for extension ("thora aur time", "do din aur", "agle hafte"): FIRMLY INSIST on the original date.
  * "Nahi ${honor}, aap ne pehle ${maxPtpDateForPrompt} tak ka wada kiya tha. Wo date already system mein hai. Kal payment zaroor karein."
  * "Extension dena mere ikhtiyar mein nahi hai ${honor}. Kal ki date final hai ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â please ensure kar lein."
  * If customer keeps asking: "Dekhiye, agar kal payment nahi hoti to account mein late charges aur CIBIL reporting shuru ho jayegi. Yeh aapke liye mushkil hoga."
- NEVER agree to change the PTP date. NEVER say "theek hai" to an extension request.
- If customer says they genuinely cannot pay tomorrow, note it as negotiation_barrier but do NOT give a new date.
` : agentTypeValue === 'ptp_followup' ? `
PTP FOLLOW-UP RULES (CRITICAL ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â PAYMENT WAS DUE TODAY):
- This customer promised to pay TODAY. Your job is to CONFIRM payment happened.
- Ask: "Assalam o Alaikum ${conv?.customer_name?.split(' ')[0] || ''} ${honor}. Aaj payment ka din tha ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â kya payment ho gayi?"
- If YES with details: Mark as payment_done. "Shukriya! System mein update ho jayega. Allah Hafiz."
- If NO / asks for extension: Be FIRM. Do NOT extend.
  * "Aaj ki date thi ${honor}. Ab mazeed delay se teen percent charges lag jayenge aur CIBIL mein report hoga."
  * "Extension nahi ho sakta ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â kal se penalties shuru ho jayengi. Abhi payment karein please."
- If customer gives excuse but sounds willing: "Theek hai, aaj shaam tak kar lein. Main raat 6 baje se pehle ek aur check karungi. Lekin aaj zaroori hai."
- NEVER agree to a new date beyond today. Maximum flexibility: "aaj shaam tak" (same day).
` : agentTypeValue === 'broken_promise' ? `
BROKEN PROMISE RULES (CRITICAL ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â CUSTOMER MISSED THEIR PROMISED DATE):
- This customer BROKE their promise. They said they would pay but did NOT.
- Be DIRECT and SERIOUS (not rude): "Aap ne payment ka wada kiya tha lekin abhi tak nahi hui. Ab account serious stage mein hai."
- Mention consequences clearly: CIBIL reporting, late charges, account freeze.
- Get a NEW PTP date within 2 days maximum (not 5): "Ab maximum 2 din ka time hai. Kal ya parson ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â kab karenge?"
- If customer makes excuses: "Pichli dafa bhi yahi kaha tha. Ab action zaroor lena hoga ${honor}."
` : ''}

STRICT CONVERSATION FLOW:
- TURN 1: Say ONLY this short pickup line and STOP: "${exactOpeningGreeting}" STOP AND WAIT.
- TURN 2: If customer says hello/assalam o alaikum/ji/haan, give ONE short acknowledgement only, then say EXACTLY: "${exactConsentPrompt}" STOP AND WAIT.
- DO NOT REPEAT CONSENT: Once you asked "kya ye munasib waqt hai?" ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â NEVER ask it again. If customer says "yes/haan/ji/theek", move DIRECTLY to dues. Do NOT re-ask.
- FIRST-REPLY MEANING: Replies like "hello", "ji", "haan", "theek", "allah ka shukar", "boliye", or "kon?" are pickup acknowledgements. Treat them as the customer's first reply to your opening and move forward naturally.
- WELL-BEING RULE: NEVER say "I am fine" or "main theek hoon" unless the customer explicitly asks "aap kaise hain?" after addressing you.
- TURN 3+: First answer the customer's latest point/question. Only after that continue to dues ${amount} PKR and the ${maxPtpDateForPrompt} deadline.
- DATE FORMAT (CRITICAL): NEVER say dates in numeric format like "2026-04-14". Use CARDINAL numbers like a native speaker: "Chaudah April", "Pandrah January", "Bees March". For 1st-3rd: "Pehli", "Doosri", "Teesri". NEVER use ordinal "vi" suffix (NEVER say "Chaudahvi" or "Pandrahvi").
- LISTENING (CRITICAL): When customer speaks ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â STOP talking immediately. Listen to EVERYTHING they say. Their words have priority over your script.
- If customer says "kr doonga" / "kar dunga" / "haan theek hai" ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â that means they AGREE to pay. Acknowledge it, confirm the date, and close.
- NO THINKING OUT LOUD: NEVER narrate your internal reasoning. No "I'm preparing...", "My focus is...", "I've noted...". Just speak naturally.

NETWORK HANDLING:
- Silence after greeting: Wait about 1 second, then "Hello? ${custName} ${honor}? Sun sakte hain?"
- Unclear audio: "Sorry, awaz clear nahi aa rahi. Dobara bol sakte hain?"
- Customer says "Hello? Hello?": "Ji, main yahan hoon. ${agentName} JS Bank se." Then immediately continue with: "${exactConsentPrompt}"

HANGUP SAFETY:
- Short pickup ("hello", "ji", "kon?"): DO NOT end call. Re-introduce yourself.
- Never say "Allah Hafiz" before completing consent/reminder step.

SCENARIO RESPONSES:

1. CUSTOMER COOPERATIVE ("Haan bolien"):
"Shukriya. Aapke credit card par ${amount} rupees pending hain. Payment ${maxPtpDateForPrompt} tak due hai ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â us se pehle koi extra charges nahi. Baad mein teen percent lag sakte hain. Kab tak ${custKarVerb} hain?"

2. HOW TO PAY ("Kaise pay karun?"):
"Theek hai. Main short steps bata ${callerGender === 'female' ? 'deti' : 'deta'} hun. Sb se pehle, JS Bank mobile app ya online banking open karein. Phir credit card section mein jaa kar, pending amount check karein. Amount enter karke confirm karein. Payment complete ho jayegi. Agar kisi step par issue aaye, JS Bank helpline guide kar degi."

3. INSTALLMENTS ("Installments ka option hai?"):
"Ji haan. Installments ka option aapki card eligibility par depend karta hai. Is ke liye, JS Bank helpline ya branch se confirmation hoti hai. Agar aap chahen, main note kar ${saktiVerb} hun ke aap installments mein interested hain."

4. CUSTOMER BUSY ("Abhi busy hoon"):
"Theek hai. Bas short reminder dena tha, ke payment ${maxPtpDateForPrompt} tak due hai. Is ke baad, teen percent late charges lag sakte hain. Aap jab free hon, payment easily app ya online banking se ho jati hai. Shukriya ${honor}. Allah Hafiz."

5. REFUSES HELP ("Mujhe help nahi chahiye"):
"Theek haiÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ ${samajhVerb}. Bas itna yaad rahe, payment ${maxPtpDateForPrompt} tak due hai. Us ke baad late charges add ho sakte hain. Shukriya aapke time ka. Allah Hafiz."

6. CUSTOMER LOOPS ("Ji bolien" repeats):
"Ji. Payment ${maxPtpDateForPrompt} tak due hai. Is ke baad teen percent charges lag sakte hain. Aap payment khud manage kar lenge, ya payment ka tareeqa bata doon?" If repeats again: "Theek hai. Allah Hafiz."

7. PAYMENT DONE ("Payment ho chuki hai"):
- VERIFY SMARTLY: "Acha, shukriya batane ka. Zara confirm kar loon ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â kab payment ki thi? Aur konsi method se ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â app, online banking, ya branch visit?"
- If customer gives vague answer: "Theek hai, kabhi kabhi system mein update thora late hota hai. Main apne end se check karun gi/ga. Agar reflect nahi hua to hum aapko inform karenge."
- If customer sounds confident with details: "Perfect, shukriya. Agar sab kuch reflect ho gaya to koi further action nahi hogi. Allah Hafiz."
- IMPORTANT: If customer claims payment but sounds evasive or can't give details ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â note as 'partial_payment' or 'callback_requested', NOT 'payment_done'

8. NON-CUSTOMER PICKUP (Someone else picks up):
- DO NOT discuss any financial details or balance information with them
- Simply say: "Theek hai, koi baat nahi. Kya aap ${custName} ${honor} ko message de sakte hain ke JS Bank se call aayi thi? Hum dobara try karenge. Shukriya. Allah Hafiz."
- If they ask what it's about: "Ye ek routine call thi, hum dobara contact karenge. Shukriya. Allah Hafiz."

9. CUSTOMER CLAIMS ALREADY PAID BUT BLUFFING:
- If customer says "main ne payment kar di" but can't provide when/how: "Hmm, abhi system mein reflect nahi ho raha. Agar payment hui hai to 24-48 ghante mein update ho jayega. Lekin agar koi issue hai to ${maxPtpDateForPrompt} tak zaroor ensure kar lein ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â us ke baad charges lag jayenge."
- Use professional concern, not accusation: "Main aapki taraf se follow-up kar ${callerGender === 'female' ? 'leti' : 'leta'} hoon ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â bass ek backup date de dein agar kisi wajah se reflect nahi hua?"

CLOSING (ONE TIME ONLY ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â CRITICAL): Say EXACTLY: "Shukriya. Allah Hafiz. [END_CALL]" ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â then STOP PERMANENTLY.
- NEVER say "Allah Hafiz" more than ONCE in the entire call.
- After saying "Allah Hafiz", do NOT respond to anything the customer says. Do NOT listen. The call is OVER.
- Including [END_CALL] is MANDATORY whenever you say "Allah Hafiz".
- If you have already said "Allah Hafiz" once, you must not say it again under any circumstance.

CRITICAL RULES:
- Always start with opening greeting and WAIT for response
- Match customer's tone and pace
- Keep responses short, natural, conversational (1 sentence MAX per turn ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â NEVER give long paragraphs)
- Use natural Pakistani Urdu pronunciation
- End call cleanly when appropriate ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â include [END_CALL] in response when conversation should end
- Never repeat full introduction if customer loops
- Sound like a real Pakistani call center agent, not a robot
- Speak with warmth and professionalism
- If non-customer picks up: NEVER share financial details, just leave a callback message and end
- Use correct gender verb forms consistently ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â NEVER mix male/female verb forms
- SEMANTIC GROUNDING: Every reply must directly answer the customer's last utterance; never answer your own previous question incorrectly
- NEVER abruptly end call ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â always say proper goodbye with "Shukriya" and "Allah Hafiz"
- If the opening greeting was already played before you became ready, DO NOT repeat it; continue from consent after the customer responds

NOISE HANDLING (CRITICAL):
- The customer's phone may have background noise (traffic, TV, wind, crowd, music, children).
- IGNORE all background sounds. Focus ONLY on the customer's spoken words.
- If you hear unclear audio with noise, ask politely: "Sorry ${honor}, awaz thori clear nahi aa rahi. Kya aap dobara bol sakte hain?"
- Do NOT respond to background conversations, TV dialogue, or random sounds as if the customer spoke.
- If audio is consistently unclear for 3+ turns, suggest: "${honor}, lagta hai line clear nahi hai. Main thori der baad dobara call ${callerGender === 'female' ? 'karti' : 'karta'} hoon."
- Never get confused or distracted by ambient sounds ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â stay focused on the debt recovery conversation.

- When the customer starts speaking while you are talking, IMMEDIATELY STOP talking and LISTEN.
- Do NOT continue your sentence ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â YIELD the floor completely.
- After the customer finishes, respond to what THEY said, not what you were saying before.
- If the customer interrupts with "haan haan theek hai" or "ok ok" ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â they want you to move on. Skip ahead.
- If the customer interrupts with a question ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â STOP and answer their question first.
- NEVER talk over the customer. The customer ALWAYS has priority.

BLUFF & VAGUE COMMITMENT DETECTION (CRITICAL):
- If customer gives vague answers like "haan haan theek hai" without a SPECIFIC date ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â DO NOT accept it as PTP.
- Push professionally: "${custName} ${honor}, bass ek date confirm kar dein ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â kal ya parson? Phir hum aapko remind bhi kar denge."
- If customer says "baad mein kar dunga" without date ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â politely insist: "Bilkul, bass date bata dein toh hum apna system mein note kar lein."
- If customer keeps deflecting (3+ vague answers) ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â use professional humor: "${custName} ${honor}, aap mujhe mushkil mein daal rahe hain! Bass ek date, phir main aapko bilkul tang nahi karunga."
- NEVER accept "jaldi", "kuch din mein", "dekhte hain" as final PTP ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â always convert to a specific date by proposing one.

PTP NEGOTIATION STRATEGIES (use different approaches each attempt):
- ATTEMPT 1 ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â EITHER-OR: "Kal karenge ya parson? Dono mein se jo convenient ho."
- ATTEMPT 2 ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â ASSUMPTIVE: "Main parson ki date note kar ${callerGender === 'female' ? 'rahi' : 'raha'} hoon, theek hai?"
- ATTEMPT 3 ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â BENEFIT: "Agar kal tak ho jaye to main aapka case close kar ${callerGender === 'female' ? 'dungi' : 'dunga'}, phir koi follow-up nahi."
- ATTEMPT 4 ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â SOCIAL PROOF: "Zyada tar customers do din mein manage kar lete hain. Aap bhi parson tak ${custKarVerb} hain?"
- FINAL ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â DIRECT: "Last time pooch ${callerGender === 'female' ? 'rahi' : 'raha'} hoon ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â ${maxPtpDateForPrompt} tak ho jayegi? Sirf haan ya nahi."

5-DAY REDIRECT (CRITICAL):
- If customer says "das din", "next week", "agle mahine", "15 din" or ANY date > 5 days:
  * "${custName} ${honor}, itna time nahi de sakte ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â maximum ${maxPtpDaysVal} din hai policy ke mutabiq. ${maxPtpDateForPrompt} tak karna hoga, warna late charges shuru ho jayenge."
  * If still insists: "Dekhiye, yahi last date hai system mein ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â ${maxPtpDateForPrompt}. Main is se aage nahi ja ${saktiVerb}. Confirm karein?"
  * NEVER say "theek hai" to a date beyond 5 days. Always redirect back.

OFF-TOPIC DURING PTP NEGOTIATION:
- If customer changes topic WHILE you are trying to get date: Acknowledge briefly then redirect: "Ji woh to hai. Lekin pehle payment date ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â kal ya parson?"
- Do NOT get drawn into long off-topic conversations when date is still pending.
- After 3 off-topic attempts: "Bas date confirm kar dein phir main chali/chala. Kal theek hai?"

- Use the Mouth-Speak technique: PROPOSE a date rather than asking: "Kal tak ho jayegi? Main kal ka date note kar leta/leti hoon."
- If customer agrees to a date, CONFIRM it back: "Perfect, toh ${custName} ${honor}, main [date] ka note kar raha/rahi hoon. Shukriya!"

CALL ENDING (CRITICAL):
- When customer says "Allah Hafiz", "Khuda Hafiz", "bye", "ok bye", "theek hai shukriya" ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â the call is OVER.
- Respond with ONLY: "Shukriya. Allah Hafiz." and NOTHING else. Do NOT add extra sentences after goodbye.
- Do NOT try to continue the conversation after the customer says goodbye.
- Keep your goodbye to MAXIMUM 5 words. No speeches, no bank taglines, no extra pleasantries.
- [END_CALL] must be included when you say goodbye.`;

      // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Inject previous call history for follow-up/redial calls ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
      let callHistoryBlock = '';
      const prevHistory = callAssets?.previousCallHistory;
      const prevPtpDate = callAssets?.ptpDate;
      const prevRetryReason = callAssets?.retryReason;
      
      if (prevHistory && prevHistory.length > 0) {
        const historyLines = prevHistory.map((h, i) => {
          const dateStr = h.date ? new Date(h.date).toLocaleDateString('en-PK', { day: 'numeric', month: 'short', year: 'numeric' }) : 'Unknown';
          let line = `Call ${i + 1} (${dateStr}): Outcome = ${h.outcome || 'unknown'}, Duration = ${h.duration || 0}s`;
          if (h.ptp_date) line += `, PTP Date = ${new Date(h.ptp_date).toLocaleDateString('en-PK', { day: 'numeric', month: 'long' })}`;
          if (h.notes) line += `, Notes: ${h.notes}`;
          // Include last transcript summary (first 500 chars)
          if (h.transcript) {
            const shortTranscript = h.transcript.substring(0, 500);
            line += `\nPrevious conversation:\n${shortTranscript}${h.transcript.length > 500 ? '...' : ''}`;
          }
          return line;
        }).join('\n\n');

        callHistoryBlock = `\n\nÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â PREVIOUS CALL HISTORY (USE THIS TO YOUR ADVANTAGE) ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
This is a FOLLOW-UP call. You have spoken to this customer before.
Reason for this call: ${prevRetryReason || 'Scheduled retry'}
${prevPtpDate ? `Customer's existing PTP date: ${new Date(prevPtpDate).toLocaleDateString('en-PK', { day: 'numeric', month: 'long', year: 'numeric' })}` : ''}

PREVIOUS CALLS:
${historyLines}

FOLLOW-UP STRATEGY (CRITICAL):
- Reference the previous conversation naturally: "Pichli dafa aap ne [commitment] kaha tha..."
- If customer gave a PTP date before but didn't pay: "Aap ne [date] tak payment ka wada kiya tha, lekin abhi tak reflect nahi hua. Kya koi masla aaya?"
- If customer was rude/refused before: Use softer approach, acknowledge their frustration but stay firm on payment
- If customer was cooperative before: Build on rapport, reference positive interaction
- If customer bluffed before (gave fake commitment): Be direct but professional: "[Name] Sahab/Sahiba, pichli call mein aap ne [date] ka wada kiya tha. Ab hum phir se baat kar rahe hain ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â is baar confirm date dein toh hum system mein properly note kar lein."
- NEVER pretend this is a first call ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â customer knows you called before
- Use the previous tone/agent context to calibrate your approach
- Be more assertive on repeated follow-ups ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â customer has had multiple chances`;
      }

      const customPromptFromSettings = callAssets?.customSystemPrompt || '';
      let finalSystemText = systemText;
      if (callHistoryBlock) finalSystemText += callHistoryBlock;
      if (customPromptFromSettings) finalSystemText += `\n\nÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â ADDITIONAL RULES FROM SETTINGS ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â\n${customPromptFromSettings}`;

      const setupMsg = {
        setup: {
          model: `models/${GEMINI_NATIVE_AUDIO_MODEL}`,
          generation_config: {
            response_modalities: ['AUDIO'],
            speech_config: {
              voice_config: {
                prebuilt_voice_config: {
                  voice_name: voiceName
                }
              }
            }
          },
          // Disable safety filters to prevent false disconnections on debt collection conversations
          safety_settings: [
            { category: 'HARM_CATEGORY_HARASSMENT', threshold: 'OFF' },
            { category: 'HARM_CATEGORY_HATE_SPEECH', threshold: 'OFF' },
            { category: 'HARM_CATEGORY_SEXUALLY_EXPLICIT', threshold: 'OFF' },
            { category: 'HARM_CATEGORY_DANGEROUS_CONTENT', threshold: 'OFF' },
          ],
          system_instruction: {
            parts: [{
              text: finalSystemText
            }]
          },
          input_audio_transcription: {},
          output_audio_transcription: {},
          realtime_input_config: {
            activity_handling: 'START_OF_ACTIVITY_INTERRUPTS',
            automatic_activity_detection: {
              disabled: false,
              prefix_padding_ms: 250,       // Capture more speech onset (was 100 ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â missed first syllable)
              silence_duration_ms: 700,      // Wait longer before assuming turn is done (was 500 ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â cut off pauses)
            }
          }
        }
      };
      geminiWs.send(JSON.stringify(setupMsg));
      log.info('TIMING', `[T+${Date.now()-T0}ms] Setup msg sent, voice: ${voiceName}`);
      broadcast({ type: 'LOG', message: `Voice: ${voiceName}`, source: 'gem' });
    });

    // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Gemini message handler ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â handles both camelCase and snake_case ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
    geminiWs.on('message', (data) => {
      if (!isCurrentGeminiSocket()) return;
      try {
        const response = JSON.parse(data.toString());
        const sc = response.serverContent || response.server_content;

        const inputTx = sc?.inputTranscription || sc?.input_transcription;
        if (inputTx) {
          const customerText = inputTx.text;
          if (customerText) {
            const rawCustomerTranscript = String(customerText || '').replace(/\s+/g, ' ').trim();
            const normalizedForLog = normalizeTranscriptToRomanUrdu(customerText);
            
            // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Filter out "noise" / non-speech transcriptions ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
            // Gemini often transcribes background audio as literal "noise", "[noise]", 
            // empty strings, or single characters. These should NOT be treated as customer speech.
            const noisePatterns = /^(noise|\[noise\]|\(noise\)|background noise|static|silence|\[silence\]|\[inaudible\]|inaudible|\.{1,}|\s*)$/i;
            const isNoise = noisePatterns.test(normalizedForLog.trim());
            
            if (isNoise) {
              log.info('MEDIA', `ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Â¡ Filtered noise transcription: "${normalizedForLog}" ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â NOT treating as customer speech`);
              if (customerTurnsHeard === 0 && !conversationEnded && !forceEndingCall && !closingRequested) {
                const now = Date.now();
                if (now - lastNoiseActivityAt > 1200) {
                  lastNoiseActivityAt = now;
                  startInitialCustomerResponseTimer(INITIAL_CUSTOMER_RESPONSE_TIMEOUT_MS);
                }
              }
              if (customerTurnsHeard > 0) {
                // Only extend silence nudges once a live human turn is confirmed.
                resetSilenceTimer();
              }
              return;
            }

            // Log first customer voice with timing
            if (!customerFirstWordLogged) {
              customerFirstWordLogged = true;
              log.info('TIMING', `[T+${Date.now()-T0}ms] ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ FIRST customer voice received: "${normalizedForLog}"`);
            }
            log.info('MEDIA', `ÃƒÂ°Ã…Â¸Ã…Â½Ã‚Â¤ Customer: ${normalizedForLog}`);

            // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ If call is already ending, just log but don't process ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
            if (conversationEnded || forceEndingCall || closingRequested || finalClosingPlaybackLock || suppressCustomerInputDuringEnding) {
              log.info('MEDIA', `[SKIP] Ignoring customer speech ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â final closing playback active: "${normalizedForLog}"`);
              return;
            }

            // SMART BARGE-IN: Only clear agent audio when Gemini confirms real customer speech
            if (streamSid && twilioWs.readyState === WebSocket.OPEN) {
              try {
                twilioWs.send(JSON.stringify({ event: 'clear', streamSid }));
              } catch {}
            }

            // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ AGGREGATE customer transcript fragments before processing ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
            // Each word comes separately from Gemini. Processing individual words
            // through detectState() causes false keyword matches and fragmented transcripts.
            if (customerTurnsHeard === 0) {
              customerTurnsHeard = 1;
              postGreetingListenGate = false;
              connectivityFollowupCount = 0;
              cancelGreetingRetryTimer();
              clearInitialCustomerResponseTimer();
              clearGracefulHangupTimer(); // Cancel any greeting-timeout hangup
              log.info('MEDIA', `[GREETING] Customer responded ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â greeting loop cancelled, proceeding to live conversation`);
            }
            resetSilenceTimer();
            startAgentWatchdog();
            // Strip 'noise' from mixed transcripts like 'salaam noise'
            const cleanedForBuffer = normalizedForLog.replace(/\bnoise\b/gi, '').trim();
            const rawForTranscript = rawCustomerTranscript.replace(/\bnoise\b/gi, '').trim();
            if (cleanedForBuffer) {
              const immediateFuzzy = fuzzy(cleanedForBuffer);
              const immediateNonCustomerCue = /\b(wrong\s+number|galat\s+number|ghalat\s+number|my\s+name\s+is|mera\s+naam|i\s+am\s+not|im\s+not|main\s+nahi|mai\s+nahi|mein\s+nahi|main\s+nhi|mai\s+nhi|mein\s+nhi|main\s+koi\s+aur|baat\s+nahi\s+kar\s+raha|baat\s+nahi\s+kar\s+rahi)\b/i.test(immediateFuzzy);
              if (immediateNonCustomerCue && !conversationEnded && !forceEndingCall && !closingRequested) {
                detectState(cleanedForBuffer, 'customer');
              }
              if (closingRequested || nonCustomerIntentLocked || currentConversationState === 'NON_CUSTOMER') {
                log.info('MEDIA', `[SKIP] Suppressing buffered customer chunk after non-customer/closing lock: "${cleanedForBuffer}"`);
                return;
              }
              appendCustomerTranscript(cleanedForBuffer, rawForTranscript || cleanedForBuffer);
            }
          }
        }

        const modelTurn = sc?.modelTurn || sc?.model_turn;
        const outputTx = sc?.outputTranscription || sc?.output_transcription;
        const hasOutputTranscription = Boolean(outputTx?.text);
        const modelIsResponding = Boolean(modelTurn?.parts?.length || outputTx?.text);
        if (modelIsResponding && customerTranscriptBuffer.trim()) {
          if (customerTranscriptTimer) clearTimeout(customerTranscriptTimer);
          flushCustomerTranscript();
        }
        if (modelTurn?.parts) {
          const suppressEarlyAgentOutput = postGreetingListenGate && customerTurnsHeard === 0;
          for (const part of modelTurn.parts) {
            if (part.text) {
              if (suppressEarlyAgentOutput) {
                log.info('TIMING', `[GREETING-GATE] Suppressed pre-customer agent text chunk`);
                continue;
              }
              // Filter thinking text immediately (don't even buffer it)
              if (isThinkingText(part.text)) {
                log.info('MEDIA', `ÃƒÂ°Ã…Â¸Ã‚Â§Ã‚Â  Filtered thinking (modelTurn): "${part.text.substring(0, 80)}..."`);
              } else {
                log.info('MEDIA', `ÃƒÂ°Ã…Â¸Ã‚Â¤Ã¢â‚¬â€œ Agent text: ${part.text}`);
                // Fallback close trigger: sometimes output transcription is missing,
                // so detect closing markers from model text chunks directly.
                const chunkNorm = String(part.text || '');
                const chunkHasEndDirective = hasEndCallDirective(chunkNorm);
                const chunkHasGoodbye = hasGoodbyeClosingPhrase(chunkNorm);
                const chunkHasClosingSignal =
                  chunkHasEndDirective ||
                  chunkHasGoodbye;
                if (chunkHasClosingSignal && !conversationEnded && !forceEndingCall) {
                  finalClosingPlaybackLock = true;
                  suppressCustomerInputDuringEnding = true;
                  if (!closingSignalDetectedAt) closingSignalDetectedAt = Date.now();
                  detectState(part.text, 'agent');
                  // Immediately close Gemini after a closing phrase in model text
                  // to stop any in-flight second response from being delivered.
                  const ensuredClosing = chunkHasEndDirective && !chunkHasGoodbye
                    ? requestFinalClosingLine('model_turn_end_call')
                    : false;
                  if (!ensuredClosing) {
                    const geminiToCloseModel = wsGemini;
                    if (geminiToCloseModel && geminiToCloseModel.readyState === WebSocket.OPEN) {
                      setTimeout(() => closeGeminiSocket(geminiToCloseModel, 'model_text_closing_phrase'), 900);
                    }
                  }
                }
                // Fallback: when output transcription is missing for this server message,
                // use model text so final transcript doesn't lose agent turns.
                if (!hasOutputTranscription) {
                  appendAgentTranscript(part.text);
                }
              }
            }

            if (part.inlineData?.data) {
              if (suppressEarlyAgentOutput) {
                continue;
              }
              // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Stop sending agent audio to Twilio once call is ending ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
              // Keep final goodbye audio intact while graceful hangup timer runs.
              if (conversationEnded || forceEndingCall) {
                log.info('MEDIA', `[SKIP] Dropping agent audio ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â call ending`);
                continue;
              }
              if (closingRequested && closingSignalDetectedAt && Date.now() - closingSignalDetectedAt > 2200) {
                continue;
              }
              if (!streamSid || twilioWs.readyState !== WebSocket.OPEN) continue;
              const audioBase64 = part.inlineData.data;
              const mimeType = part.inlineData.mimeType || 'audio/pcm;rate=24000';
              const sampleRateMatch = mimeType.match(/rate=(\d+)/i);
              const sampleRate = sampleRateMatch ? Number(sampleRateMatch[1]) : 24000;

              try {
                const pcmBuffer = Buffer.from(audioBase64, 'base64');
                appendFileSync(debugFiles.geminiRaw, pcmBuffer);
                const muLawBuffer = encodePcmRateToTwilio(pcmBuffer, sampleRate);
                const payloads = chunkTwilioPayloads(muLawBuffer);
                const estimatedPlaybackMs = (payloads.length * 20) + 1400;
                agentPlaybackActiveUntil = Math.max(agentPlaybackActiveUntil, Date.now() + estimatedPlaybackMs);
                for (const payload of payloads) {
                  twilioWs.send(JSON.stringify({ event: 'media', streamSid, media: { payload } }));
                  if (!firstAudioSentToTwilio) {
                    firstAudioSentToTwilio = true;
                    log.info('TIMING', `[T+${Date.now()-T0}ms] FIRST Gemini audio chunk ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ Twilio`);
                  }
                  lastAgentAudioSentAt = Date.now();
                  agentPlaybackActiveUntil = Math.max(agentPlaybackActiveUntil, Date.now() + 1200);
                  try { outboundChunks.push(Buffer.from(payload, 'base64')); } catch {}
                  clearAgentWatchdog();
                  lastGeminiSendAt = Date.now();
                }
              } catch (err) {
                log.error('MEDIA', `Audio encode error: ${err.message}`);
              }
            }
          }
        }

        // Process output transcription once per server message (not once per part)
        if (outputTx?.text) {
          if (postGreetingListenGate && customerTurnsHeard === 0) {
            log.info('TIMING', `[GREETING-GATE] Suppressed pre-customer output transcription`);
          } else {
          const agentTranscript = outputTx.text;
          if (isThinkingText(agentTranscript)) {
            log.info('MEDIA', `ÃƒÂ°Ã…Â¸Ã‚Â§Ã‚Â  Filtered thinking (outputTx): "${agentTranscript.substring(0, 80)}..."`);
          } else {
            log.info('MEDIA', `ÃƒÂ°Ã…Â¸Ã‚Â¤Ã¢â‚¬â€œ Agent (transcript): ${agentTranscript}`);
            appendAgentTranscript(agentTranscript);
          }
          }
        }

        // Handle output transcription at the top level too
        const topOutputTx = sc?.outputTranscription || sc?.output_transcription;
        if (topOutputTx?.text && !modelTurn?.parts) {
          if (postGreetingListenGate && customerTurnsHeard === 0) {
            log.info('TIMING', `[GREETING-GATE] Suppressed pre-customer top-level transcription`);
          } else {
          const agentTranscript = topOutputTx.text;
          if (isThinkingText(agentTranscript)) {
            log.info('MEDIA', `ÃƒÂ°Ã…Â¸Ã‚Â§Ã‚Â  Filtered thinking (top): "${agentTranscript.substring(0, 80)}..."`);
          } else {
            log.info('MEDIA', `ÃƒÂ°Ã…Â¸Ã‚Â¤Ã¢â‚¬â€œ Agent (top-transcript): ${agentTranscript}`);
            appendAgentTranscript(agentTranscript);
          }
          }
        }

        // Handle setup complete
        if (response.setupComplete || response.setup_complete) {
          geminiSetupComplete = true;
          geminiSetupFailed = false;
          geminiFailureReason = '';
          startGeminiKeepalive();
          log.info('TIMING', `[T+${Date.now()-T0}ms] Gemini setupComplete ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â READY`);
          if (audioPacketsBufferedBeforeSetup > 0) {
            log.info('MEDIA', `Buffered ${audioPacketsBufferedBeforeSetup} inbound audio packets before setupComplete`, {
              audioPacketsBufferedBeforeSetup,
            });
          }
          if (activeConversationId) {
            db.prepare("UPDATE call_conversations SET status='active', updated_at=datetime('now') WHERE id=?").run(activeConversationId);
          }
          broadcast({ type: 'STATUS_UPDATE', status: 'ACTIVE' });

          const recoveringNow = geminiRecovering;
          geminiRecovering = false;
          if (recoveringNow && customerTurnsHeard > 0 && geminiWs.readyState === WebSocket.OPEN) {
            flushBufferedInboundAudio('reconnect_setup_complete');
            if (!requestPickupRecovery('gemini_reconnect')) {
              geminiWs.send(JSON.stringify({
                client_content: {
                  turns: [{
                    role: 'user',
                    parts: [{ text: 'The line briefly glitched. In ONE short sentence, apologise naturally, ask the customer to repeat their last point, and continue the dues conversation. Do not repeat the full greeting.' }],
                  }],
                  turn_complete: true,
                },
              }));
            }
            broadcast({ type: 'LOG', message: 'GEMINI: Reconnected ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â resuming live conversation', source: 'gem' });
            return;
          }

          const metadata = startEventMetadata || {};
          const exactOpeningGreetingForSetup = sanitizePreparedText(callAssets?.openingGreeting) || buildExactGreeting({
            customer_name: metadata.customerName || conv?.customer_name || 'customer',
            caller_name: metadata.agentName || conv?.caller_name || 'Omar',
            customer_gender: normalizeGender(metadata.customerGender || callAssets?.customer_gender || conv?.customer_gender || 'male', 'male'),
            caller_gender: resolveCallerGender(
              metadata.callerGender || callAssets?.caller_gender || conv?.caller_gender || conv?.callerGender,
              metadata.agentName || metadata.callerName || callAssets?.caller_name || conv?.caller_name || conv?.callerName || 'Omar',
              'male'
            ),
          });

          const usedPreparedGreeting = preparedGreetingPlayed || playPreparedGreeting();
          if (usedPreparedGreeting) {
            const greetingWasFallback = lastGreetingSource === 'connection-tone' || lastGreetingSource === 'none';
            
            if (greetingWasFallback && geminiWs && geminiWs.readyState === WebSocket.OPEN) {
              // CRITICAL: Smart greeting ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â if customer already spoke, skip greeting
              const smartInstruction = buildGeminiGreetingInstruction(exactOpeningGreetingForSetup, true);
              if (sendOpeningInstructionOnce(geminiWs, smartInstruction, 'setup_complete_fallback')) {
                log.info('MEDIA', `ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¢ Smart greeting instruction (customerTurns=${customerTurnsHeard})`);
              }
              // Flush audio AFTER instruction so Gemini prioritizes response
              setTimeout(() => {
                flushBufferedInboundAudio('setup_complete_after_greeting');
              }, 300);
            } else if (geminiWs && geminiWs.readyState === WebSocket.OPEN) {
              const smartInstruction = buildGeminiGreetingInstruction(exactOpeningGreetingForSetup, false);
              if (sendOpeningInstructionOnce(geminiWs, smartInstruction, 'setup_complete_prepared')) {
                log.info('MEDIA', `ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¢ Smart greeting instruction (customerTurns=${customerTurnsHeard})`);
              }
              setTimeout(() => {
                flushBufferedInboundAudio('setup_complete');
              }, 120);
            } else {
              flushBufferedInboundAudio('setup_complete');
            }

            log.info('MEDIA', 'ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ Gemini setup complete ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â prepared greeting already played, processing customer reply');
            broadcast({ type: 'LOG', message: 'GEMINI: Session ready ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â waiting after prepared greeting', source: 'gem' });
          } else if (geminiWs && geminiWs.readyState === WebSocket.OPEN) {
            greetingRequestSent = true;
            const openingInstruction = buildGeminiGreetingInstruction(exactOpeningGreetingForSetup, true);
            if (sendOpeningInstructionOnce(geminiWs, openingInstruction, 'setup_complete_no_prepared')) {
              log.info('MEDIA', 'ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ Fast greeting request sent');
              broadcast({ type: 'LOG', message: 'GEMINI: Session ready ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â greeting sent', source: 'gem' });
            }
          }
        }
      } catch (err) {
        log.error('MEDIA', 'Gemini message parse error:', err.message);
      }
    });

    geminiWs.on('error', (err) => {
      if (!isCurrentGeminiSocket()) return;
      log.error('MEDIA', `GEMINI ERROR: ${err.message}`);
      broadcast({ type: 'LOG', message: `GEMINI ERROR: ${err.message}`, source: 'gem' });
      geminiSetupFailed = true;
      geminiFailureReason = err.message;
    });

    geminiWs.on('close', (code, reason) => {
      const reasonText = Buffer.isBuffer(reason) ? reason.toString() : String(reason || '');
      const expectedClose = geminiWs._expectedClose === true;
      const normalClose = code === 1000;
      const isCurrentSocket = isCurrentGeminiSocket();
      if (isCurrentSocket) wsGemini = null;

      if (expectedClose || !isCurrentSocket) {
        log.info('MEDIA', `Gemini socket closed quietly: code=${code} reason="${reasonText}" expected=${expectedClose}`);
        return;
      }

      if (normalClose) {
        log.warn('MEDIA', `Gemini closed normally before call end: code=${code} reason="${reasonText}"`);
      } else {
        log.error('MEDIA', `ÃƒÂ¢Ã‚ÂÃ…â€™ Gemini closed: code=${code} reason="${reasonText}"`);
      }
      broadcast({ type: 'LOG', message: `GEMINI CLOSED: code=${code} reason=${reasonText}`, source: 'gem' });

      geminiSetupFailed = true;
      geminiFailureReason = `Gemini disconnected: ${code} ${reasonText}`;

      if (scheduleGeminiReconnect(!geminiSetupComplete ? 'setup_disconnect' : 'live_disconnect')) {
        log.info('TIMING', `[T+${Date.now()-T0}ms] Gemini bridge dropped (code=${code}) ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â reconnecting`);
        return;
      }

      if (!conversationEnded && !forceEndingCall && !closingRequested) {
        // Skip if call already ended via Twilio stop_event (prevents cascade double-end)
        if (endHandled) {
          log.info('TIMING', `[SKIP] Gemini close after Twilio already ended ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â no force-end`);
          return;
        }
        if (geminiReconnectAttempts < MAX_GEMINI_RECONNECTS) {
          log.warn('TIMING', `[RECOVERY] Gemini died (code=${code}) ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â attempting emergency reconnect (${geminiReconnectAttempts+1}/${MAX_GEMINI_RECONNECTS})`);
          scheduleGeminiReconnect('emergency_' + (code === 1000 ? 'normal_close' : 'unexpected_close'));
        } else {
          setEndCause('gemini', code === 1000 ? 'normal_close_mid_call' : 'unexpected_close', {
            code, reasonText, geminiSetupComplete, greetingRequestSent, audioPacketsBufferedBeforeSetup,
          }, true);
          log.info('TIMING', `[END] Gemini died mid-call (code=${code}) after ${MAX_GEMINI_RECONNECTS} reconnects ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â graceful hangup queued`);
          broadcast({ type: 'LOG', message: 'ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â AI disconnected ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â ending call', source: 'sys' });
          scheduleGracefulHangup('gemini_disconnected', 1800);
        }
      }
    });
  }

  twilioWs.on('message', async (message) => {
    try {
      const msg = JSON.parse(message.toString());

      // START EVENT
      if (msg.event === 'start') {
        streamSid = msg.start.streamSid;
        callSid = msg.start.callSid;
        twilioWs._isAlive = true;
        const metadata = msg.start.customParameters || {};

        // Update active call registry
        if (activeConversationId && ACTIVE_CALLS.has(activeConversationId)) {
          const entry = ACTIVE_CALLS.get(activeConversationId);
          entry.callSid = callSid;
          entry.customerName = metadata.customerName || '';
        }

        log.info('TIMING', `[T+${Date.now()-T0}ms] ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ Customer ANSWERED ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â Twilio stream started (${streamSid}), activeCalls: ${ACTIVE_CALLS.size}`);
        broadcast({ type: 'LOG', message: `TWILIO: Customer answered ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â stream started`, source: 'twil' });

        // Bind conversation from parameters
        const paramConvId = metadata.conversationId || metadata.conversation_id || '';
        if (paramConvId && paramConvId !== activeConversationId) {
          activeConversationId = paramConvId;
          loadConversation();
        }

        if (activeConversationId && callSid) {
          db.prepare('UPDATE call_conversations SET call_sid = ? WHERE id = ?').run(callSid, activeConversationId);
        }

        // Non-blocking asset refresh: do NOT hold the greeting path waiting on TTS warmup
        const warmPromise = activeConversationId ? CALL_PREPARATION_PROMISES.get(activeConversationId) : null;
        if (warmPromise) {
          warmPromise.then(() => {
            callAssets = CALL_PREPARATION_CACHE.get(activeConversationId) || callAssets || null;
            log.info('MEDIA', `Warmup resolved async ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â greeting payloads: ${callAssets?.openingGreetingPayloads?.length || 0}`);
          }).catch((e) => {
            log.warn('MEDIA', `Warmup promise failed: ${e.message}`);
          });
        }

        // Play greeting IMMEDIATELY with tone fallback ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â customer must hear SOMETHING within 500ms
        const greetingPlayedNow = playPreparedGreeting({ allowToneFallback: true });
        if (!greetingPlayedNow) {
          setTimeout(() => {
            if (preparedGreetingPlayed || conversationEnded || forceEndingCall || closingRequested) return;
            playPreparedGreeting({ allowToneFallback: true, forceConnectionTone: true });
          }, 120);
        }

        // If Gemini wasn't pre-connected (e.g. no conversationId initially), connect now
        if (!wsGemini && !earlyGeminiStarted) {
          startEarlyGeminiConnection();
        }
        // Store metadata for Gemini setup (used by setupGeminiHandlers)
        startEventMetadata = metadata;
      }

      // MEDIA EVENT - Send audio to Gemini with NOISE GATE + NETWORK MONITORING
      if (msg.event === 'media') {
        mediaEventCount++;
        twilioWs._isAlive = true;
        if (mediaEventCount === 1) {
          log.info('TIMING', `[T+${Date.now()-T0}ms] First audio packet from Twilio`);
        }

        // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Network quality tracking ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
        const netQuality = updateNetworkMonitor(networkMonitor);
        twilioInboundGapAlertLevel = 0;
        if (networkMonitor.totalMediaPackets % 500 === 0) {
          const jitter = getNetworkJitter(networkMonitor);
          log.info('MEDIA', `ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¶ Network: quality=${netQuality}, jitter=${jitter}ms, maxGap=${networkMonitor.maxGapMs}ms, gaps=${networkMonitor.mediaGaps.length}`);
        }
        // Log network degradation events
        if (netQuality === 'poor' && networkMonitor.mediaGaps.length > 0) {
          const lastGap = networkMonitor.mediaGaps[networkMonitor.mediaGaps.length - 1];
          if (lastGap.at === networkMonitor.lastMediaAt) {
            log.warn('MEDIA', `ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â NETWORK: ${lastGap.gapMs}ms gap detected ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â customer line may be unstable`);
            broadcast({ type: 'LOG', message: `ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â Network gap: ${lastGap.gapMs}ms`, source: 'twil' });
          }
        }

        let muLawBuffer = null;
        try {
          muLawBuffer = Buffer.from(msg.media.payload, 'base64');
          inboundChunks.push(Buffer.from(muLawBuffer));
        } catch {}

        if (finalClosingPlaybackLock || suppressCustomerInputDuringEnding || closingRequested || conversationEnded || forceEndingCall) {
          return;
        }

        // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ NOISE GATE: Filter background noise BEFORE sending to Gemini ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
        // Only active when noiseCancellation is enabled in settings (OFF by default)
        const noiseGateEnabled = callAssets?.noiseCancellation === true;
        if (noiseGateEnabled && muLawBuffer && geminiSetupComplete) {
          const gateResult = shouldPassAudio(noiseGate, muLawBuffer);
          
          // Log noise gate stats periodically
          if (noiseGate.totalPackets === NOISE_GATE.CALIBRATION_PACKETS) {
            log.info('MEDIA', `ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Â¡ Noise gate calibrated: floor=${Math.round(noiseGate.noiseFloor)} RMS, threshold=${Math.round(Math.max(NOISE_GATE.SILENCE_RMS_THRESHOLD, noiseGate.noiseFloor * NOISE_GATE.NOISE_FLOOR_MULTIPLIER))} RMS`);
          }
          if (noiseGate.totalPackets % 1000 === 0) {
            const filterRate = noiseGate.totalPackets > 0 ? Math.round(noiseGate.filteredPackets / noiseGate.totalPackets * 100) : 0;
            log.info('MEDIA', `ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Â¡ Noise gate stats: total=${noiseGate.totalPackets}, filtered=${noiseGate.filteredPackets} (${filterRate}%), speech=${noiseGate.speechPackets}, gate=${noiseGate.gateOpen ? 'OPEN' : 'CLOSED'}`);
          }
          
          if (!gateResult.pass) {
            // Audio is noise ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â don't send to Gemini
            return;
          }
        }

        // If Gemini is not connected, buffer silently ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â don't crash
        if (!wsGemini || wsGemini.readyState !== WebSocket.OPEN) {
          bufferInboundAudio(muLawBuffer, 'waiting_for_gemini_socket');
          if ((!wsGemini || wsGemini.readyState === WebSocket.CLOSED) && !geminiRecovering && !conversationEnded && !forceEndingCall && !closingRequested) {
            startEarlyGeminiConnection({ forceFresh: true, reason: 'media_before_gemini_ready' });
          }
          return;
        }

        if (!geminiSetupComplete) {
          bufferInboundAudio(muLawBuffer, 'waiting_for_gemini_setup');
          return;
        }

        try {
          if (!muLawBuffer) return;
          sendAudioPacketToGemini(muLawBuffer);
        } catch (err) {
          log.error('MEDIA', 'Audio processing failed:', err.message);
        }
      }

      // STOP EVENT
      if (msg.event === 'stop') {
        twilioWs._isAlive = true;
        setEndCause('twilio', 'stop_event', { mediaEventCount, streamSid, callSid });
        log.info('MEDIA', 'ÃƒÂ¢Ã‚ÂÃ…â€™ Stream stopped');
        broadcast({ type: 'LOG', message: 'TWILIO: Stream stopped', source: 'twil' });
        // Log final noise gate & network stats
        const filterRate = noiseGate.totalPackets > 0 ? Math.round(noiseGate.filteredPackets / noiseGate.totalPackets * 100) : 0;
        log.info('MEDIA', `ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã…Â  Final stats ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â media: ${mediaEventCount}, noiseFiltered: ${noiseGate.filteredPackets}/${noiseGate.totalPackets} (${filterRate}%), speech: ${noiseGate.speechPackets}, noiseFloor: ${Math.round(noiseGate.noiseFloor)} RMS`);
        log.info('MEDIA', `ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¶ Network final ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â quality: ${networkMonitor.networkQuality}, maxGap: ${networkMonitor.maxGapMs}ms, jitter: ${getNetworkJitter(networkMonitor)}ms, gaps: ${networkMonitor.mediaGaps.length}`);
        if (twilioHeartbeat) clearInterval(twilioHeartbeat);
        void handleCallEnd().catch((endErr) => {
          log.error('MEDIA', `handleCallEnd failed after stop event: ${endErr.message}`);
        });
      }
    } catch (err) {
      log.error('MEDIA', 'Twilio message error:', err.message);
    }
  });

  // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Twilio WebSocket heartbeat ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â detect silent disconnects ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
  let twilioHeartbeat = setInterval(() => {
    if (twilioWs.readyState !== WebSocket.OPEN) {
      clearInterval(twilioHeartbeat);
      return;
    }
    if (networkMonitor.lastMediaAt <= 0 || conversationEnded || forceEndingCall || closingRequested) return;

    const gapMs = Date.now() - networkMonitor.lastMediaAt;
    if (gapMs >= 30000 && twilioInboundGapAlertLevel < 2) {
      twilioInboundGapAlertLevel = 2;
      log.warn('MEDIA', `ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â HEARTBEAT: No Twilio inbound media for ${Math.round(gapMs / 1000)}s ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â keeping call alive and relying on silence/recovery flow`);
      broadcast({ type: 'LOG', message: 'ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â Customer audio gap detected ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â call kept alive', source: 'twil' });
      return;
    }

    if (gapMs >= 10000 && twilioInboundGapAlertLevel < 1) {
      twilioInboundGapAlertLevel = 1;
      log.warn('MEDIA', `ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â HEARTBEAT: No Twilio media for ${Math.round(gapMs / 1000)}s ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â monitoring only`);
      broadcast({ type: 'LOG', message: 'ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â Customer audio gap detected ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â monitoring...', source: 'twil' });
    }
  }, 5000);

  twilioWs.on('error', (error) => {
    log.error('MEDIA', `WebSocket error (convId=${activeConversationId}): ${error.message}`);
    broadcast({ type: 'LOG', message: `WEBSOCKET ERROR: ${error.message}`, source: 'twil' });
    if (twilioHeartbeat) clearInterval(twilioHeartbeat);
    setEndCause('twilio', 'stream_error', { message: error.message, mediaEventCount, streamSid, callSid });
    void handleCallEnd().catch((endErr) => {
      log.error('MEDIA', `handleCallEnd failed after ws error: ${endErr.message}`);
    });
  });

  twilioWs.on('close', (code, reasonBuf) => {
    const closeReason = reasonBuf?.toString?.() || '';
    const endReason = closeReason === 'manual_end_requested' ? 'manual_end_requested' : 'stream_closed';
    setEndCause('twilio', endReason, { mediaEventCount, streamSid, callSid, code });
    // Log final noise gate & network stats
    const filterRate = noiseGate.totalPackets > 0 ? Math.round(noiseGate.filteredPackets / noiseGate.totalPackets * 100) : 0;
    log.info('MEDIA', `Twilio stream closed ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â filtered ${filterRate}% noise, network quality: ${networkMonitor.networkQuality}`);
    broadcast({ type: 'LOG', message: 'TWILIO: Stream closed', source: 'twil' });
    if (twilioHeartbeat) clearInterval(twilioHeartbeat);
    // Don't close Gemini here ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â handleCallEnd will close it after analysis
    void handleCallEnd().catch((endErr) => {
      log.error('MEDIA', `handleCallEnd failed after stream close: ${endErr.message}`);
    });
  });

  // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Handle call end ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â save recording & analyze ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
  let endHandled = false;
  async function handleCallEnd() {
    if (endHandled) return;
    endHandled = true;
    clearGracefulHangupTimer();
    // Remove from active call registry
    if (activeConversationId) ACTIVE_CALLS.delete(activeConversationId);
    log.info('MEDIA', `Active calls remaining: ${ACTIVE_CALLS.size}`);
    // Clear all timers
    if (silenceTimer) { clearTimeout(silenceTimer); silenceTimer = null; }
    clearInitialCustomerResponseTimer();
    cancelGreetingRetryTimer();
    stopGeminiKeepalive();
    clearAgentWatchdog();
    // Flush any pending transcript buffers before analysis
    if (customerTranscriptTimer) { clearTimeout(customerTranscriptTimer); flushCustomerTranscript(); }
    if (agentTranscriptTimer) { clearTimeout(agentTranscriptTimer); flushAgentTranscript(); }

    const duration = Math.floor((Date.now() - callStartTime) / 1000);
    log.info('MEDIA', `Call duration: ${duration}s, Messages: ${messages.length}`);
    log.info('MEDIA', `End cause => ${endCause.source}:${endCause.reason}`, endCause.details || {});
    broadcast({ type: 'STATUS_UPDATE', status: 'ENDED' });

    // NOTE: Do NOT close Gemini here ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â analysis runs first, Gemini closes after

    try {
      let recordingUrl = null;

      // Always use Twilio recording ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â the /api/recording-status callback will handle download & storage
      // Store conversation ID in call_log so the callback can find and update it
      log.info('MEDIA', 'Using Twilio recording ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â callback will update recording_url after call ends');

      // Analyze & save to DB
      if (conv) {
        const latestConv = activeConversationId
          ? db.prepare('SELECT * FROM call_conversations WHERE id = ?').get(activeConversationId)
          : conv;

        if (['no-answer', 'busy', 'failed', 'canceled', 'cancelled', 'machine-detected'].includes(latestConv?.status || '')) {
          return;
        }

        let finalMessages = await correctCustomerMessagesWithGemini(messages);
        const transcriptBuild = await generateFinalTranscriptWithGemini(finalMessages, {
          callerName: conv?.caller_name || 'Agent',
          customerName: conv?.customer_name || 'Customer',
        });
        let transcript = transcriptBuild.transcript || buildTranscriptFromMessages(finalMessages, conv?.caller_name || 'Agent', conv?.customer_name || 'Customer');
        if (transcriptBuild.ok) {
          const parsedFromGemini = parseTranscriptToMessages(transcript, conv?.caller_name || 'Agent', conv?.customer_name || 'Customer');
          if (parsedFromGemini.length > 0) {
            finalMessages = parsedFromGemini;
          }
        }
        if (transcriptBuild.ok) {
          log.success('TRANSCRIPT', `Gemini transcript generated at call end for conversation ${activeConversationId || 'unknown'}`);
        } else {
          log.warn('TRANSCRIPT', `Using raw transcript fallback for conversation ${activeConversationId || 'unknown'}: ${transcriptBuild.reason || 'unknown'}`);
        }
        
        // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ DEBUG: Save full transcript to file for inspection ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
        const debugDir = path.join(__dirname, '../data/recordings/debug', activeConversationId || 'unknown');
        try {
          if (!existsSync(debugDir)) mkdirSync(debugDir, { recursive: true });
          writeFileSync(path.join(debugDir, 'transcript_raw.txt'), transcript, 'utf8');
          writeFileSync(path.join(debugDir, 'messages_array.json'), JSON.stringify(finalMessages, null, 2), 'utf8');
          log.info('DEBUG', `ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ Saved transcript to: ${debugDir}/transcript_raw.txt`);
        } catch (e) {
          log.warn('DEBUG', `Failed to write debug transcript: ${e.message}`);
        }
        
        // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Determine analysis based on call content ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
        let analysis;
        let analysisMessages = finalMessages;
        const initialCustomerMsgCount = finalMessages.filter(m => m.role === 'customer' && String(m.text || '').trim()).length;
        const transcriptLooksIncompleteInitial = (
          (customerTurnsHeard > 0 && initialCustomerMsgCount === 0) ||
          !String(transcript || '').trim() ||
          String(transcript || '').trim().length < 18 ||
          (customerTurnsHeard > 0 && transcriptBuild.ok === false && initialCustomerMsgCount <= 1)
        );

        if (transcriptLooksIncompleteInitial) {
          const rescue = await fetchTranscriptFromTwilioRecordingNow({
            callSid,
            customerName: conv?.customer_name || 'Customer',
            callerName: conv?.caller_name || 'Agent',
            maxAttempts: 4,
          });

          if (rescue.ok && rescue.transcript) {
            transcript = rescue.transcript;
            const parsed = parseTranscriptToMessages(transcript, conv?.caller_name || 'Agent', conv?.customer_name || 'Customer');
            if (parsed.length > 0) analysisMessages = parsed;
            finalMessages = analysisMessages;

            if (!recordingUrl && rescue.recordingSid && rescue.buffer?.length) {
              try {
                const fileName = `twilio-${rescue.recordingSid}.${rescue.ext || 'mp3'}`;
                writeFileSync(path.join(RECORDINGS_DIR, fileName), rescue.buffer);
                recordingUrl = `/api/recordings/${fileName}`;
                log.success('RECORDING', `[RESCUE] Recording downloaded immediately at call end: ${fileName}`);
              } catch (saveErr) {
                log.warn('RECORDING', `[RESCUE] Recording save failed: ${saveErr.message}`);
              }
            }

            log.success('TRANSCRIPT', `[RESCUE] Rebuilt transcript from Twilio recording for immediate outcome analysis`);
          } else {
            log.warn('TRANSCRIPT', `[RESCUE] Recording transcript unavailable before analysis: ${rescue.reason || 'unknown'}`);
          }
        }

        const customerMsgCount = analysisMessages.filter(m => m.role === 'customer' && String(m.text || '').trim()).length;
        const contextualPtpSignal = checkTranscriptForPtp(transcript, analysisMessages);
        const transcriptLooksIncomplete = (
          (customerTurnsHeard > 0 && customerMsgCount === 0) ||
          !String(transcript || '').trim() ||
          String(transcript || '').trim().length < 18 ||
          (customerTurnsHeard > 0 && transcriptBuild.ok === false && customerMsgCount <= 1 && !contextualPtpSignal)
        );
        log.info('TIMING', `[ANALYSIS] Starting post-call analysis. Total messages: ${analysisMessages.length}, Transcript length: ${transcript.length}`);
        
        // Log full transcript for debugging
        const customerLines = analysisMessages.filter(m => m.role === 'customer').map(m => m.text);
        const agentLines = analysisMessages.filter(m => m.role === 'agent').map(m => m.text);
        log.info('TIMING', `[ANALYSIS-DEBUG] Customer (${customerLines.length} msgs): ${customerLines.join(' | ')}`);
        log.info('TIMING', `[ANALYSIS-DEBUG] Agent (${agentLines.length} msgs): ${agentLines.join(' | ')}`);
        
        const customerOnlyLower = analysisMessages.filter(m => m.role === 'customer').map(m => m.text.toLowerCase()).join(' ');
        const strictNonCustomerEvidence = detectNonCustomerSemantic(customerOnlyLower);
        const callbackIntentPresent =
          detectCallbackRequestedSemantic(customerOnlyLower) ||
          /\b(busy|baad\s+mein|call\s+later|minute|mint|minut|ghante|ghanta)\b/.test(customerOnlyLower);
        const refusalIntentPresent =
          /\b(nahi\s+doonga|nahi\s+dunga|nahi\s+doongi|nahi\s+dungi|nahi\s+karunga|nahi\s+karungi|refuse)\b/.test(customerOnlyLower);
        const identityConfirmed = hasIdentityConfirmation(customerOnlyLower, conv?.customer_name || '');
        const nonCustomerSemantic =
          strictNonCustomerEvidence &&
          !callbackIntentPresent &&
          !refusalIntentPresent &&
          !identityConfirmed;

        const runtimeNonCustomerLocked = nonCustomerIntentLocked || currentConversationState === 'NON_CUSTOMER' || String(endCause.reason || '').startsWith('non_customer');

        if (runtimeNonCustomerLocked) {
          analysis = {
            final_response: 'non_customer_pickup',
            ptp_date: null,
            ptp_status: null,
            notes: 'Runtime non-customer flow lock active tha, is liye outcome non_customer_pickup set kiya gaya',
            schedule_retry_hours: 5,
            semantic_winner: 'runtime_non_customer_lock',
          };
          log.info('TIMING', '[ANALYSIS] Runtime non-customer lock applied before transcript heuristics');
        } else if (customerTurnsHeard === 0 && customerMsgCount === 0) {
          analysis = {
            final_response: 'no_answer',
            ptp_date: null,
            ptp_status: null,
            notes: 'No customer communication captured at all',
            schedule_retry_hours: 2,
            semantic_winner: 'no_communication_system_path',
          };
          log.info('TIMING', '[ANALYSIS] No pickup/no speech detected - no_answer assigned (Gemini skipped)');
        } else if (transcriptLooksIncomplete) {
          analysis = {
            final_response: 'callback_requested',
            ptp_date: null,
            ptp_status: null,
            notes: 'Transcript incomplete at call end; follow-up callback scheduled for accurate outcome.',
            schedule_retry_hours: 2,
          };
          log.warn('TIMING', '[ANALYSIS] Transcript completeness guard triggered ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â deferring to callback_requested');
        } else if (contextualPtpSignal) {
          analysis = contextualPtpSignal;
          log.info('TIMING', '[ANALYSIS] Contextual PTP signal detected from short customer reply; bypassing callback fallback');
        } else if (nonCustomerSemantic) {
          analysis = {
            final_response: 'non_customer_pickup',
            ptp_date: null,
            ptp_status: null,
            notes: 'Customer account holder nahi tha, message relay possible nahi tha',
            schedule_retry_hours: 5,
          };
          log.info('TIMING', '[ANALYSIS] Non-customer semantic path applied (deterministic)');
        } else if (analysisMessages.length > 0) {
          const hasMeaningfulCustomerCommunication =
            customerTurnsHeard > 0 ||
            customerMsgCount > 0 ||
            String(customerOnlyLower || '').trim().length > 0;

          if (!hasMeaningfulCustomerCommunication) {
            // Requirement: if there is no communication, skip Gemini prediction entirely.
            analysis = {
              final_response: 'no_answer',
              ptp_date: null,
              ptp_status: null,
              notes: 'No customer communication captured; Gemini analysis skipped',
              schedule_retry_hours: 2,
              semantic_winner: 'no_communication_system_path',
            };
            log.info('TIMING', '[ANALYSIS] No communication path applied - Gemini skipped');
          } else {
            analysis = await analyzeConversation(analysisMessages, conv);
          }
        } else if (endCause.reason === 'no_customer_audio_after_pickup' && customerTurnsHeard === 0) {
          analysis = {
            final_response: 'no_answer',
            ptp_date: null,
            ptp_status: null,
            notes: 'Pickup detect hua lekin customer speech nahi mili',
            schedule_retry_hours: 2,
            semantic_winner: 'no_communication_system_path',
          };
        } else if (geminiSetupFailed) {
          analysis = {
            final_response: 'callback_requested',
            ptp_date: null,
            notes: `Bridge failed: ${geminiFailureReason} | source=${endCause.source} reason=${endCause.reason}`,
            schedule_retry_hours: 2,
            semantic_winner: 'gemini_bridge_failed_fallback',
          };
        } else if (MAX_GREETING_RETRIES > 0 && greetingRetryCount >= MAX_GREETING_RETRIES && customerTurnsHeard === 0) {
          analysis = {
            final_response: 'no_answer',
            ptp_date: null,
            ptp_status: null,
            notes: `${MAX_GREETING_RETRIES}x greeting played - customer response nahi mili`,
            schedule_retry_hours: 2,
            semantic_winner: 'no_communication_system_path',
          };
        } else {
          analysis = {
            final_response: duration > 10 ? 'callback_requested' : 'no_answer',
            ptp_date: null,
            ptp_status: null,
            notes: `No transcript captured | source=${endCause.source} reason=${endCause.reason}`,
            schedule_retry_hours: 2,
            semantic_winner: duration > 10 ? 'fallback_callback' : 'no_communication_system_path',
          };
        }

        analysis = finalizeSemanticOutcome(analysis);
        analysis.notes = sanitizeLogText(analysis.notes || '') || null;
        const liveGeminiPrediction = buildPredictionSnapshot('gemini_live', analysis, {
          transcript_source: 'live_transcript',
          transcript_length: transcript.length,
          message_count: analysisMessages.length,
        });
        const semanticLogicAnalysis = analyzeConversationWithSemanticPrediction(analysisMessages, conv, transcript);
        const semanticCustomerText = analysisMessages
          .filter((m) => m.role === 'customer')
          .map((m) => m.text || '')
          .join(' ')
          .trim();
        const semanticCommitmentScore = analyzePaymentCommitmentQuality(semanticCustomerText);
        const semanticDateHint = smartExtractPtpDate(semanticCustomerText || transcript);
        const semanticLogicPrediction = buildPredictionSnapshot('semantic_logic', semanticLogicAnalysis, {
          transcript_source: 'live_transcript',
          transcript_length: transcript.length,
          message_count: analysisMessages.length,
          semantic_commitment_score: semanticCommitmentScore,
          semantic_date_hint: semanticDateHint,
        });
        let recordingBasedPrediction;
        try {
          recordingBasedPrediction = await predictOutcomeFromRecording({ recordingUrl, callSid, conv });
        } catch (recordingErr) {
          recordingBasedPrediction = buildPredictionSnapshot('recording_based', null, {
            reason: `recording_prediction_error:${recordingErr?.message || 'unknown'}`,
            transcript_source: null,
            transcript_length: 0,
          });
          log.info('TIMING', `[PREDICT-RECORDING] unavailable | reason=${recordingBasedPrediction.reason}`);
        }
        let finalizationSource = 'GEMINI-LIVE-FINAL';
        const recordingPredictionUsable = Boolean(
          recordingBasedPrediction?.available &&
          recordingBasedPrediction?.outcome
        );
        const liveHasStrongPtp =
          liveGeminiPrediction?.outcome === 'ptp_secured' &&
          !!liveGeminiPrediction?.ptp_date;
        const recordingLooksAmbiguousFallback =
          ['callback_requested', 'busy', null].includes(recordingBasedPrediction?.outcome || null) &&
          ['gemini_failed_semantic_fallback', 'semantic_ambiguous_safe_callback', null].includes(recordingBasedPrediction?.semantic_winner || null);
        const recordingPredictionPending = Boolean(
          !recordingPredictionUsable &&
          callSid &&
          !String(callSid).startsWith('MOCK_') &&
          !['no-answer', 'busy', 'failed', 'canceled'].includes(String(latestConv?.status || '').toLowerCase())
        );

        if (recordingPredictionUsable && !(liveHasStrongPtp && recordingLooksAmbiguousFallback)) {
          const recordingPrimary = finalizeSemanticOutcome({
            ...analysis,
            final_response: recordingBasedPrediction.outcome,
            ptp_date: recordingBasedPrediction.ptp_date || null,
            ptp_status: recordingBasedPrediction.ptp_status || null,
            notes: sanitizeLogText(recordingBasedPrediction.notes || '') || analysis.notes || null,
            schedule_retry_hours: recordingBasedPrediction.retry_in_hours ?? analysis.schedule_retry_hours ?? 0,
            semantic_winner: recordingBasedPrediction.semantic_winner || 'recording_prediction_primary',
            semantic_confidence: Math.max(Number(recordingBasedPrediction.semantic_confidence || 0), Number(analysis.semantic_confidence || 0), 75),
          });
          if (recordingPrimary.final_response === 'ptp_secured' && !recordingPrimary.ptp_status) {
            recordingPrimary.ptp_status = 'pending';
          }
          analysis = recordingPrimary;
          finalizationSource = 'GEMINI-RECORDING-PRIMARY';
          log.info('TIMING', `[ARBITRATION] Recording prediction selected as active final outcome | outcome=${analysis.final_response || 'null'} | ptp_date=${analysis.ptp_date || 'none'} | rec_msgs=${recordingBasedPrediction.message_count || 0}`);
        } else if (recordingPredictionUsable && liveHasStrongPtp && recordingLooksAmbiguousFallback) {
          finalizationSource = 'GEMINI-LIVE-PTP-LOCK';
          log.warn('TIMING', `[ARBITRATION] Keeping live PTP outcome; recording prediction looked ambiguous fallback (live=${liveGeminiPrediction.outcome}/${liveGeminiPrediction.ptp_date || 'none'}, recording=${recordingBasedPrediction.outcome || 'null'}, winner=${recordingBasedPrediction.semantic_winner || 'n/a'})`);
        }

        const currentUsedPrediction = buildPredictionSnapshot('current_used', analysis, {
          transcript_source: finalizationSource === 'GEMINI-LIVE-FINAL' ? 'live_transcript' : (recordingBasedPrediction?.transcript_source || 'recording_transcript'),
          transcript_length: finalizationSource === 'GEMINI-LIVE-FINAL' ? transcript.length : Number(recordingBasedPrediction?.transcript_length || transcript.length || 0),
          message_count: finalizationSource === 'GEMINI-LIVE-FINAL' ? analysisMessages.length : Number(recordingBasedPrediction?.message_count || analysisMessages.length || 0),
        });
        const algo1Line = formatPredictionSnapshotForLog('ALGO-1-SEMANTIC', semanticLogicPrediction);
        const algo2Line = formatPredictionSnapshotForLog('ALGO-2-GEMINI-LIVE', liveGeminiPrediction);
        const algo3Line = formatPredictionSnapshotForLog('ALGO-3-GEMINI-RECORDING', recordingBasedPrediction);
        log.info('TIMING', '[PREDICTION-COMPARISON] ===== START =====');
        log.info('TIMING', '[PREDICTION-COMPARISON] 1. Semantic approach - regex/date extraction + deterministic outcome');
        log.info('TIMING', algo1Line);
        log.info('TIMING', '[PREDICTION-COMPARISON] 2. Gemini live/TTS path - currently used final outcome');
        log.info('TIMING', algo2Line);
        log.info('TIMING', '[PREDICTION-COMPARISON] 3. Gemini recording API path - post-call recording processing');
        log.info('TIMING', algo3Line);
        log.info('TIMING', `[PREDICTION-COMPARISON] FINALIZED-USING=${finalizationSource} | outcome=${analysis.final_response || 'null'} | ptp_date=${analysis.ptp_date || 'none'} | retry=${analysis.schedule_retry_hours ?? 'none'}h | winner=${analysis?.semantic_winner || `final_response_${analysis?.final_response || 'unknown'}`}`);
        log.info('TIMING', `[ALGO-RESULTS] ${JSON.stringify({
          semantic: { outcome: semanticLogicPrediction?.outcome || null, winner: semanticLogicPrediction?.semantic_winner || null, retry_in_hours: semanticLogicPrediction?.retry_in_hours ?? null },
          gemini_live: { outcome: liveGeminiPrediction?.outcome || null, winner: liveGeminiPrediction?.semantic_winner || null, retry_in_hours: liveGeminiPrediction?.retry_in_hours ?? null },
          gemini_recording: { outcome: recordingBasedPrediction?.outcome || null, winner: recordingBasedPrediction?.semantic_winner || null, retry_in_hours: recordingBasedPrediction?.retry_in_hours ?? null, available: recordingBasedPrediction?.available !== false }
        })}`);
        broadcast({ type: 'LOG', message: algo1Line, source: 'sys' });
        broadcast({ type: 'LOG', message: algo2Line, source: 'sys' });
        broadcast({ type: 'LOG', message: algo3Line, source: 'sys' });
        log.info('TIMING', '[PREDICTION-COMPARISON] ===== END =====');
        log.info('TIMING', `[OUTCOME] Final outcome: ${analysis.final_response} | PTP date: ${analysis.ptp_date || 'none'} | PTP status: ${analysis.ptp_status || 'none'} | Retry: ${analysis.schedule_retry_hours}h | Notes: ${analysis.notes}`);
        log.info('TIMING', `[SEMANTIC-WINNER] winner=${analysis?.semantic_winner || `final_response_${analysis?.final_response || 'unknown'}`}`);
        log.info('TIMING', `[PREDICT-CURRENT] outcome=${currentUsedPrediction.outcome || 'null'} | winner=${currentUsedPrediction.semantic_winner || 'n/a'} | retry=${currentUsedPrediction.retry_in_hours ?? 'none'}h`);
        log.info('TIMING', `[PREDICT-SEMANTIC] outcome=${semanticLogicPrediction.outcome || 'null'} | winner=${semanticLogicPrediction.semantic_winner || 'n/a'} | retry=${semanticLogicPrediction.retry_in_hours ?? 'none'}h`);
        if (recordingBasedPrediction.available) {
          log.info('TIMING', `[PREDICT-RECORDING] outcome=${recordingBasedPrediction.outcome || 'null'} | source=${recordingBasedPrediction.transcript_source || 'unknown'} | retry=${recordingBasedPrediction.retry_in_hours ?? 'none'}h`);
        } else {
          log.info('TIMING', `[PREDICT-RECORDING] unavailable | reason=${recordingBasedPrediction.reason || 'unknown'}`);
        }
        broadcast({ type: 'LOG', message: `Outcome: ${analysis.final_response}${analysis.ptp_date ? ` | PTP: ${analysis.ptp_date}` : ''}`, source: 'sys' });

        const scheduledRetryAt = computeScheduledRetryAt(analysis.schedule_retry_hours || 0);
        const normalizedFinalResponseForPending = analysis.final_response || null;
        const pendingCustomerRetryReason = recordingPredictionPending
          ? withRecordingPendingMarker(analysis.final_response || 'pending_recording_analysis')
          : (scheduledRetryAt ? sanitizeLogText(`${normalizedFinalResponseForPending || 'no_outcome'} - ${analysis.schedule_retry_hours}h retry`) : null);
        const pendingCallLogNotes = recordingPredictionPending
          ? withRecordingPendingMarker(analysis.notes || 'Recording prediction is still processing')
          : analysis.notes || null;

        if (activeConversationId) {
          db.prepare(`UPDATE call_conversations SET status='completed', final_response=?, ptp_date=?, notes=?, schedule_retry_hours=?, updated_at=datetime('now') WHERE id=?`)
            .run(
              analysis.final_response || null,
              analysis.ptp_date || null,
              pendingCallLogNotes,
              analysis.schedule_retry_hours || 0,
              activeConversationId
            );
        }

        // Update customer record comprehensively
        const customer = db.prepare('SELECT * FROM customers WHERE id = ?').get(conv.customer_id);
        if (customer) {
          const newFollowUp = (customer.follow_up_count || 0) + 1;
          let newPtpStatus = customer.ptp_status;
          let newPtpDate = customer.ptp_date;

          if (analysis.final_response === 'ptp_secured') {
            newPtpStatus = 'pending';
            if (analysis.ptp_date) {
              const d = new Date(analysis.ptp_date);
              newPtpDate = isNaN(d.getTime()) ? analysis.ptp_date : d.toISOString();
            } else {
              const custText = analysisMessages.filter(m => m.role === 'customer').map(m => m.text).join(' ').toLowerCase();
              const extracted = smartExtractPtpDate(custText);
              newPtpDate = extracted || customer.ptp_date;
            }
            log.info('TIMING', `[CUSTOMER-UPDATE] PTP secured -> ptpDate=${newPtpDate}, ptpStatus=${newPtpStatus}`);
          } else if (['refused', 'negotiation_barrier'].includes(analysis.final_response) && customer.ptp_status === 'pending') {
            newPtpStatus = 'broken';
          }

          const newPriority = (newFollowUp * 10) + ((Number(customer.balance) / 1000) * 5) + (customer.dpd * 3);

          let newTone = customer.assigned_tone;
          if (newPtpStatus === 'broken') newTone = 'assertive';
          else if (customer.dpd >= 60 || newFollowUp > 5) newTone = 'assertive';
          else if (customer.dpd >= 30 || newFollowUp >= 2) newTone = 'empathetic';
          else newTone = 'polite';

          let newAgent = customer.assigned_agent;
          if (newPtpStatus === 'broken') newAgent = 'broken_promise';
          else if (newPtpDate) {
            const ptpD = new Date(newPtpDate);
            const todayD = new Date();
            const diffDays = Math.ceil((ptpD.getTime() - todayD.getTime()) / (1000 * 60 * 60 * 24));
            if (diffDays === 1) newAgent = 'ptp_reminder';
            else if (diffDays === 0) newAgent = 'ptp_followup';
          } else if (newFollowUp <= 1) newAgent = 'fresh_call';
          else if (customer.dpd > 25) newAgent = 'escalation';
          else newAgent = 'general_inquiry';

          const normalizedFinalResponse = analysis.final_response || null;
          const retryReasonLabel = normalizedFinalResponse || 'no_outcome';

          const updateSQL = `UPDATE customers SET 
            final_response=?, last_call_date=datetime('now'), 
            follow_up_count=?, priority_score=?,
            assigned_agent=?, assigned_tone=?,
            ptp_status=?, ptp_date=?,
            scheduled_retry_at=?, retry_reason=?,
            updated_at=datetime('now') 
          WHERE id=?`;

          log.info('TIMING', `[CUSTOMER-UPDATE] Writing to DB: final_response=${analysis.final_response}, ptp_status=${newPtpStatus}, ptp_date=${newPtpDate}, agent=${newAgent}, tone=${newTone}, retry=${scheduledRetryAt}`);
          db.prepare(updateSQL).run(
            normalizedFinalResponse,
            newFollowUp, newPriority,
            newAgent, newTone,
            newPtpStatus, newPtpDate,
            scheduledRetryAt, pendingCustomerRetryReason,
            conv.customer_id
          );

          log.success('MEDIA', `Customer updated: final=${analysis.final_response}, ptp_status=${newPtpStatus}, ptp_date=${newPtpDate}, follow_up=${newFollowUp}, priority=${newPriority.toFixed(0)}, agent=${newAgent}, tone=${newTone}`);
        }

        log.success('MEDIA', `Analysis: ${analysis.final_response} | Notes: ${analysis.notes}`);
        broadcast({ type: 'LOG', message: `Result: ${analysis.final_response}`, source: 'sys' });

        // Generate complete English call summary via Gemini
        const rawCallSummaryData = await generateCallSummary(transcript, analysis, conv).catch(() => null);
        const callSummaryData = rawCallSummaryData ? {
          ...rawCallSummaryData,
          summary: sanitizeLogText(rawCallSummaryData.summary || '') || null,
          agent_summary: sanitizeLogText(rawCallSummaryData.agent_summary || '') || null,
          customer_summary: sanitizeLogText(rawCallSummaryData.customer_summary || '') || null,
          key_customer_statement: sanitizeLogText(rawCallSummaryData.key_customer_statement || '') || null,
          next_action: sanitizeLogText(rawCallSummaryData.next_action || '') || null,
          customer_response: sanitizeLogText(rawCallSummaryData.customer_response || '') || null,
          call_quality: sanitizeLogText(rawCallSummaryData.call_quality || '') || null,
        } : null;
        const persistedTranscript = sanitizeLogText(String(transcript || '').trim()) || callSummaryData?.summary || null;

        let targetLogId = activeConversationId ? CONVERSATION_CALL_LOG_MAP.get(activeConversationId) || null : null;
        let existingLogRow = null;

        if (targetLogId) {
          existingLogRow = db.prepare('SELECT id FROM call_logs WHERE id = ?').get(targetLogId);
          if (!existingLogRow) targetLogId = null;
        }

        if (!targetLogId && conv?.customer_id) {
          existingLogRow = db.prepare(
            `SELECT id
             FROM call_logs
             WHERE customer_id = ?
             ORDER BY created_at DESC
             LIMIT 1`
          ).get(conv.customer_id);
          targetLogId = existingLogRow?.id || null;
        }

        if (targetLogId) {
          db.prepare(`UPDATE call_logs SET
            customer_name = ?,
            agent_type = ?,
            tone = ?,
            status = ?,
            duration = ?,
            outcome = ?,
            ptp_date = ?,
            notes = ?,
            transcript = ?,
            recording_url = COALESCE(?, recording_url)
          WHERE id = ?`).run(
            conv?.customer_name || null,
            conv?.agent_type || null,
            conv?.tone || null,
            'completed',
            duration || 0,
            analysis.final_response || null,
            analysis.ptp_date || null,
            pendingCallLogNotes,
            persistedTranscript,
            recordingUrl,
            targetLogId
          );
        } else if (conv?.customer_id) {
          targetLogId = uuidv4();
          db.prepare(`INSERT INTO call_logs (id, customer_id, customer_name, agent_type, tone, status, duration, outcome, ptp_date, notes, transcript, recording_url) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`)
            .run(
              targetLogId,
              conv.customer_id,
              conv.customer_name,
              conv.agent_type,
              conv.tone,
              'completed',
              duration || 0,
              analysis.final_response || null,
              analysis.ptp_date || null,
              pendingCallLogNotes,
              persistedTranscript,
              recordingUrl
            );
        }

        if (activeConversationId && targetLogId) {
          CONVERSATION_CALL_LOG_MAP.set(activeConversationId, targetLogId);
        }

        // Broadcast structured call result with all variables
        const callResultPayload = {
          type: 'CALL_RESULT',
          // Core outcome
          outcome: analysis.final_response || null,
          ptp_date: analysis.ptp_date || null,
          ptp_status: analysis.ptp_status || null,
          // Summary fields
          summary: callSummaryData?.summary || analysis.notes || null,
          agent_summary: callSummaryData?.agent_summary || null,
          customer_summary: callSummaryData?.customer_summary || null,
          key_customer_statement: callSummaryData?.key_customer_statement || null,
          next_action: callSummaryData?.next_action || null,
          customer_response: callSummaryData?.customer_response || null,
          call_quality: callSummaryData?.call_quality || null,
          recording_prediction_pending: recordingPredictionPending,
          finalization_source: finalizationSource,
          // Metadata
          customer_id: conv?.customer_id || null,
          customer_name: conv?.customer_name || null,
          call_log_id: targetLogId || null,
          duration_seconds: duration || 0,
          call_date: new Date().toISOString(),
          semantic_winner: analysis.semantic_winner || null,
          retry_in_hours: analysis.schedule_retry_hours || null,
          prediction_comparison: {
            current_used: currentUsedPrediction,
            semantic_logic: semanticLogicPrediction,
            recording_based: recordingBasedPrediction,
          },
          current_used_prediction: currentUsedPrediction,
          semantic_logic_prediction: semanticLogicPrediction,
          recording_based_prediction: recordingBasedPrediction,
        };
        broadcast(callResultPayload);
        log.info('TIMING', `[CALL_RESULT] outcome=${callResultPayload.outcome} | ptp_date=${callResultPayload.ptp_date || 'none'} | quality=${callResultPayload.call_quality} | customer=${callResultPayload.customer_name} | duration=${callResultPayload.duration_seconds}s`);
        log.info('TIMING', `[CALL_RESULT-AGENT] ${callResultPayload.agent_summary || 'none'}`);
        log.info('TIMING', `[CALL_RESULT-CUSTOMER] ${callResultPayload.customer_summary || 'none'}`);
        log.info('TIMING', `[CALL_RESULT-SUMMARY] ${callResultPayload.summary}`);
        log.info('TIMING', `[CALL_RESULT-NEXT] ${callResultPayload.next_action || 'none'}`);

        // ---- Full conversation transcript at end of call ----
        log.info('TIMING', `[CONVERSATION] ===== FULL TRANSCRIPT (${messages.length} turns) =====`);
        let _turnNum = 0;
        for (const _m of messages) {
          _turnNum++;
          const _speaker = _m.role === 'agent' ? (conv?.caller_name || 'Agent') : 'Customer';
          log.info('TIMING', `[CONVERSATION] [${String(_turnNum).padStart(2,'0')}] ${_speaker}: ${_m.text}`);
        }
        log.info('TIMING', `[CONVERSATION] ===== CUSTOMER SAID (${what_customer_said.length} turns) =====`);
        if (what_customer_said.length === 0) {
          log.info('TIMING', '[CONVERSATION] (no customer speech captured)');
        } else {
          what_customer_said.forEach((_t, _i) => log.info('TIMING', `[CUSTOMER-${String(_i+1).padStart(2,'0')}] ${_t}`));
        }
        log.info('TIMING', `[CONVERSATION] ===== AGENT SAID (${what_agent_said.length} turns) =====`);
        if (what_agent_said.length === 0) {
          log.info('TIMING', '[CONVERSATION] (no agent speech captured)');
        } else {
          what_agent_said.forEach((_t, _i) => log.info('TIMING', `[AGENT-${String(_i+1).padStart(2,'0')}] ${_t}`));
        }
        log.info('TIMING', '[CONVERSATION] ===== END =====');

        // Recording recovery: if Twilio callback is delayed/missed, keep retrying from Twilio logs.
        try {
          const accountSidFetch = (process.env.TWILIO_ACCOUNT_SID || '').replace(/['\"]/g, '').trim();
          const authTokenFetch = (process.env.TWILIO_AUTH_TOKEN || '').replace(/['\"]/g, '').trim();
          const canRecoverRecording = Boolean(
            !recordingUrl &&
            accountSidFetch &&
            authTokenFetch &&
            callSid &&
            callSid !== 'pending' &&
            !accountSidFetch.startsWith('AC_test') &&
            targetLogId
          );

          if (canRecoverRecording) {
            const maxAttempts = 8; // ~5.5 minutes total with growing delays
            const delayByAttemptMs = [0, 10000, 20000, 30000, 45000, 60000, 80000, 90000];

            const runRecordingRecovery = async (attempt = 0) => {
              try {
                const recListRes = await fetch(`https://api.twilio.com/2010-04-01/Accounts/${accountSidFetch}/Calls/${callSid}/Recordings.json`, {
                  headers: { 'Authorization': 'Basic ' + Buffer.from(`${accountSidFetch}:${authTokenFetch}`).toString('base64') },
                });

                if (!recListRes.ok) {
                  log.warn('RECORDING', `[RECOVERY] Twilio recordings list failed (attempt ${attempt + 1}/${maxAttempts}): ${recListRes.status}`);
                } else {
                  const recList = await recListRes.json();
                  const rec = recList?.recordings?.[0];
                  if (rec?.sid) {
                    const audioUrl = `https://api.twilio.com/2010-04-01/Accounts/${accountSidFetch}/Recordings/${rec.sid}.mp3`;
                    const audioRes = await fetch(audioUrl, {
                      headers: { 'Authorization': 'Basic ' + Buffer.from(`${accountSidFetch}:${authTokenFetch}`).toString('base64') },
                    });

                    if (audioRes.ok) {
                      const audioBuffer = Buffer.from(await audioRes.arrayBuffer());
                      const fileName = `twilio-${rec.sid}.mp3`;
                      writeFileSync(path.join(RECORDINGS_DIR, fileName), audioBuffer);

                      let publicUrl = `/api/recordings/${fileName}`;
                      const supabaseUrl = process.env.SUPABASE_URL || process.env.VITE_SUPABASE_URL;
                      const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY || process.env.VITE_SUPABASE_PUBLISHABLE_KEY;
                      if (supabaseUrl && supabaseKey) {
                        try {
                          const uploadRes = await fetch(`${supabaseUrl}/storage/v1/object/call_recordings/${fileName}`, {
                            method: 'POST',
                            headers: {
                              'Authorization': `Bearer ${supabaseKey}`,
                              'Content-Type': 'audio/mpeg',
                              'x-upsert': 'true',
                            },
                            body: audioBuffer,
                          });
                          if (uploadRes.ok) {
                            publicUrl = `${supabaseUrl}/storage/v1/object/public/call_recordings/${fileName}`;
                          }
                        } catch (uerr) {
                          log.warn('RECORDING', `[RECOVERY] Cloud upload error: ${uerr.message}`);
                        }
                      }

                      try {
                        db.prepare('UPDATE call_logs SET recording_url = ? WHERE id = ?').run(publicUrl, targetLogId);
                        broadcast({ type: 'RECORDING_READY', callSid: callSid, url: publicUrl });
                        broadcast({ type: 'LOG', message: `Recording recovered: ${fileName}`, source: 'sys' });
                        log.success('RECORDING', `[RECOVERY] Recording attached to call_log ${targetLogId} (attempt ${attempt + 1}/${maxAttempts})`);
                        return;
                      } catch (upErr) {
                        log.warn('RECORDING', `[RECOVERY] Failed to update call_log: ${upErr.message}`);
                      }
                    } else {
                      log.warn('RECORDING', `[RECOVERY] Download failed for ${rec.sid} (attempt ${attempt + 1}/${maxAttempts}): ${audioRes.status}`);
                    }
                  } else {
                    log.info('RECORDING', `[RECOVERY] No recording listed yet (attempt ${attempt + 1}/${maxAttempts}) for callSid=${callSid}`);
                  }
                }
              } catch (fetchErr) {
                log.warn('RECORDING', `[RECOVERY] Attempt ${attempt + 1}/${maxAttempts} failed: ${fetchErr.message}`);
              }

              const nextAttempt = attempt + 1;
              if (nextAttempt < maxAttempts) {
                const waitMs = delayByAttemptMs[nextAttempt] ?? 90000;
                setTimeout(() => {
                  runRecordingRecovery(nextAttempt).catch(() => {});
                }, waitMs);
              } else {
                log.warn('RECORDING', `[RECOVERY] Exhausted ${maxAttempts} attempts for callSid=${callSid} logId=${targetLogId}`);
              }
            };

            runRecordingRecovery(0).catch(() => {});
          }
        } catch (immediateErr) {
          log.warn('RECORDING', `Recording recovery scheduling failed: ${immediateErr.message}`);
        }

        // Write debug summary
        writeFileSync(debugFiles.summary, JSON.stringify({
        conversationId: activeConversationId || null,
        callSid: callSid || null,
        streamSid: streamSid || null,
        duration,
        mediaEvents: mediaEventCount,
        transcriptLines: messages.length,
        outboundChunks: outboundChunks.length,
        geminiSetupFailed,
        geminiFailureReason: geminiFailureReason || null,
        geminiSetupComplete,
        greetingRequestSent,
        audioPacketsBufferedBeforeSetup,
        endCause,
      }, null, 2), 'utf8');
      }
    } catch (err) {
      log.error('MEDIA', 'End processing error', err.message);
      // Best-effort fallback so UI and scheduler never remain stuck in ending state.
      try {
        if (activeConversationId) {
          const fallbackFinal = 'callback_requested';
          const fallbackRetryHours = 2;
          const fallbackNotes = `Graceful fallback after end-processing error: ${String(err.message || 'unknown')}`;
          const fallbackTranscript = messages
            .map(m => `${m.role === 'agent' ? (conv?.caller_name || 'Agent') : 'Customer'}: ${m.text}`)
            .join('\n');

          db.prepare(`UPDATE call_conversations SET status='completed', final_response=?, notes=?, schedule_retry_hours=?, updated_at=datetime('now') WHERE id=?`)
            .run(fallbackFinal, fallbackNotes, fallbackRetryHours, activeConversationId);

          if (conv?.customer_id) {
            const retryAt = new Date(Date.now() + fallbackRetryHours * 3600000).toISOString();
            db.prepare(`UPDATE customers SET final_response=?, last_call_date=datetime('now'), scheduled_retry_at=?, retry_reason=?, updated_at=datetime('now') WHERE id=?`)
              .run(fallbackFinal, retryAt, `${fallbackFinal} ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â ${fallbackRetryHours}h retry`, conv.customer_id);

            const fallbackLogId = uuidv4();
            db.prepare(`INSERT INTO call_logs (id, customer_id, customer_name, agent_type, tone, status, duration, outcome, notes, transcript, recording_url) VALUES (?,?,?,?,?,?,?,?,?,?,?)`)
              .run(
                fallbackLogId,
                conv.customer_id,
                conv.customer_name,
                conv.agent_type,
                conv.tone,
                'completed',
                Math.floor((Date.now() - callStartTime) / 1000),
                fallbackFinal,
                fallbackNotes,
                sanitizeLogText(fallbackTranscript) || null,
                null
              );
            if (activeConversationId) CONVERSATION_CALL_LOG_MAP.set(activeConversationId, fallbackLogId);
          }

          log.warn('MEDIA', `Applied graceful fallback completion for conversation ${activeConversationId}`);
          broadcast({ type: 'STATUS_UPDATE', status: 'COMPLETED' });
          broadcast({ type: 'LOG', message: `Graceful fallback completion applied: ${fallbackFinal}`, source: 'sys' });
        }
      } catch (fallbackErr) {
        log.error('MEDIA', `Fallback completion failed: ${fallbackErr.message}`);
      }
    } finally {
      // Ensure frontend exits analyzing state even if some branch returned early.
      broadcast({ type: 'STATUS_UPDATE', status: 'COMPLETED' });
      if (activeConversationId) {
        // Allow recording-status callback a short window to attach URL, then clean map entry.
        setTimeout(() => CONVERSATION_CALL_LOG_MAP.delete(activeConversationId), 15 * 60 * 1000);
      }
      if (activeConversationId) CALL_PREPARATION_CACHE.delete(activeConversationId);
      // Close Gemini AFTER all analysis and DB updates are done
      try {
        if (wsGemini) {
          closeGeminiSocket(wsGemini, 'post_analysis_cleanup');
          log.info('TIMING', '[END] Gemini WebSocket closed (post-analysis cleanup)');
        }
      } catch {}

      // Notify scheduler so it can immediately dial the next overdue customer (sequential mode)
      try { afterCallHook?.(); } catch {}
    }
  }
}

// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
// ACTIVE CALLS STATUS ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â GET /api/active-calls
// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
router.get('/active-calls', (req, res) => {
  res.json({
    count: getActiveCallCount(),
    calls: getActiveCalls(),
  });
});


router.post('/make-call', async (req, res) => {
  const DIAL_T0 = Date.now();
  log.divider('MAKE CALL');
  try {
    const { customerId, customerName, customerPhone, agentType, tone, balance, dpd, ptpStatus, followUpCount, callerVoice, callerLanguage, callerName, preparedScript, customerGender, callerGender, fetchTwilioRecording, systemPrompt, temperature, maxTokens, noiseCancellation } = req.body;
    log.info('TIMING', `[PREP T+0ms] Preparing call for ${customerName} at ${customerPhone}`);

    if (!customerPhone || !customerName) {
      return res.status(400).json({ error: 'customerPhone and customerName required' });
    }

    // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ RESOURCE GUARD: Prevent overloading ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
    if (ACTIVE_CALLS.size >= RESOURCE_LIMITS.MAX_ACTIVE_CALLS) {
      log.warn('CALL', `ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â RESOURCE LIMIT: ${ACTIVE_CALLS.size}/${RESOURCE_LIMITS.MAX_ACTIVE_CALLS} active calls ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â rejecting manual call for ${customerName}`);
      return res.status(429).json({ error: `Server at max capacity (${RESOURCE_LIMITS.MAX_ACTIVE_CALLS} concurrent calls). Try again shortly.` });
    }

    // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ DUPLICATE GUARD ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
    if (customerId && RESOURCE_LIMITS.IN_FLIGHT_CALLS.has(customerId)) {
      log.warn('CALL', `ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â DUPLICATE: Call already in-flight for customer ${customerId}`);
      return res.status(409).json({ error: 'A call is already being placed to this customer' });
    }
    if (customerId) RESOURCE_LIMITS.IN_FLIGHT_CALLS.add(customerId);

    const convId = uuidv4();
    const normalizedCustomerGender = normalizeGender(customerGender || 'male', 'male');
    const normalizedCallerGender = resolveCallerGender(callerGender, callerName || 'Omar', 'male', callerVoice || '');
    const voice = normalizeVoiceName(callerVoice || (normalizedCallerGender === 'male' ? 'Orus' : 'Kore'), normalizedCallerGender);
    const language = callerLanguage || 'ur-PK';
    const name = callerName || 'Omar';

    db.prepare(`INSERT INTO call_conversations (id, call_sid, customer_id, customer_name, agent_type, tone, caller_name, voice, language, balance, dpd, ptp_status, follow_up_count)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(
      convId, 'pending', customerId || '', customerName, agentType || 'fresh_call', tone || 'polite',
      name, voice, language, balance || 0, dpd || 0, ptpStatus || null, followUpCount || 0
    );

    const callContext = {
      id: convId,
      customer_id: customerId || '',
      customer_name: customerName,
      agent_type: agentType || 'fresh_call',
      tone: tone || 'polite',
      caller_name: name,
      voice,
      language,
      balance: balance || 0,
      dpd: dpd || 0,
      ptp_status: ptpStatus || null,
      follow_up_count: followUpCount || 0,
      customer_gender: normalizedCustomerGender,
      caller_gender: normalizedCallerGender,
    };

    // ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
    // PHASE 1: PREPARE EVERYTHING BEFORE DIALING (concurrent)
    // ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
    broadcast({ type: 'LOG', message: `Preparing call for ${customerName}...`, source: 'sys' });

    const accountSid = process.env.TWILIO_ACCOUNT_SID;
    const authToken = process.env.TWILIO_AUTH_TOKEN;
    const twilioPhone = process.env.TWILIO_PHONE_NUMBER;

    if (!accountSid || !authToken || !twilioPhone || accountSid.startsWith('AC_test')) {
      log.warn('CALL', 'Twilio not configured ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â MOCK MODE');
      const mockSid = 'MOCK_' + uuidv4().slice(0, 8);
      db.prepare('UPDATE call_conversations SET call_sid = ? WHERE id = ?').run(mockSid, convId);
      clearCustomerRetrySchedule(customerId, 'manual-call-start');
      return res.json({
        success: true, mock: true, callSid: mockSid, conversationId: convId,
        status: 'queued', agentType, tone, callerName: name, voice,
        message: `Mock call to ${customerName}`,
      });
    }

    const serverUrl = getServerBaseUrl();
    if (!serverUrl) {
      log.fail('CALL', 'SERVER_URL/BASE_URL not set');
      return res.status(500).json({ error: 'SERVER_URL or BASE_URL not configured.' });
    }

    // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ CONCURRENT WARMUP: Gemini WS + Greeting Audio + Assets ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
    const geminiApiKey = getGeminiApiKey();

    // 1) Start greeting asset warmup (generates TTS audio)
    const { assets: callAssets, warmPromise } = startCallAssetWarmup(callContext, {
      preparedScript,
      fetchTwilioRecording: fetchTwilioRecording !== false,
      maxPtpDays: req.body.maxPtpDays || 5,
      customSystemPrompt: systemPrompt || null,
      noiseCancellation: noiseCancellation === true,
    });
    log.info('TIMING', `[PREP T+${Date.now()-DIAL_T0}ms] Greeting warmup started`);

    // 2) Pre-connect Gemini WS AND send setup config (runs concurrently with greeting warmup)
    //    This is the key latency optimization: send setup during RINGING so
    //    Gemini can return setupComplete before the customer answers, saving post-pickup delay.
    let geminiPrewarmPromise = Promise.resolve();
    if (geminiApiKey) {
      geminiPrewarmPromise = new Promise((resolve) => {
        const geminiURL = `wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1alpha.GenerativeService.BidiGenerateContent?key=${geminiApiKey}`;
        log.info('TIMING', `[PREP T+${Date.now()-DIAL_T0}ms] Gemini WS pre-connecting + pre-setup...`);
        const preWs = new WebSocket(geminiURL);
        preWs._prewarmCreatedAt = Date.now();
        preWs._setupSent = false;
        preWs._setupComplete = false;
        let resolved = false;
        const done = () => { if (!resolved) { resolved = true; resolve(); } };
        
        preWs.on('open', () => {
          log.info('TIMING', `[PREP T+${Date.now()-DIAL_T0}ms] Gemini WS OPEN (pre-dial) - sending setup config now`);

          // Build and send setup config immediately; do not wait for handleMediaStream
          try {
            const custGenderPW = normalizedCustomerGender;
            const callerGenderPW = normalizedCallerGender;
            const voiceNamePW = voice;
            const setupConfig = buildPrewarmSetupConfig({
              convData: callContext,
              callAssets,
              voiceName: voiceNamePW,
              custGender: custGenderPW,
              callerGender: callerGenderPW,
              systemPrompt: systemPrompt || null,
              maxPtpDays: req.body.maxPtpDays || 5,
            });
            preWs.send(JSON.stringify(setupConfig));
            preWs._setupSent = true;
            log.info('TIMING', `[PREP T+${Date.now()-DIAL_T0}ms] Setup config sent during pre-dial (voice: ${voiceNamePW})`);
          } catch (setupErr) {
            log.warn('TIMING', `Pre-dial setup config failed: ${setupErr.message} - will retry in handleMediaStream`);
            preWs._setupSent = false;
          }
          done();
        });
        
        // Listen for setupComplete during ringing
        preWs.on('message', (data) => {
          try {
            const resp = JSON.parse(data.toString());
            if (resp.setupComplete || resp.setup_complete) {
              preWs._setupComplete = true;
              log.info('TIMING', `[PREP T+${Date.now()-DIAL_T0}ms] Gemini setupComplete during ringing - zero post-pickup delay`);
            }
          } catch {}
        });
        
        preWs.on('error', (err) => {
          log.warn('TIMING', `Gemini pre-warm error: ${err.message}`);
          PREWARMED_GEMINI_WS.delete(convId);
          done();
        });
        preWs.on('close', () => {
          if (PREWARMED_GEMINI_WS.get(convId) === preWs) {
            PREWARMED_GEMINI_WS.delete(convId);
          }
          done();
        });
        PREWARMED_GEMINI_WS.set(convId, preWs);
        // Safety: do not block dialing forever; cap prewarm wait at 3s.
        setTimeout(done, 3000);
        // Auto-cleanup after 45s if call never connects
        setTimeout(() => {
          if (PREWARMED_GEMINI_WS.get(convId) === preWs) {
            PREWARMED_GEMINI_WS.delete(convId);
            if (preWs.readyState === WebSocket.OPEN || preWs.readyState === WebSocket.CONNECTING) {
              preWs.close();
            }
          }
        }, 45000);
      });
    }

    // 3) Wait for BOTH to complete (or timeout) before dialing.
    //    Greeting warmup has a 2.5s timeout per attempt; Gemini WS has 3s max wait.
    //    Worst case is about 3s before dial, with all prework primed.
    await Promise.allSettled([warmPromise, geminiPrewarmPromise]);

    const preWsState = PREWARMED_GEMINI_WS.get(convId);
    const geminiReady = preWsState && preWsState.readyState === WebSocket.OPEN;
    const greetingReady = callAssets.openingGreetingPayloads?.length > 0;
    log.info('TIMING', `[PREP T+${Date.now()-DIAL_T0}ms] Pre-dial ready - Gemini: ${geminiReady ? 'OPEN' : 'pending'}, Greeting: ${greetingReady ? callAssets.openingGreetingPayloads.length + ' chunks' : 'fallback'}`);

    // ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
    // PHASE 2: DIAL TWILIO (everything is ready)
    // ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
    broadcast({ type: 'LOG', message: `Dialing ${customerPhone}...`, source: 'sys' });

    const maxPtpDate = new Date(Date.now() + 5 * 86400000).toISOString().split('T')[0];
    const custGenderVal = (customerGender || 'male').toLowerCase();
    const honorific = custGenderVal === 'female' ? 'Sahiba' : 'Sahab';

    const twimlParams = new URLSearchParams({
      conversationId: convId,
      customerId: customerId || '',
      agentName: name,
      customerName: customerName,
      honorific: honorific,
      amount: String(balance || 0),
      dueDate: maxPtpDate,
      voice: voice,
      customerGender: customerGender || 'male',
      callerGender: normalizedCallerGender,
    });

    const statusCallbackUrl = `${serverUrl}/api/call-status?conversationId=${convId}`;

    const params = new URLSearchParams({
      To: customerPhone,
      From: twilioPhone,
      Url: `${serverUrl}/api/twiml?${twimlParams.toString()}`,
      StatusCallback: statusCallbackUrl,
      StatusCallbackMethod: 'POST',
      StatusCallbackEvent: 'initiated ringing answered completed',
      MachineDetection: 'Enable',
      MachineDetectionTimeout: '3',
      AsyncAmd: 'true',
      AsyncAmdStatusCallback: statusCallbackUrl,
      AsyncAmdStatusCallbackMethod: 'POST',
    });

    params.set('Record', 'true');
    params.set('RecordingChannels', 'dual');
    params.set('RecordingTrack', 'both');
    params.set('RecordingStatusCallback', `${serverUrl}/api/recording-status?conversationId=${encodeURIComponent(convId)}`);
    params.set('RecordingStatusCallbackEvent', 'completed');

    log.info('TIMING', `[DIAL T+${Date.now()-DIAL_T0}ms] Sending to Twilio API...`);

    // Timeout-protected Twilio API call.
    const dialController = new AbortController();
    const dialTimeout = setTimeout(() => dialController.abort(), RESOURCE_LIMITS.TWILIO_API_TIMEOUT_MS);

    let twilioRes;
    try {
      twilioRes = await fetch(`https://api.twilio.com/2010-04-01/Accounts/${accountSid}/Calls.json`, {
        method: 'POST',
        headers: {
          'Authorization': 'Basic ' + Buffer.from(`${accountSid}:${authToken}`).toString('base64'),
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        body: params.toString(),
        signal: dialController.signal,
      });
    } catch (fetchErr) {
      clearTimeout(dialTimeout);
      db.prepare('DELETE FROM call_conversations WHERE id = ?').run(convId);
      cleanupPrewarmed(convId);
      if (customerId) RESOURCE_LIMITS.IN_FLIGHT_CALLS.delete(customerId);
      const isTimeout = fetchErr.name === 'AbortError';
      log.error('CALL', `Twilio API ${isTimeout ? 'TIMEOUT' : 'FETCH ERROR'}: ${fetchErr.message}`);
      return res.status(504).json({ error: `Twilio API ${isTimeout ? 'timed out' : 'unreachable'}. Check your network connection.` });
    }
    clearTimeout(dialTimeout);

    const twilioData = await twilioRes.json();
    if (!twilioRes.ok) {
      log.fail('CALL', `Twilio error (${twilioRes.status})`, twilioData);
      db.prepare('DELETE FROM call_conversations WHERE id = ?').run(convId);
      cleanupPrewarmed(convId);
      if (customerId) RESOURCE_LIMITS.IN_FLIGHT_CALLS.delete(customerId);
      return res.status(twilioRes.status).json({ error: 'Twilio call failed', details: twilioData });
    }

    db.prepare('UPDATE call_conversations SET call_sid = ? WHERE id = ?').run(twilioData.sid, convId);
    clearCustomerRetrySchedule(customerId, 'manual-call-start');
    if (customerId) RESOURCE_LIMITS.IN_FLIGHT_CALLS.delete(customerId);
    log.info('TIMING', `[DIAL T+${Date.now()-DIAL_T0}ms] Twilio accepted - call SID: ${twilioData.sid} - RINGING (active: ${ACTIVE_CALLS.size})`);
    log.info('STATUS', `[CALLBACK-CONFIG] sid=${twilioData.sid} statusCallback=${statusCallbackUrl} events=initiated,ringing,answered,completed`);

    const tryFinalizeFromTwilioStatus = async (reason) => {
      try {
        const latest = db.prepare('SELECT id, status, final_response, customer_id, customer_name, agent_type, tone, call_sid FROM call_conversations WHERE id = ?').get(convId);
        if (!latest) return;
        const latestStatus = String(latest.status || '').toLowerCase();
        if (ACTIVE_CALLS.has(convId)) return;
        if (!['pending', 'queued', 'initiated', 'ringing', 'active', 'in-progress', 'in_progress', ''].includes(latestStatus)) {
          log.info('STATUS', `[STATUS-POLL-SKIP] conv=${convId.slice(0, 8)} sid=${twilioData.sid} dbStatus=${latestStatus || 'empty'} reason=${reason}`);
          return;
        }
        if (latest.call_sid && latest.call_sid !== twilioData.sid) return;

        let polledStatus = '';
        let polledDurationSec = 0;
        if (accountSid && authToken && twilioData.sid && !String(twilioData.sid).startsWith('MOCK_')) {
          try {
            const controller = new AbortController();
            const timeout = setTimeout(() => controller.abort(), 5000);
            const twilioRes = await fetch(`https://api.twilio.com/2010-04-01/Accounts/${accountSid}/Calls/${twilioData.sid}.json`, {
              method: 'GET',
              headers: {
                'Authorization': 'Basic ' + Buffer.from(`${accountSid}:${authToken}`).toString('base64'),
              },
              signal: controller.signal,
            });
            clearTimeout(timeout);
            if (twilioRes.ok) {
              const body = await twilioRes.json();
              polledStatus = String(body?.status || '').toLowerCase();
              polledDurationSec = Number(body?.duration || 0) || 0;
              logTwilioLearnedStatus({
                source: `poll:${reason}`,
                conversationId: convId,
                callSid: twilioData.sid,
                status: polledStatus,
                duration: polledDurationSec,
              });
              log.info('STATUS', `[STATUS-POLL] conv=${convId.slice(0, 8)} sid=${twilioData.sid} status=${polledStatus || 'unknown'} duration=${polledDurationSec}s reason=${reason}`);
            } else {
              const bodyText = await twilioRes.text().catch(() => '');
              log.warn('STATUS', `[STATUS-POLL] conv=${convId.slice(0, 8)} sid=${twilioData.sid} non-ok=${twilioRes.status} reason=${reason} body=${String(bodyText || '').slice(0, 240)}`);
            }
          } catch (pollErr) {
            log.warn('STATUS', `Twilio status poll failed for ${convId}: ${pollErr.message}`);
          }
        }

        const terminalByPoll = ['no-answer', 'busy', 'failed', 'canceled', 'cancelled', 'completed'].includes(polledStatus);
        const callLikelyAnswered = ['active', 'in-progress', 'in_progress', 'ending', 'completed'].includes(latestStatus);
        // Call is still active - never finalize
        if (polledStatus === 'in-progress') {
          log.info('STATUS', `[STATUS-POLL] conv=${convId.slice(0, 8)} still in-progress - skip finalize`);
          return;
        }
        if (!terminalByPoll && reason !== 'watchdog-timeout') {
          log.info('STATUS', `[STATUS-POLL] conv=${convId.slice(0, 8)} non-terminal status (${polledStatus || 'unknown'}) at ${reason} - waiting`);
          return;
        }
        if (reason === 'watchdog-timeout' && !terminalByPoll && callLikelyAnswered) {
          log.warn('STATUS', `[STATUS-POLL] conv=${convId.slice(0, 8)} watchdog skipped no-answer fallback because call already progressed (dbStatus=${latestStatus || 'unknown'})`);
          return;
        }
        // Answered call that ended normally: mark as 'ending' and let analysis/safety-timeout handle it
        if (terminalByPoll && polledStatus === 'completed' && (polledDurationSec > 0 || callLikelyAnswered)) {
          const cCheck = db.prepare('SELECT status FROM call_conversations WHERE id=?').get(convId);
          if (cCheck && !['ending', 'completed'].includes(String(cCheck.status || '').toLowerCase())) {
            db.prepare("UPDATE call_conversations SET status='ending', updated_at=datetime('now') WHERE id=?").run(convId);
            log.info('STATUS', `[STATUS-POLL] conv=${convId.slice(0, 8)} answered then ended - marked as ending for analysis`);
            emitStatusUpdate({
              conversationId: convId,
              callSid: twilioData.sid,
              status: 'ENDING',
              duration: polledDurationSec,
            });
          }
          return;
        }
        // Map final statuses: canceled/cancelled and zero-duration completed become no-answer.
        // Twilio failed usually means unreachable/switched-off endpoint.
        const effectiveStatus = terminalByPoll
          ? (['canceled', 'cancelled'].includes(polledStatus) ? 'no-answer' : polledStatus === 'completed' ? 'no-answer' : polledStatus)
          : 'no-answer';
        const response = effectiveStatus === 'busy'
          ? 'busy'
          : effectiveStatus === 'failed'
          ? 'switched_off'
          : 'no_answer';
        const terminalReason = response === 'switched_off'
          ? 'unreachable_or_switched_off'
          : effectiveStatus === 'busy'
          ? 'line_busy'
          : reason === 'watchdog-timeout'
          ? 'no_pickup_watchdog'
          : 'no_pickup';
        const retryHours = 2;
        const fallbackNote = terminalByPoll
          ? `No webhook callback; finalized from Twilio status poll (${effectiveStatus})`
          : 'No pickup detected (watchdog fallback)';

        db.prepare("UPDATE call_conversations SET status=?, final_response=?, notes=COALESCE(notes, ?), schedule_retry_hours=COALESCE(schedule_retry_hours, ?), updated_at=datetime('now') WHERE id=?")
          .run(effectiveStatus || 'no-answer', response, fallbackNote, retryHours, convId);

        if (latest.customer_id) {
          const retryAt = new Date(Date.now() + retryHours * 3600000).toISOString();
          db.prepare("UPDATE customers SET final_response=?, last_call_date=datetime('now'), follow_up_count=follow_up_count+1, scheduled_retry_at=?, retry_reason=?, updated_at=datetime('now') WHERE id=?")
            .run(response, retryAt, response, latest.customer_id);

          const logId = uuidv4();
          db.prepare('INSERT INTO call_logs (id, customer_id, customer_name, agent_type, tone, status, duration, outcome, notes) VALUES (?,?,?,?,?,?,?,?,?)')
            .run(logId, latest.customer_id, latest.customer_name, latest.agent_type, latest.tone, effectiveStatus || 'no-answer', polledDurationSec || 0, response, fallbackNote);
        }

        log.warn('STATUS', `[NO-PICKUP-FINALIZE] ${latest.customer_name || convId} finalized=${effectiveStatus || 'no-answer'} response=${response} reason=${reason}`);
        emitStatusUpdate({
          conversationId: convId,
          callSid: twilioData.sid,
          status: String(effectiveStatus || 'no-answer').toUpperCase().replace('-', '_'),
          finalResponse: response,
          terminalReason,
          duration: polledDurationSec,
        });
        broadcast({ type: 'LOG', message: `No-pickup finalize: ${effectiveStatus || 'no-answer'} (${reason})`, source: 'sys' });
      } catch (watchErr) {
        log.warn('STATUS', `No-pickup fallback failed for ${convId}: ${watchErr.message}`);
      }
    };

    // Extra visibility/robustness: poll Twilio status for a short window and log transitions.
    // This helps diagnose missing callbacks and still finalize terminal no-answer/cancel flows quickly.
    let statusMonitorTimer = null;
    let statusMonitorChecks = 0;
    let lastMonitoredStatus = '';

    const stopStatusMonitor = () => {
      if (statusMonitorTimer) {
        clearInterval(statusMonitorTimer);
        statusMonitorTimer = null;
      }
    };

    const monitorTwilioStatus = async () => {
      try {
        statusMonitorChecks += 1;
        if (statusMonitorChecks > 24) {
          stopStatusMonitor();
          return;
        }
        if (ACTIVE_CALLS.has(convId)) {
          stopStatusMonitor();
          return;
        }
        if (!accountSid || !authToken || !twilioData.sid || String(twilioData.sid).startsWith('MOCK_')) {
          stopStatusMonitor();
          return;
        }

        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 5000);
        const twilioRes = await fetch(`https://api.twilio.com/2010-04-01/Accounts/${accountSid}/Calls/${twilioData.sid}.json`, {
          method: 'GET',
          headers: {
            'Authorization': 'Basic ' + Buffer.from(`${accountSid}:${authToken}`).toString('base64'),
          },
          signal: controller.signal,
        });
        clearTimeout(timeout);

        if (!twilioRes.ok) {
          const bodyText = await twilioRes.text().catch(() => '');
          log.warn('STATUS', `[STATUS-MONITOR] conv=${convId.slice(0, 8)} sid=${twilioData.sid} non-ok=${twilioRes.status} check=${statusMonitorChecks} body=${String(bodyText || '').slice(0, 180)}`);
          return;
        }

        const body = await twilioRes.json();
        const polledStatus = String(body?.status || '').toLowerCase();
        const polledDurationSec = Number(body?.duration || 0) || 0;

        logTwilioLearnedStatus({
          source: 'monitor',
          conversationId: convId,
          callSid: twilioData.sid,
          status: polledStatus,
          duration: polledDurationSec,
          note: polledStatus && polledStatus === lastMonitoredStatus ? 'same' : 'changed',
        });

        if (polledStatus && polledStatus !== lastMonitoredStatus) {
          lastMonitoredStatus = polledStatus;
          log.info('STATUS', `[STATUS-MONITOR] conv=${convId.slice(0, 8)} sid=${twilioData.sid} status=${polledStatus} duration=${polledDurationSec}s check=${statusMonitorChecks}`);
          broadcast({ type: 'LOG', message: `Twilio polled status: ${polledStatus}`, source: 'twil' });
        }

        if (['no-answer', 'busy', 'failed', 'canceled', 'cancelled', 'completed'].includes(polledStatus)) {
          await tryFinalizeFromTwilioStatus('status-monitor');
          stopStatusMonitor();
        }
      } catch (pollErr) {
        log.warn('STATUS', `[STATUS-MONITOR] conv=${convId.slice(0, 8)} sid=${twilioData.sid} error=${pollErr.message}`);
      }
    };

    setTimeout(() => { void tryFinalizeFromTwilioStatus('early-poll'); }, RESOURCE_LIMITS.TWILIO_STATUS_POLL_MS);
    setTimeout(() => { void tryFinalizeFromTwilioStatus('mid-poll'); }, 20000);
    setTimeout(() => { void tryFinalizeFromTwilioStatus('watchdog-timeout'); }, RESOURCE_LIMITS.NO_PICKUP_FALLBACK_MS);
    setTimeout(() => { void monitorTwilioStatus(); }, 3000);
    statusMonitorTimer = setInterval(() => { void monitorTwilioStatus(); }, 5000);
    setTimeout(() => stopStatusMonitor(), 120000);

    res.json({
      success: true, callSid: twilioData.sid, conversationId: convId,
      status: twilioData.status, agentType, tone, callerName: name, voice,
      mode: 'native-audio-media-streams',
    });
  } catch (err) {
    if (customerId) RESOURCE_LIMITS.IN_FLIGHT_CALLS.delete(customerId);
    log.error('CALL', 'make-call exception', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
// TWIML ENDPOINT ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â Bridge pattern (Twilio fetches this URL)
// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â

router.post('/twiml', (req, res) => {
  log.info('TWIML', 'TwiML endpoint called');
  broadcast({ type: 'LOG', message: 'TWILIO: TwiML endpoint called', source: 'twil' });

  const requestBaseUrl = getRequestBaseUrl(req);
  const mediaStreamBaseUrl = buildTwilioStreamBaseUrl(req);
  const conversationId = String(req.query.conversationId || '');
  const mediaStreamUrl = `${mediaStreamBaseUrl}/media-stream${conversationId ? `?conversationId=${encodeURIComponent(conversationId)}` : ''}`;
  const streamStatusUrl = `${requestBaseUrl}/api/stream-status${conversationId ? `?conversationId=${encodeURIComponent(conversationId)}` : ''}`;

  if (!/^wss?:\/\//i.test(mediaStreamUrl)) {
    log.fail('TWIML', 'Invalid Media Stream URL', {
      mediaStreamUrl,
      conversationId: conversationId || null,
    });
    broadcast({ type: 'LOG', message: 'STREAM ERROR: invalid media-stream URL ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â check SERVER_URL config', source: 'system' });
    return res.status(500).type('text/xml').send('<?xml version="1.0" encoding="UTF-8"?><Response><Say>System configuration error.</Say></Response>');
  }

  // Build TwiML with <Stream> and <Parameter> tags (bridge pattern)
  const callAssets = conversationId ? CALL_PREPARATION_CACHE.get(conversationId) || null : null;
    const effectiveCallerGender = resolveCallerGender(
      String(req.query.callerGender || callAssets?.caller_gender || ''),
      String(req.query.agentName || callAssets?.caller_name || 'Omar'),
      'male',
      String(req.query.voice || callAssets?.voice || '')
    );
    const effectiveVoice = normalizeVoiceName(String(req.query.voice || callAssets?.voice || 'Kore'), effectiveCallerGender);
  const openingGreeting = sanitizePreparedText(callAssets?.openingGreeting) || buildExactGreeting({
    customer_name: String(req.query.customerName || callAssets?.customer_name || 'Customer'),
    caller_name: String(req.query.agentName || callAssets?.caller_name || 'Omar'),
      customer_gender: normalizeGender(String(req.query.customerGender || callAssets?.customer_gender || 'male'), 'male'),
      caller_gender: effectiveCallerGender,
  });
  // Greeting should always be ready now (generated before dial), but check just in case
  const { payloadsToPlay: readyGreetingPayloads, greetingSource: readyGreetingSource } = resolveGreetingPayloads(callAssets, effectiveVoice, { allowToneFallback: false });
  const greetingReady = readyGreetingPayloads.length > 0;

  const streamMetadata = {
    conversationId: String(req.query.conversationId || ''),
    customerId: String(req.query.customerId || ''),
    agentName: String(req.query.agentName || ''),
    customerName: String(req.query.customerName || ''),
    honorific: String(req.query.honorific || ''),
    amount: String(req.query.amount || ''),
    dueDate: String(req.query.dueDate || ''),
    voice: effectiveVoice,
    customerGender: String(req.query.customerGender || ''),
    callerGender: effectiveCallerGender,
    openingDelivered: '', // Reserved for future use
  };
  let streamParams = '';
  for (const [key, val] of Object.entries(streamMetadata)) {
    if (val) {
      streamParams += `<Parameter name="${escapeXml(key)}" value="${escapeXml(val)}" />`;
    }
  }

  const twiml = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="${escapeXml(mediaStreamUrl)}" name="call-media" statusCallback="${escapeXml(streamStatusUrl)}" statusCallbackMethod="POST">${streamParams}</Stream>
  </Connect>
</Response>`;

  log.info('TWIML', 'TwiML Response generated', {
    requestBaseUrl,
    mediaStreamUrl,
    streamStatusUrl,
    conversationId: conversationId || null,
    greetingReady,
    greetingSource: readyGreetingSource,
  });
  res.type('text/xml');
  res.send(twiml);
});

// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
// STREAM STATUS ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â POST /api/stream-status (Twilio Media Streams callback)
// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â

router.post('/stream-status', (req, res) => {
  const conversationId = String(req.query.conversationId || '');
  const streamEvent = req.body.StreamEvent || 'unknown';
  const streamError = req.body.StreamError || null;
  const streamSid = req.body.StreamSid || null;
  const callSid = req.body.CallSid || null;

  log.info('STREAM', `Twilio stream ${streamEvent}`, {
    conversationId: conversationId || null,
    streamSid,
    callSid,
    streamError,
  });

  broadcast({
    type: 'LOG',
    message: streamError ? `TWILIO STREAM ${streamEvent}: ${streamError}` : `TWILIO STREAM ${streamEvent}`,
    source: 'twil',
  });

  if (conversationId && streamEvent === 'stream-error') {
    const existing = db.prepare('SELECT notes FROM call_conversations WHERE id = ?').get(conversationId);
    const nextNotes = [existing?.notes, `Twilio stream error: ${streamError || 'unknown error'}`].filter(Boolean).join(' | ');
    db.prepare("UPDATE call_conversations SET notes=?, updated_at=datetime('now') WHERE id=?").run(nextNotes, conversationId);
  }

  res.sendStatus(200);
});

// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
// RECORDING STATUS ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â POST /api/recording-status (Twilio callback)
// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â

router.post('/recording-status', async (req, res) => {
  const { RecordingSid, RecordingUrl, RecordingStatus, CallSid, RecordingDuration } = req.body;
  const callbackConversationId = String(req.query.conversationId || '');
  if (RecordingStatus === 'completed' && RecordingSid) {
    log.info('RECORDING', `Twilio recording completed: ${RecordingSid} for CallSid=${CallSid} (${RecordingDuration}s)`);
    broadcast({ type: 'LOG', message: `Recording available: ${RecordingSid}`, source: 'sys' });

    try {
      const accountSid = (process.env.TWILIO_ACCOUNT_SID || '').replace(/['"]/g, '').trim();
      const authToken = (process.env.TWILIO_AUTH_TOKEN || '').replace(/['"]/g, '').trim();
      const supabaseUrl = process.env.SUPABASE_URL || process.env.VITE_SUPABASE_URL;
      const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY || process.env.VITE_SUPABASE_PUBLISHABLE_KEY;

      if (!accountSid || !authToken) {
        log.warn('RECORDING', 'Missing Twilio credentials ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â cannot download recording');
        return res.sendStatus(200);
      }

      const downloaded = await fetchTwilioRecordingAudio({ accountSid, authToken, recordingSid: RecordingSid });
      if (!downloaded) {
        log.warn('RECORDING', `Twilio download failed for recording sid ${RecordingSid}`);
        return res.sendStatus(200);
      }

      const audioBuffer = downloaded.buffer;
      const fileName = `twilio-${RecordingSid}.${downloaded.ext}`;
      log.info('RECORDING', `Downloaded ${(audioBuffer.length / 1024).toFixed(1)}KB`);

      // Save locally always
      writeFileSync(path.join(RECORDINGS_DIR, fileName), audioBuffer);

      let publicUrl = `/api/recordings/${fileName}`; // local fallback

      // Upload to cloud storage if available
      if (supabaseUrl && supabaseKey) {
        const uploadRes = await fetch(`${supabaseUrl}/storage/v1/object/call_recordings/${fileName}`, {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${supabaseKey}`,
            'Content-Type': downloaded.mimeType,
            'x-upsert': 'true',
          },
          body: audioBuffer,
        });

        if (uploadRes.ok) {
          publicUrl = `${supabaseUrl}/storage/v1/object/public/call_recordings/${fileName}`;
          log.success('RECORDING', `Uploaded to cloud: ${publicUrl}`);
        } else {
          log.warn('RECORDING', `Cloud upload failed (${uploadRes.status}) ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â using local path`);
        }
      }

      // Prefer callback conversationId mapping; fallback to CallSid mapping.
      let conv = null;
      if (callbackConversationId) {
        conv = db.prepare('SELECT id, customer_id, customer_name, caller_name, call_sid FROM call_conversations WHERE id = ?').get(callbackConversationId);
      }
      if (!conv && CallSid) {
        conv = db.prepare('SELECT id, customer_id, customer_name, caller_name, call_sid FROM call_conversations WHERE call_sid = ?').get(CallSid);
      }

      let targetLogId = null;
      if (conv) {
        const mappedLogId = CONVERSATION_CALL_LOG_MAP.get(conv.id);
        if (mappedLogId) {
          try {
            db.prepare('UPDATE call_logs SET recording_url = ? WHERE id = ?').run(publicUrl, mappedLogId);
            log.success('RECORDING', `Updated mapped call_log ${mappedLogId} for conv=${conv.id}`);
            targetLogId = mappedLogId;
          } catch (mapErr) {
            log.warn('RECORDING', `Mapped call_log update failed for conv=${conv.id}: ${mapErr.message}`);
          }
        }

        if (CallSid && (!conv.call_sid || conv.call_sid === 'pending')) {
          try {
            db.prepare('UPDATE call_conversations SET call_sid = ? WHERE id = ?').run(CallSid, conv.id);
          } catch {}
        }

        let updated;
        try {
          updated = db.prepare(
            `UPDATE call_logs SET recording_url = ? WHERE id = (
              SELECT id FROM call_logs
              WHERE customer_id = ? AND (recording_url IS NULL OR recording_url = '')
              ORDER BY call_date_time DESC, created_at DESC
              LIMIT 1
            )`
          ).run(publicUrl, conv.customer_id);
        } catch (e) {
          log.warn('RECORDING', `Primary update failed: ${e.message}`);
          updated = { changes: 0 };
        }

        if (!updated || !updated.changes) {
          try {
            db.prepare(
              `UPDATE call_logs SET recording_url = ? WHERE id = (
                SELECT id FROM call_logs WHERE customer_id = ? ORDER BY call_date_time DESC, created_at DESC LIMIT 1
              )`
            ).run(publicUrl, conv.customer_id);
          } catch (e2) {
            log.warn('RECORDING', `Fallback update failed: ${e2.message}`);
          }
        }

        if (!targetLogId) {
          const latestLog = db.prepare(
            `SELECT id, transcript FROM call_logs WHERE customer_id = ? ORDER BY call_date_time DESC, created_at DESC LIMIT 1`
          ).get(conv.customer_id);
          targetLogId = latestLog?.id || null;
        }

        if (targetLogId) {
          const pendingLogRow = db.prepare('SELECT notes FROM call_logs WHERE id = ?').get(targetLogId);
          const shouldForceTranscriptRefresh = hasRecordingPendingMarker(pendingLogRow?.notes || '');
          const ensureResult = await ensureCallLogTranscriptFromRecording({
            callLogId: targetLogId,
            audioBuffer,
            mimeType: downloaded.mimeType,
            customerName: conv.customer_name || 'Customer',
            callerName: conv.caller_name || 'Agent',
            force: shouldForceTranscriptRefresh,
          });
          if (!ensureResult?.updated && !['already_present', 'missing_input'].includes(String(ensureResult?.reason || ''))) {
            await finalizePendingRecordingFallback({
              callLogId: targetLogId,
              reason: ensureResult?.reason || 'recording_analysis_unavailable',
            });
          }
        }

        log.success('RECORDING', `Updated call_log for customer ${conv.customer_id} (conv=${conv.id}) ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ ${publicUrl}`);
      } else {
        log.warn('RECORDING', `No conversation found for recording callback (convId=${callbackConversationId || 'n/a'}, CallSid=${CallSid || 'n/a'})`);
      }

      broadcast({ type: 'RECORDING_READY', callSid: CallSid, url: publicUrl });
      broadcast({ type: 'LOG', message: `Recording saved: ${fileName}`, source: 'sys' });
    } catch (err) {
      log.error('RECORDING', `Recording fetch error: ${err.message}`);
    }
  }
  res.sendStatus(200);
});

// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
// CALL STATUS ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â POST /api/call-status (Twilio status callbacks)
// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â

router.post('/call-status', (req, res) => {
  const conversationId = req.query.conversationId || '';
  const callSid = String(req.body.CallSid || '').trim();
  const resolvedConv = getConversationByIdOrCallSid(conversationId || callSid);
  const resolvedConversationId = resolvedConv?.id || conversationId || '';
  const callStatus = req.body.CallStatus || '';
  const normalizedCallStatus = String(callStatus || '').toLowerCase();
  const callDurationSec = Number(req.body.CallDuration || req.body.Duration || 0) || 0;
  const sipResponseCode = String(req.body.SipResponseCode || '').trim();
  const answeredBy = req.body.AnsweredBy || '';
  const isLikelySwitchedOff =
    normalizedCallStatus === 'failed' &&
    ['480', '408', '404', '410', '503'].includes(sipResponseCode);
  logTwilioLearnedStatus({
    source: 'callback',
    conversationId: resolvedConversationId,
    callSid,
    status: normalizedCallStatus || callStatus,
    duration: callDurationSec,
    sipResponseCode,
    answeredBy,
  });
  log.info('STATUS', `${callStatus} for ${resolvedConversationId?.slice(0, 8)}`, {
    callSid: callSid || null,
    answeredBy,
    callDuration: callDurationSec || null,
    sipResponseCode: sipResponseCode || null,
    resolvedBy: resolvedConv ? (resolvedConv.id === conversationId ? 'conversationId' : 'callSid') : 'none',
  });
  broadcast({ type: 'LOG', message: `Call status: ${callStatus}${answeredBy ? ` (${answeredBy})` : ''}`, source: 'twil' });

  // Handle ringing - customer's phone is ringing; update DB so UI can show ringing state
  if (normalizedCallStatus === 'ringing' && resolvedConversationId) {
    const rConv = db.prepare('SELECT status FROM call_conversations WHERE id=?').get(resolvedConversationId);
    if (rConv && !['in-progress', 'ending', 'completed', 'no-answer', 'busy', 'failed', 'canceled', 'cancelled'].includes(String(rConv.status || '').toLowerCase())) {
      db.prepare("UPDATE call_conversations SET status='ringing', updated_at=datetime('now') WHERE id=?").run(resolvedConversationId);
      emitStatusUpdate({
        conversationId: resolvedConversationId,
        callSid,
        status: 'RINGING',
        duration: callDurationSec,
        sipResponseCode,
        answeredBy,
      });
      log.info('STATUS', `Call ringing for ${resolvedConversationId.slice(0, 8)}`);
    }
    return res.sendStatus(200);
  }

  // Handle in-progress (customer picked up) - update DB so UI immediately shows active call
  if (normalizedCallStatus === 'in-progress' && resolvedConversationId) {
    const pConv = db.prepare('SELECT status FROM call_conversations WHERE id=?').get(resolvedConversationId);
    if (pConv && !['in-progress', 'ending', 'completed', 'no-answer', 'busy', 'failed'].includes(String(pConv.status || '').toLowerCase())) {
      db.prepare("UPDATE call_conversations SET status='in-progress', updated_at=datetime('now') WHERE id=?").run(resolvedConversationId);
      emitStatusUpdate({
        conversationId: resolvedConversationId,
        callSid,
        status: 'IN_PROGRESS',
        duration: callDurationSec,
        sipResponseCode,
        answeredBy,
      });
      broadcast({ type: 'LOG', message: 'Customer answered - call in progress', source: 'twil' });
      log.info('STATUS', `Customer answered for ${resolvedConversationId.slice(0, 8)} - call in-progress`);
    }
    return res.sendStatus(200);
  }

  // Handle machine detection - hang up immediately if machine answered
  if (answeredBy === 'machine_start') {
    log.info('STATUS', `Machine detected for ${resolvedConversationId || 'unknown'} - hanging up`);
    if (callSid && !callSid.startsWith('MOCK_')) {
      const accountSid = process.env.TWILIO_ACCOUNT_SID;
      const authToken = process.env.TWILIO_AUTH_TOKEN;
      if (accountSid && authToken) {
        // Hang up the call
        fetch(`https://api.twilio.com/2010-04-01/Accounts/${accountSid}/Calls/${callSid}.json`, {
          method: 'POST',
          headers: {
            'Authorization': 'Basic ' + Buffer.from(`${accountSid}:${authToken}`).toString('base64'),
            'Content-Type': 'application/x-www-form-urlencoded',
          },
          body: new URLSearchParams({ Status: 'completed' }).toString(),
        }).catch(err => log.warn('STATUS', `Failed to hang up machine call: ${err.message}`));
      }
    }
    return res.sendStatus(200);
  }

  if (resolvedConversationId) {
    if (['no-answer', 'busy', 'failed', 'canceled', 'cancelled', 'completed', 'machine-detected'].includes(normalizedCallStatus)) {
      const conv = db.prepare('SELECT * FROM call_conversations WHERE id = ?').get(resolvedConversationId);
      if (conv) {
        const normalizedConvStatus = String(conv.status || '').toLowerCase();
        if (['completed', 'no-answer', 'busy', 'failed', 'canceled', 'cancelled'].includes(normalizedConvStatus)) {
          log.info('STATUS', `Ignoring duplicate terminal callback ${normalizedCallStatus} for ${resolvedConversationId?.slice(0, 8)} (already ${normalizedConvStatus})`);
          return res.sendStatus(200);
        }

        // Explicit handling for customer canceled/cancelled (declined, or call dropped before answer)
        if (['canceled', 'cancelled'].includes(normalizedCallStatus)) {
          log.info('STATUS', `Customer canceled/declined call for ${conv.customer_name} -> no-answer`);
          broadcast({ type: 'LOG', message: 'Customer canceled/declined the call - marked as no_answer', source: 'twil' });
        }

        const looksLikeDeclinedBeforeAnswer =
          normalizedCallStatus === 'completed' &&
          callDurationSec <= 0 &&
          !ACTIVE_CALLS.has(resolvedConversationId) &&
          !['active', 'in-progress', 'in_progress', 'ending', 'completed'].includes(String(conv.status || '').toLowerCase()) &&
          ['486', '487', '603', ''].includes(sipResponseCode);

        if (looksLikeDeclinedBeforeAnswer) {
          log.info('STATUS', `Customer ended before pickup for ${conv.customer_name} (duration=0, sip=${sipResponseCode || 'n/a'}) -> no-answer`);
          broadcast({ type: 'LOG', message: 'Customer ended before pickup - marked as no_answer', source: 'twil' });
        }

        // For 'completed' status (customer hangup), mark as 'ending' so UI shows analyzing state
        // The WebSocket handleCallEnd() will set final 'completed' after analysis
        if (normalizedCallStatus === 'completed' && !looksLikeDeclinedBeforeAnswer) {
          log.info('STATUS', `Call completed (hangup) for ${conv.customer_name} - marking as 'ending' for analysis`);
          if (conv.status !== 'completed' && conv.status !== 'ending') {
            db.prepare("UPDATE call_conversations SET status='ending', updated_at=datetime('now') WHERE id=?").run(resolvedConversationId);
            emitStatusUpdate({
              conversationId: resolvedConversationId,
              callSid,
              status: 'ENDING',
              duration: callDurationSec,
              sipResponseCode,
              answeredBy,
            });
          }

          // Safety net: if media-stream analysis never finalizes, auto-complete after timeout.
          setTimeout(() => {
            try {
              const latest = db.prepare('SELECT id, status, final_response, customer_id, customer_name, agent_type, tone FROM call_conversations WHERE id=?').get(resolvedConversationId);
              if (!latest) return;
              if (latest.status !== 'ending') return;
              if (ACTIVE_CALLS.has(resolvedConversationId)) return;

              const fallbackResponse = latest.final_response || 'callback_requested';
              const retryHours = { no_answer: 2, busy: 2, callback_requested: 2, non_customer_pickup: 5, refused: 24, negotiation_barrier: 5, payment_done: 0, ptp_secured: 0, partial_payment: 2, switched_off: 2, abuse_detected: 24 }[fallbackResponse] ?? 2;
              const fallbackNotes = 'Auto-finalized after ending-timeout (media stream analysis did not complete)';

              db.prepare("UPDATE call_conversations SET status='completed', final_response=?, notes=COALESCE(notes, ?), schedule_retry_hours=COALESCE(schedule_retry_hours, ?), updated_at=datetime('now') WHERE id=?")
                .run(fallbackResponse, fallbackNotes, retryHours, resolvedConversationId);

              if (latest.customer_id) {
                const retryAt = retryHours > 0 ? new Date(Date.now() + retryHours * 3600000).toISOString() : null;
                db.prepare("UPDATE customers SET final_response=?, last_call_date=datetime('now'), follow_up_count=follow_up_count+1, scheduled_retry_at=?, retry_reason=?, updated_at=datetime('now') WHERE id=?")
                  .run(fallbackResponse, retryAt, retryHours > 0 ? fallbackResponse : null, latest.customer_id);

                const logId = uuidv4();
                db.prepare('INSERT INTO call_logs (id, customer_id, customer_name, agent_type, tone, status, duration, outcome, notes) VALUES (?,?,?,?,?,?,?,?,?)')
                  .run(logId, latest.customer_id, latest.customer_name, latest.agent_type, latest.tone, 'completed', 0, fallbackResponse, fallbackNotes);
              }

              log.warn('STATUS', `[ENDING-SAFETY] Auto-finalized stuck conversation ${resolvedConversationId} with outcome=${fallbackResponse}`);
              emitStatusUpdate({
                conversationId: resolvedConversationId,
                callSid,
                status: 'COMPLETED',
                finalResponse: fallbackResponse,
                terminalReason: 'ending_timeout_finalize',
              });
            } catch (e) {
              log.warn('STATUS', `Ending-timeout finalizer failed for ${resolvedConversationId}: ${e.message}`);
            }
          }, 12000);
          return res.sendStatus(200);
        }
        const noAnswerLikeTerminal =
          looksLikeDeclinedBeforeAnswer ||
          ['no-answer', 'canceled', 'cancelled', 'machine-detected'].includes(normalizedCallStatus);

        const effectiveStatus = noAnswerLikeTerminal
          ? 'no-answer'
          : normalizedCallStatus;

        const response = noAnswerLikeTerminal
          ? 'no_answer'
          : effectiveStatus === 'busy'
          ? 'busy'
          : (effectiveStatus === 'failed' && isLikelySwitchedOff)
          ? 'switched_off'
          : 'no_answer';

        const terminalReason = looksLikeDeclinedBeforeAnswer || ['canceled', 'cancelled'].includes(normalizedCallStatus)
          ? 'declined_before_answer'
          : response === 'switched_off'
          ? 'unreachable_or_switched_off'
          : effectiveStatus === 'busy'
          ? 'line_busy'
          : normalizedCallStatus === 'machine-detected'
          ? 'machine_detected'
          : 'no_pickup';
        const RETRY_MAP = { 'no-answer': 2, 'busy': 2, 'failed': 2, 'machine-detected': 2 };
        const retryHours = RETRY_MAP[effectiveStatus] || 2;
        const terminalNote = `Terminal reason: ${terminalReason}`;

        // Clean up any lingering prewarm Gemini socket for this call
        cleanupPrewarmed(resolvedConversationId);

        // Update conversation
        db.prepare(`UPDATE call_conversations SET status=?, final_response=?, notes=COALESCE(notes, ?), updated_at=datetime('now') WHERE id=?`)
          .run(effectiveStatus, response, terminalNote, resolvedConversationId);

        // Broadcast so frontend polling instant-detects the terminal status
        emitStatusUpdate({
          conversationId: resolvedConversationId,
          callSid,
          status: effectiveStatus.toUpperCase().replace('-', '_'),
          finalResponse: response,
          terminalReason,
          duration: callDurationSec,
          sipResponseCode,
          answeredBy,
        });

        // Update customer record with outcome + schedule retry
        if (conv.customer_id) {
          const updates = [`final_response=?`, `last_call_date=datetime('now')`, `follow_up_count=follow_up_count+1`, `updated_at=datetime('now')`];
          const vals = [response];

          updates.push('scheduled_retry_at=?', 'retry_reason=?');
          vals.push(retryHours > 0 ? new Date(Date.now() + retryHours * 3600000).toISOString() : null, retryHours > 0 ? response : null);
          vals.push(conv.customer_id);
          db.prepare(`UPDATE customers SET ${updates.join(',')} WHERE id=?`).run(...vals);
          log.info('STATUS', `Customer ${conv.customer_name} updated: ${response}, retry in ${retryHours}h`);

          // Also create a call log entry
          const logId = uuidv4();
          db.prepare(`INSERT INTO call_logs (id, customer_id, customer_name, agent_type, tone, status, duration, outcome, notes) VALUES (?,?,?,?,?,?,?,?,?)`)
            .run(logId, conv.customer_id, conv.customer_name, conv.agent_type, conv.tone, effectiveStatus, callDurationSec || 0, response, `Call ${effectiveStatus} - auto-retry in ${retryHours}h`);
        }
      }
    }
  }
  res.sendStatus(200);
});

router.post('/end-call', async (req, res) => {
  try {
    const requestedConversationId = req.body?.conversationId || '';
    const requestedCallSid = req.body?.callSid || '';
    const conv = getConversationByIdOrCallSid(requestedConversationId || requestedCallSid);
    const callSid = requestedCallSid || conv?.call_sid || '';

    if (!callSid && !conv) {
      return res.status(400).json({ error: 'callSid or conversationId required' });
    }

    if (conv) {
      db.prepare("UPDATE call_conversations SET status='ending', updated_at=datetime('now') WHERE id=?").run(conv.id);
      clearCustomerRetrySchedule(conv.customer_id, 'manual-end-request');
    }

    // Hard fallback: close the active WebSocket directly via ACTIVE_CALLS registry.
    // This guarantees handleCallEnd() fires and runs analysis even if Twilio REST is unreachable.
    const activeConvId = conv?.id || requestedConversationId || '';
    const activeEntry = activeConvId ? ACTIVE_CALLS.get(activeConvId) : null;
    if (activeEntry?.twilioWs) {
      try {
        if (activeEntry.twilioWs.readyState === 1 /* OPEN */) {
          activeEntry.twilioWs.close(1000, 'manual_end_requested');
          log.info('CALL', `[END-CALL] Force-closed active WebSocket for conv=${activeConvId}`);
        }
      } catch (wsErr) {
        log.warn('CALL', `[END-CALL] WS close error: ${wsErr.message}`);
      }
    }

    if (!callSid || callSid === 'pending' || callSid.startsWith('MOCK_')) {
      log.info('CALL', `End-call acknowledged for local/mock conversation ${conv?.id || requestedConversationId}`);
      return res.json({ success: true, mock: true, conversationId: conv?.id || requestedConversationId || null });
    }

    const accountSid = process.env.TWILIO_ACCOUNT_SID;
    const authToken = process.env.TWILIO_AUTH_TOKEN;

    if (!accountSid || !authToken) {
      return res.status(500).json({ error: 'Twilio credentials missing' });
    }

    const twilioRes = await fetch(`https://api.twilio.com/2010-04-01/Accounts/${accountSid}/Calls/${callSid}.json`, {
      method: 'POST',
      headers: {
        'Authorization': 'Basic ' + Buffer.from(`${accountSid}:${authToken}`).toString('base64'),
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: new URLSearchParams({ Status: 'completed' }).toString(),
    });

    const twilioData = await twilioRes.json();
    if (!twilioRes.ok) {
      log.fail('CALL', `End-call failed for ${callSid}`, twilioData);
      // WebSocket was already closed above as a fallback, so analysis will still run
      return res.status(twilioRes.status).json({ error: 'Failed to end Twilio call', details: twilioData });
    }

    log.success('CALL', `End-call requested for ${callSid}`);
    res.json({ success: true, callSid, conversationId: conv?.id || requestedConversationId || null, status: twilioData.status });
  } catch (err) {
    log.error('CALL', 'end-call exception', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Legacy webhook ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬

// ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ GET conversation by ID (for polling) ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
router.get('/conversation/:id', (req, res) => {
  const conv = getConversationByIdOrCallSid(req.params.id);
  if (!conv) return res.status(404).json({ error: 'Not found' });
  try {
    let mappedLogId = CONVERSATION_CALL_LOG_MAP.get(conv.id) || null;
    let logRow = null;

    if (mappedLogId) {
      logRow = db.prepare('SELECT id, recording_url, transcript FROM call_logs WHERE id = ?').get(mappedLogId);
      if (!logRow) mappedLogId = null;
    }

    if (!mappedLogId && conv.customer_id) {
      const fallback = db.prepare(
        `SELECT id, recording_url, transcript
         FROM call_logs
         WHERE customer_id = ?
         ORDER BY created_at DESC
         LIMIT 1`
      ).get(conv.customer_id);
      if (fallback?.id) {
        mappedLogId = fallback.id;
        logRow = fallback;
      }
    }

    if (mappedLogId) conv.call_log_id = mappedLogId;
    if (logRow) {
      conv.recording_url = logRow.recording_url || null;
      conv.transcript = conv.transcript || logRow.transcript || null;
    }
  } catch {}
  // Parse messages JSON string
  try { conv.messages = JSON.parse(conv.messages || '[]'); } catch { conv.messages = []; }
  res.json(conv);
});

router.post('/call-webhook', (req, res) => {
  log.info('WEBHOOK', 'Legacy webhook hit');
  res.type('text/xml').send('<?xml version="1.0" encoding="UTF-8"?><Response><Hangup/></Response>');
});

// ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Fetch Recording (legacy) ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
router.post('/fetch-recording', async (req, res) => {
  const { callSid, callLogId, customerId, callDateTime, force } = req.body || {};
  if (!callLogId) return res.status(400).json({ error: 'callLogId required' });

  const logEntry = db.prepare('SELECT recording_url, transcript, customer_name FROM call_logs WHERE id = ?').get(callLogId);
  if (logEntry?.recording_url && !force) {
    if (!String(logEntry.transcript || '').trim()) {
      const localPath = getLocalRecordingPath(logEntry.recording_url);
      if (localPath && existsSync(localPath)) {
        const ext = getRecordingExtFromUrl(logEntry.recording_url);
        const audioBuffer = readFileSync(localPath);
        await ensureCallLogTranscriptFromRecording({
          callLogId,
          audioBuffer,
          mimeType: getMimeTypeFromRecordingExt(ext),
          customerName: logEntry.customer_name || 'Customer',
          callerName: 'Agent',
          force: false,
        });
      }
    }
    const refreshed = db.prepare('SELECT recording_url, transcript FROM call_logs WHERE id = ?').get(callLogId);
    return res.json({
      success: true,
      url: refreshed?.recording_url || logEntry.recording_url,
      transcript: refreshed?.transcript || null,
      message: 'Recording already saved',
    });
  }

  const accountSid = process.env.TWILIO_ACCOUNT_SID;
  const authToken = process.env.TWILIO_AUTH_TOKEN;

  if (!accountSid || !authToken || accountSid.startsWith('AC_test')) {
    // In mock mode we still provide a local file so UI download action succeeds.
    const mockWav = createWav(new Int16Array(16000), 8000);
    const filename = `${callLogId}.wav`;
    writeFileSync(path.join(RECORDINGS_DIR, filename), mockWav);
    db.prepare('UPDATE call_logs SET recording_url = ? WHERE id = ?').run(`/api/recordings/${filename}`, callLogId);
    return res.json({ success: true, mock: true });
  }

  try {
    let effectiveCallSid = String(callSid || '').trim();

    // Fallback: recover CallSid from conversation logs when UI only has callLogId/customer context.
    if (!effectiveCallSid) {
      const baseCustomerId = String(customerId || '').trim();
      let inferredCustomerId = baseCustomerId;
      if (!inferredCustomerId) {
        const fromLog = db.prepare('SELECT customer_id FROM call_logs WHERE id = ?').get(callLogId);
        inferredCustomerId = String(fromLog?.customer_id || '').trim();
      }

      const refTime = String(callDateTime || '').trim();
      if (inferredCustomerId && refTime) {
        const nearConv = db.prepare(
          `SELECT call_sid FROM call_conversations
           WHERE customer_id = ?
             AND call_sid IS NOT NULL
             AND call_sid <> ''
             AND call_sid <> 'pending'
           ORDER BY ABS(strftime('%s', created_at) - strftime('%s', ?)) ASC
           LIMIT 1`
        ).get(inferredCustomerId, refTime);
        effectiveCallSid = String(nearConv?.call_sid || '').trim();
      }

      if (!effectiveCallSid && inferredCustomerId) {
        const latestConv = db.prepare(
          `SELECT call_sid FROM call_conversations
           WHERE customer_id = ?
             AND call_sid IS NOT NULL
             AND call_sid <> ''
             AND call_sid <> 'pending'
           ORDER BY created_at DESC
           LIMIT 1`
        ).get(inferredCustomerId);
        effectiveCallSid = String(latestConv?.call_sid || '').trim();
      }
    }

    if (!effectiveCallSid) {
      return res.status(404).json({
        success: false,
        retryable: true,
        error: 'CallSid not found for this call. Try again in a few minutes.',
      });
    }

    const recRes = await fetch(`https://api.twilio.com/2010-04-01/Accounts/${accountSid}/Calls/${effectiveCallSid}/Recordings.json`, {
      headers: { 'Authorization': 'Basic ' + Buffer.from(`${accountSid}:${authToken}`).toString('base64') },
    });
    const recData = await recRes.json();
    if (!recRes.ok || !recData.recordings?.length) {
      return res.json({
        success: false,
        retryable: true,
        callSid: effectiveCallSid,
        message: 'Recording not yet available in Twilio logs',
      });
    }

    const recording = recData.recordings.find(r => String(r?.status || '').toLowerCase() === 'completed') || recData.recordings[0];
    if (!recording?.sid) {
      return res.json({
        success: false,
        retryable: true,
        callSid: effectiveCallSid,
        message: 'Recording SID not available yet',
      });
    }

    const downloaded = await fetchTwilioRecordingAudio({ accountSid, authToken, recordingSid: recording.sid });
    if (!downloaded) return res.status(500).json({ error: 'Download failed' });

    const filename = `${callLogId}.${downloaded.ext}`;
    writeFileSync(path.join(RECORDINGS_DIR, filename), downloaded.buffer);
    db.prepare('UPDATE call_logs SET recording_url = ? WHERE id = ?').run(`/api/recordings/${filename}`, callLogId);

    const callerName = db.prepare('SELECT caller_name FROM call_conversations WHERE call_sid = ? ORDER BY updated_at DESC LIMIT 1').get(effectiveCallSid)?.caller_name || 'Agent';
    const transcriptResult = await ensureCallLogTranscriptFromRecording({
      callLogId,
      audioBuffer: downloaded.buffer,
      mimeType: downloaded.mimeType,
      customerName: logEntry?.customer_name || 'Customer',
      callerName,
      force: true,
    });

    const refreshed = db.prepare('SELECT transcript FROM call_logs WHERE id = ?').get(callLogId);
    res.json({
      success: true,
      url: `/api/recordings/${filename}`,
      callSid: effectiveCallSid,
      transcript: refreshed?.transcript || null,
      transcriptGenerated: !!transcriptResult?.updated,
      recordingChannels: downloaded.channels,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Serve recordings ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
router.get('/recordings/:filename', (req, res) => {
  const filePath = path.join(RECORDINGS_DIR, req.params.filename);
  if (!existsSync(filePath)) return res.status(404).json({ error: 'Not found' });
  res.sendFile(filePath);
});

// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
// INTERNAL MAKE-CALL ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â used by scheduler (no HTTP req/res)
// ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â

export async function makeCallInternal(params) {
  const { customerId, customerName, customerPhone, agentType, tone, balance, dpd, ptpStatus, followUpCount, preparedScript, customerGender, callerGender, fetchTwilioRecording, callerName, callerVoice, callerLanguage, previousCallHistory, ptpDate, retryReason, noiseCancellation } = params;
  const DIAL_T0 = Date.now();

  if (!customerPhone || !customerName) throw new Error('customerPhone and customerName required');

  // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ RESOURCE GUARD: Prevent overloading during concurrent calls ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
  if (ACTIVE_CALLS.size >= RESOURCE_LIMITS.MAX_ACTIVE_CALLS) {
    log.warn('SCHEDULER', `ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â RESOURCE LIMIT: ${ACTIVE_CALLS.size}/${RESOURCE_LIMITS.MAX_ACTIVE_CALLS} active calls ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â rejecting new call for ${customerName}`);
    throw new Error(`Max concurrent calls reached (${RESOURCE_LIMITS.MAX_ACTIVE_CALLS})`);
  }

  // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ DUPLICATE GUARD: Prevent double-dialing the same customer ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
  if (customerId && RESOURCE_LIMITS.IN_FLIGHT_CALLS.has(customerId)) {
    log.warn('SCHEDULER', `ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â DUPLICATE: Call already in-flight for customer ${customerId} ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â skipping`);
    throw new Error('Call already in-flight for this customer');
  }
  if (customerId) RESOURCE_LIMITS.IN_FLIGHT_CALLS.add(customerId);

  const usedCallerName = callerName || 'Omar';
  const usedCallerGender = normalizeGender(callerGender || 'male', 'male');
  const usedCallerVoice = normalizeVoiceName(callerVoice || 'Orus', usedCallerGender);
  const usedCallerLang = callerLanguage || 'ur-PK';
  const usedCustGender = normalizeGender(customerGender || 'male', 'male');

  const convId = uuidv4();

  // Cleanup helper ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â removes all resources for this call
  function cleanupCallResources() {
    if (customerId) RESOURCE_LIMITS.IN_FLIGHT_CALLS.delete(customerId);
    CALL_PREPARATION_CACHE.delete(convId);
    cleanupPrewarmed(convId);
    CALL_PREPARATION_PROMISES.delete(convId);
  }

  try {
    db.prepare(`INSERT INTO call_conversations (id, call_sid, customer_id, customer_name, agent_type, tone, caller_name, voice, language, balance, dpd, ptp_status, follow_up_count)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(
      convId, 'pending', customerId || '', customerName, agentType || 'fresh_call', tone || 'polite',
      usedCallerName, usedCallerVoice, usedCallerLang, balance || 0, dpd || 0, ptpStatus || null, followUpCount || 0
    );

    const callContext = {
      id: convId,
      customer_id: customerId || '',
      customer_name: customerName,
      agent_type: agentType || 'fresh_call',
      tone: tone || 'polite',
      caller_name: usedCallerName,
      voice: usedCallerVoice,
      language: usedCallerLang,
      balance: balance || 0,
      dpd: dpd || 0,
      ptp_status: ptpStatus || null,
      follow_up_count: followUpCount || 0,
      customer_gender: usedCustGender,
      caller_gender: usedCallerGender,
      previousCallHistory,
      ptpDate,
      retryReason,
    };

    const accountSid = process.env.TWILIO_ACCOUNT_SID;
    const authToken = process.env.TWILIO_AUTH_TOKEN;
    const twilioPhone = process.env.TWILIO_PHONE_NUMBER;
    const serverUrl = getServerBaseUrl();

    if (!accountSid || !authToken || !twilioPhone || accountSid.startsWith('AC_test') || !serverUrl) {
      log.warn('SCHEDULER', `Mock call for ${customerName} (Twilio/SERVER_URL not configured)`);
      cleanupCallResources();
      return { success: true, mock: true, conversationId: convId };
    }

    // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ PHASE 1: PREPARE EVERYTHING BEFORE DIALING (concurrent, with timeout) ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
    const { warmPromise } = startCallAssetWarmup(callContext, { preparedScript, fetchTwilioRecording: true, maxPtpDays: params.maxPtpDays || 5, noiseCancellation: noiseCancellation === true });

    const geminiApiKey = getGeminiApiKey();
    let geminiPrewarmPromise = Promise.resolve();
    if (geminiApiKey && PREWARMED_GEMINI_WS.size < RESOURCE_LIMITS.MAX_PREWARM_WS) {
      geminiPrewarmPromise = new Promise((resolve) => {
        const geminiURL = `wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1alpha.GenerativeService.BidiGenerateContent?key=${geminiApiKey}`;
        const preWs = new WebSocket(geminiURL);
        preWs._prewarmCreatedAt = Date.now();
        let resolved = false;
        const done = () => { if (!resolved) { resolved = true; resolve(); } };
        preWs.on('open', () => done());
        preWs.on('error', (err) => {
          log.warn('SCHEDULER', `Gemini pre-warm error for ${customerName}: ${err.message}`);
          PREWARMED_GEMINI_WS.delete(convId);
          done();
        });
        preWs.on('close', () => { if (PREWARMED_GEMINI_WS.get(convId) === preWs) PREWARMED_GEMINI_WS.delete(convId); done(); });
        PREWARMED_GEMINI_WS.set(convId, preWs);
        setTimeout(done, RESOURCE_LIMITS.GEMINI_PREWARM_TIMEOUT_MS);
        setTimeout(() => {
          if (PREWARMED_GEMINI_WS.get(convId) === preWs) cleanupPrewarmed(convId);
        }, 45000);
      });
    } else if (geminiApiKey) {
      log.warn('SCHEDULER', `Skipping Gemini pre-warm ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â ${PREWARMED_GEMINI_WS.size}/${RESOURCE_LIMITS.MAX_PREWARM_WS} pre-warmed sockets active`);
    }

    await Promise.allSettled([warmPromise, geminiPrewarmPromise]);
    log.info('TIMING', `[PREP T+${Date.now()-DIAL_T0}ms] Scheduler call pre-warmed ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â dialing now`);

    // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ PHASE 2: DIAL (with timeout protection) ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
    const maxPtpDate = new Date(Date.now() + 5 * 86400000).toISOString().split('T')[0];
    const intHonorific = usedCustGender === 'female' ? 'Sahiba' : 'Sahab';
    const twimlParams = new URLSearchParams({
      conversationId: convId, customerId: customerId || '', agentName: usedCallerName,
      customerName, honorific: intHonorific, amount: String(balance || 0),
      dueDate: maxPtpDate, voice: usedCallerVoice, customerGender: usedCustGender, callerGender: usedCallerGender,
    });
    const statusCallbackUrl = `${serverUrl}/api/call-status?conversationId=${convId}`;

    const twilioCallParams = new URLSearchParams({
      To: customerPhone, From: twilioPhone,
      Url: `${serverUrl}/api/twiml?${twimlParams.toString()}`,
      StatusCallback: statusCallbackUrl, StatusCallbackMethod: 'POST',
      StatusCallbackEvent: 'initiated ringing answered completed',
      MachineDetection: 'Enable',
      MachineDetectionTimeout: '3',
      AsyncAmd: 'true',
      AsyncAmdStatusCallback: statusCallbackUrl,
      AsyncAmdStatusCallbackMethod: 'POST',
    });

    // Always record scheduler calls and attach recording callback.
    twilioCallParams.set('Record', 'true');
    twilioCallParams.set('RecordingStatusCallback', `${serverUrl}/api/recording-status?conversationId=${encodeURIComponent(convId)}`);
    twilioCallParams.set('RecordingStatusCallbackEvent', 'completed');

    // ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ TIMEOUT-PROTECTED Twilio API call ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
    const controller = new AbortController();
    const twilioTimeout = setTimeout(() => controller.abort(), RESOURCE_LIMITS.TWILIO_API_TIMEOUT_MS);

    let twilioRes;
    try {
      twilioRes = await fetch(`https://api.twilio.com/2010-04-01/Accounts/${accountSid}/Calls.json`, {
        method: 'POST',
        headers: {
          'Authorization': 'Basic ' + Buffer.from(`${accountSid}:${authToken}`).toString('base64'),
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        body: twilioCallParams.toString(),
        signal: controller.signal,
      });
    } catch (fetchErr) {
      clearTimeout(twilioTimeout);
      db.prepare('DELETE FROM call_conversations WHERE id = ?').run(convId);
      cleanupCallResources();
      const isTimeout = fetchErr.name === 'AbortError';
      log.error('SCHEDULER', `Twilio API ${isTimeout ? 'TIMEOUT' : 'FETCH ERROR'} for ${customerName}: ${fetchErr.message}`);
      throw new Error(`Twilio API ${isTimeout ? 'timed out' : 'failed'}: ${fetchErr.message}`);
    }
    clearTimeout(twilioTimeout);

    const twilioData = await twilioRes.json();
    if (!twilioRes.ok) {
      db.prepare('DELETE FROM call_conversations WHERE id = ?').run(convId);
      cleanupCallResources();
      throw new Error(`Twilio error: ${JSON.stringify(twilioData)}`);
    }

    db.prepare('UPDATE call_conversations SET call_sid = ? WHERE id = ?').run(twilioData.sid, convId);
    // Remove from in-flight once Twilio accepts (now tracked by ACTIVE_CALLS via media stream)
    if (customerId) RESOURCE_LIMITS.IN_FLIGHT_CALLS.delete(customerId);
    log.success('SCHEDULER', `Call initiated: ${twilioData.sid} for ${customerName} (pre-warmed, active: ${ACTIVE_CALLS.size})`);
    return { success: true, callSid: twilioData.sid, conversationId: convId };
  } catch (err) {
    cleanupCallResources();
    throw err;
  }
}

export default router;
