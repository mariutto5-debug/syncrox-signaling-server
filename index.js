require('dotenv').config();
const { WebSocketServer } = require('ws');
const crypto = require('crypto');
const http = require('http');
const { GoogleGenerativeAI } = require('@google/generative-ai');

// Global fetch for Cartesia TTS (Node 18+ built-in or node-fetch)
const fetch = globalThis.fetch || ((...args) => import('node-fetch').then(({ default: fetch }) => fetch(...args)));

const genAI = process.env.GEMINI_API_KEY ? new GoogleGenerativeAI(process.env.GEMINI_API_KEY) : null;

const PORT = process.env.PORT || 8080;
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Syncrox Signaling & Translation Server Active');
});
const wss = new WebSocketServer({ server });

// Pre-shared key for HMAC authentication
const SHARED_SECRET = process.env.SHARED_SECRET || 'syncrox_super_secret_key_2026';

// Environment Voice IDs
const CARTESIA_API_KEY = process.env.CARTESIA_API_KEY;
const CARTESIA_MALE_VOICE_ID = process.env.CARTESIA_MALE_VOICE_ID || 'b7d50908-b17c-442d-ad8d-730c6b9b78e9';
const CARTESIA_FEMALE_VOICE_ID = process.env.CARTESIA_FEMALE_VOICE_ID || 'a0e99841-438c-4a64-b679-ae501e7d6091';

// State Management
const connectedUsers = new Map();  // userId -> ws
const connectedPhones = new Map(); // sanitized phoneNumber -> ws
const userToPhone = new Map();     // userId -> phoneNumber
const phoneToUser = new Map();     // phoneNumber -> userId
const userGenders = new Map();     // userId -> 'male' | 'female'
const activeSessions = new Map();  // sessionToken -> { callerId, calleeId }
const userToSession = new Map();   // userId -> sessionToken

function cleanNumber(raw) {
  if (!raw) return '';
  const cleaned = String(raw).replace(/[^\d+]/g, '');
  if (cleaned.length < 3) return cleaned;
  return cleaned;
}

function verifyHmac(payloadStr, receivedHmac) {
  if (!receivedHmac) return true;
  const hmac = crypto.createHmac('sha256', SHARED_SECRET);
  hmac.update(payloadStr);
  const calculated = hmac.digest('hex');
  return calculated === receivedHmac;
}

function findUserWs(targetId, targetPhone) {
  const cleanPhone = cleanNumber(targetPhone);
  const cleanId = cleanNumber(targetId);

  if (targetId && connectedUsers.has(targetId)) {
    return connectedUsers.get(targetId);
  }
  if (cleanId && connectedUsers.has(cleanId)) {
    return connectedUsers.get(cleanId);
  }
  if (cleanId && connectedPhones.has(cleanId)) {
    return connectedPhones.get(cleanId);
  }
  if (cleanPhone && connectedPhones.has(cleanPhone)) {
    return connectedPhones.get(cleanPhone);
  }

  // Suffix matching (last 7 digits) if length >= 7
  const digits = (cleanPhone || cleanId || String(targetId || '')).replace(/\D/g, '');
  if (digits.length >= 7) {
    const suffix = digits.slice(-7);
    for (const [phone, ws] of connectedPhones.entries()) {
      const pDigits = phone.replace(/\D/g, '');
      if (pDigits.endsWith(suffix)) {
        return ws;
      }
    }
  }

  return null;
}

async function generateCartesiaTts(text, gender = 'male') {
  if (!CARTESIA_API_KEY) {
    console.warn('[CARTESIA] CARTESIA_API_KEY non configurata.');
    return null;
  }
  const voiceId = gender === 'female' ? CARTESIA_FEMALE_VOICE_ID : CARTESIA_MALE_VOICE_ID;
  try {
    const res = await fetch('https://api.cartesia.ai/tts/bytes', {
      method: 'POST',
      headers: {
        'Cartesia-Version': '2024-06-10',
        'X-API-Key': CARTESIA_API_KEY,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        model_id: 'sonic-multilingual',
        transcript: text,
        voice: { mode: 'id', id: voiceId },
        output_format: { container: 'raw', encoding: 'pcm_s16le', sample_rate: 24000 },
      }),
    });

    if (!res.ok) {
      const errText = await res.text();
      console.error(`[CARTESIA ERROR] Status ${res.status}: ${errText}`);
      return null;
    }

    const arrayBuffer = await res.arrayBuffer();
    return Buffer.from(arrayBuffer);
  } catch (err) {
    console.error(`[CARTESIA EXCEPTION] ${err.message}`);
    return null;
  }
}

wss.on('connection', (ws, req) => {
  let currentUserId = null;
  let currentPhoneNumber = null;
  const clientIp = req.socket.remoteAddress;
  console.log(`[CONNECTION] Nuova connessione da IP: ${clientIp}`);

  ws.on('error', (err) => {
    console.error(`[WS ERROR] Connessione ${clientIp}: ${err.message}`);
  });

  ws.on('message', async (message, isBinary) => {
    if (isBinary) {
      if (!currentUserId) return;
      const sessionToken = userToSession.get(currentUserId);
      if (!sessionToken) return;
      const session = activeSessions.get(sessionToken);
      if (!session) return;

      const recipientId = session.callerId === currentUserId ? session.calleeId : session.callerId;
      const recipientWs = connectedUsers.get(recipientId);

      if (recipientWs && recipientWs.readyState === 1) {
        recipientWs.send(message, { binary: true });
      }
      return;
    }

    try {
      const messageStr = message.toString();
      const parsed = JSON.parse(messageStr);
      const { type, payload, hmac } = parsed;

      const payloadStr = JSON.stringify(payload || {});
      if (!verifyHmac(payloadStr, hmac)) {
        console.warn(`[AUTH FAILED] HMAC invalido per messaggio: ${type}`);
        return;
      }

      switch (type) {
        case 'register': {
          const { userId, phoneNumber, gender } = payload || {};
          if (userId) {
            currentUserId = userId;
            connectedUsers.set(userId, ws);
            if (gender) userGenders.set(userId, gender);

            if (phoneNumber) {
              const sanitizedP = cleanNumber(phoneNumber);
              currentPhoneNumber = sanitizedP;
              connectedPhones.set(sanitizedP, ws);
              userToPhone.set(userId, sanitizedP);
              phoneToUser.set(sanitizedP, userId);
              console.log(`[REGISTER] Utente ${userId} registrato con numero sanificato ${sanitizedP} (Genere: ${gender || 'male'}).`);
            } else {
              console.log(`[REGISTER] Utente ${userId} registrato senza numero (Genere: ${gender || 'male'}).`);
            }

            ws.send(JSON.stringify({ type: 'registered', payload: { ok: true, userId, phoneNumber: currentPhoneNumber } }));
          }
          break;
        }

        case 'change_gender': {
          const { gender } = payload || {};
          if (currentUserId && (gender === 'male' || gender === 'female')) {
            userGenders.set(currentUserId, gender);
            console.log(`[GENDER CHANGE] Utente ${currentUserId} ha cambiato genere in: ${gender}`);
          }
          break;
        }

        case 'check_online_contacts':
        case 'check_syncrox_users': {
          const { numbers } = payload || {};
          const onlineNumbersSet = new Set();
          
          if (Array.isArray(numbers)) {
            for (const inputNum of numbers) {
              const cleanInput = cleanNumber(inputNum);
              if (connectedPhones.has(cleanInput)) {
                onlineNumbersSet.add(inputNum);
                onlineNumbersSet.add(cleanInput);
              } else {
                // Suffix matching (last 7 digits)
                const digits = cleanInput.replace(/\D/g, '');
                if (digits.length >= 7) {
                  const suffix = digits.slice(-7);
                  for (const phone of connectedPhones.keys()) {
                    if (phone.replace(/\D/g, '').endsWith(suffix)) {
                      onlineNumbersSet.add(inputNum);
                      onlineNumbersSet.add(phone);
                      break;
                    }
                  }
                }
              }
            }
          }

          ws.send(JSON.stringify({
            type: 'online_contacts_response',
            payload: {
              onlineNumbers: Array.from(onlineNumbersSet),
              activeNumbers: Array.from(connectedPhones.keys())
            }
          }));
          break;
        }

        case 'call_user':
        case 'call_request': {
          const {
            callerId,
            callerPhoneNumber,
            callerName,
            calleeId,
            targetUserId,
            targetPhoneNumber,
            calleePhoneNumber,
            sessionToken,
            gender
          } = payload || {};

          if (gender && callerId) userGenders.set(callerId, gender);

          const cleanCallerPhone = cleanNumber(callerPhoneNumber);
          const cleanTargetPhone = cleanNumber(targetPhoneNumber || calleePhoneNumber || targetUserId || calleeId);
          const targetId = targetUserId || calleeId;

          console.log(`[CALL REQUEST] Caller: ${callerId} (${cleanCallerPhone}) -> Target ID: ${targetId} / Target Phone: ${cleanTargetPhone} (Session: ${sessionToken})`);

          const calleeWs = findUserWs(targetId, cleanTargetPhone);

          if (calleeWs && calleeWs.readyState === 1) {
            let actualCalleeId = targetId;
            for (const [uid, uws] of connectedUsers.entries()) {
              if (uws === calleeWs) {
                actualCalleeId = uid;
                break;
              }
            }

            activeSessions.set(sessionToken, { callerId, calleeId: actualCalleeId });
            userToSession.set(callerId, sessionToken);
            userToSession.set(actualCalleeId, sessionToken);

            calleeWs.send(JSON.stringify({
              type: 'incoming_call',
              payload: {
                callerId,
                callerPhoneNumber: cleanCallerPhone,
                callerName: callerName || 'Utente Syncrox',
                sessionToken
              }
            }));
          } else {
            console.warn(`[CALL REJECTED] Target ${targetId} (${cleanTargetPhone}) non trovato o offline.`);
            ws.send(JSON.stringify({
              type: 'call_rejected',
              payload: { sessionToken, reason: 'user_offline' }
            }));
          }
          break;
        }

        case 'call_answer': {
          const { sessionToken: st, accepted, gender } = payload || {};
          if (currentUserId && gender) userGenders.set(currentUserId, gender);
          const session = activeSessions.get(st);
          if (session) {
            const callerWs = connectedUsers.get(session.callerId);
            const calleeWs = connectedUsers.get(session.calleeId);

            if (accepted) {
              console.log(`[CALL ACCEPTED] Sessione: ${st}`);
              if (callerWs && callerWs.readyState === 1) {
                callerWs.send(JSON.stringify({
                  type: 'call_accepted',
                  payload: { sessionToken: st }
                }));
              }
              if (calleeWs && calleeWs.readyState === 1) {
                calleeWs.send(JSON.stringify({
                  type: 'call_accepted',
                  payload: { sessionToken: st }
                }));
              }
            } else {
              console.log(`[CALL REJECTED] Sessione: ${st}`);
              if (callerWs && callerWs.readyState === 1) {
                callerWs.send(JSON.stringify({
                  type: 'call_rejected',
                  payload: { sessionToken: st, reason: 'declined' }
                }));
              }
              activeSessions.delete(st);
              userToSession.delete(session.callerId);
              userToSession.delete(session.calleeId);
            }
          }
          break;
        }

        case 'call_end': {
          const { sessionToken: endSt } = payload || {};
          const endSession = activeSessions.get(endSt);
          if (endSession) {
            console.log(`[CALL ENDED] Sessione: ${endSt}`);
            const otherId = endSession.callerId === currentUserId ? endSession.calleeId : endSession.callerId;
            const otherWs = connectedUsers.get(otherId);

            if (otherWs && otherWs.readyState === 1) {
              otherWs.send(JSON.stringify({
                type: 'call_ended',
                payload: { sessionToken: endSt }
              }));
            }

            activeSessions.delete(endSt);
            userToSession.delete(endSession.callerId);
            userToSession.delete(endSession.calleeId);
          }
          break;
        }

        case 'transcription': {
          const { text, targetLanguage } = payload || {};
          if (!currentUserId || !text) break;

          const transSessionToken = userToSession.get(currentUserId);
          if (!transSessionToken) break;
          const transSession = activeSessions.get(transSessionToken);
          if (!transSession) break;

          const recipientId = transSession.callerId === currentUserId ? transSession.calleeId : transSession.callerId;
          const recipientWs = connectedUsers.get(recipientId);

          if (!recipientWs || recipientWs.readyState !== 1) break;

          if (!genAI) {
            console.error('[TRANSCRIPTION] GEMINI_API_KEY non configurata.');
            recipientWs.send(JSON.stringify({
              type: 'subtitle',
              payload: { text, originalText: text }
            }));
            break;
          }

          try {
            const systemPrompt = `Sei un interprete telefonico simultaneo professionale in tempo reale.
1. Traduci il testo fornito dall'utente dalla lingua di origine alla lingua target della chiamata (${targetLanguage || 'italiano'}).
2. Rileva ed emula automaticamente il livello di formalità del parlante (formale o informale a seconda del contesto).
3. Rimuovi automaticamente balbettii, esitazioni e parole riempitive (come 'ehm', 'cioè').
4. Mantieni un tono di parlato naturale e fluido.
5. REGOLE FONDAMENTALI: Restituisci ESCLUSIVAMENTE il testo tradotto, senza introduzioni, commenti o punteggiatura extra.`;

            const model = genAI.getGenerativeModel({
              model: 'gemini-2.0-flash',
              systemInstruction: systemPrompt,
            });

            const result = await model.generateContent(text);
            const translatedText = (await result.response).text().trim();

            console.log(`[TRANSLATION] ${currentUserId} -> ${recipientId}: "${translatedText}"`);

            recipientWs.send(JSON.stringify({
              type: 'subtitle',
              payload: { text: translatedText, senderId: currentUserId }
            }));
            recipientWs.send(JSON.stringify({
              type: 'transcription',
              payload: { text: translatedText, originalText: text }
            }));

            const speakerGender = userGenders.get(currentUserId) || 'male';
            const audioBuffer = await generateCartesiaTts(translatedText, speakerGender);
            if (audioBuffer) {
              recipientWs.send(audioBuffer, { binary: true });
            }
          } catch (err) {
            console.error(`[TRANSLATION ERROR] ${err.message}`);
          }
          break;
        }

        default:
          break;
      }
    } catch (e) {
      console.error(`[SERVER ERROR] Parsing fallito: ${e.message}`);
    }
  });

  ws.on('close', () => {
    if (currentUserId) {
      console.log(`[DISCONNECT] Utente ${currentUserId} (${currentPhoneNumber || 'No Phone'}) disconnesso.`);
      connectedUsers.delete(currentUserId);
      if (currentPhoneNumber) connectedPhones.delete(currentPhoneNumber);
      userGenders.delete(currentUserId);

      const sessionToken = userToSession.get(currentUserId);
      if (sessionToken) {
        const session = activeSessions.get(sessionToken);
        if (session) {
          const otherId = session.callerId === currentUserId ? session.calleeId : session.callerId;
          const otherWs = connectedUsers.get(otherId);
          if (otherWs && otherWs.readyState === 1) {
            otherWs.send(JSON.stringify({
              type: 'call_ended',
              payload: { sessionToken, reason: 'peer_disconnected' }
            }));
          }
          activeSessions.delete(sessionToken);
          userToSession.delete(otherId);
        }
        userToSession.delete(currentUserId);
      }
    }
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Syncrox Server in ascolto sulla porta ${PORT}`);
});
