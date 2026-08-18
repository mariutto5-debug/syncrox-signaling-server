const { WebSocketServer } = require('ws');
const crypto = require('crypto');
const http = require('http');
const { GoogleGenerativeAI } = require('@google/generative-ai');

const genAI = process.env.GEMINI_API_KEY ? new GoogleGenerativeAI(process.env.GEMINI_API_KEY) : null;

const PORT = process.env.PORT || 8080;
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('OK');
});
const wss = new WebSocketServer({ server });

// Pre-shared key for HMAC authentication — set via environment variable on the server
const SHARED_SECRET = process.env.SHARED_SECRET || 'syncrox_super_secret_key_2026';

// State
const connectedUsers = new Map(); // userId -> ws
const activeSessions = new Map(); // sessionToken -> { callerId, calleeId }
const userToSession = new Map(); // userId -> sessionToken

function verifyHmac(messageStr, receivedHmac) {
    if (!receivedHmac) return false;
    const hmac = crypto.createHmac('sha256', SHARED_SECRET);
    hmac.update(messageStr);
    const calculated = hmac.digest('hex');
    return calculated === receivedHmac;
}

wss.on('connection', (ws, req) => {
    let currentUserId = null;
    const clientIp = req.socket.remoteAddress;
    console.log(`[CONNECTION] New incoming connection from IP: ${clientIp}`);

    ws.on('error', (err) => {
        console.error(`[WS ERROR] Error on connection from ${clientIp}: ${err.message}`);
    });

    ws.on('message', async (message, isBinary) => {
        if (isBinary) {
            // Handle binary audio chunk
            if (!currentUserId) return; // Unregistered user
            
            const sessionToken = userToSession.get(currentUserId);
            if (!sessionToken) return; // Not in a call
            
            const session = activeSessions.get(sessionToken);
            if (!session) return;
            
            // Determine the recipient
            const recipientId = session.callerId === currentUserId ? session.calleeId : session.callerId;
            const recipientWs = connectedUsers.get(recipientId);
            
            if (recipientWs && recipientWs.readyState === 1) { // WebSocket.OPEN
                // Relay the binary audio chunk directly
                recipientWs.send(message, { binary: true });
            }
            return;
        }

        // Handle text (JSON) signaling messages
        try {
            const messageStr = message.toString();
            const parsed = JSON.parse(messageStr);
            
            const { type, payload, hmac } = parsed;
            
            // Reconstruct the JSON string exactly as Flutter will send it to verify HMAC
            // In a real app, you'd send the payload string and HMAC separately, or sign a specific format.
            // For simplicity, Flutter will send: {"type":"...","payload":{...},"hmac":"..."}
            // We'll verify by signing the payload object stringified.
            
            const payloadStr = JSON.stringify(payload);
            if (!verifyHmac(payloadStr, hmac)) {
                console.warn(`[AUTH FAILED] Invalid HMAC for message type: ${type}`);
                return;
            }

            switch (type) {
                case 'register':
                    const { userId } = payload;
                    if (userId) {
                        currentUserId = userId;
                        connectedUsers.set(userId, ws);
                        console.log(`[REGISTER] User ${userId} connected.`);
                        ws.send(JSON.stringify({ type: 'registered', payload: { ok: true } }));
                    }
                    break;

                case 'call_request':
                    const { callerId, calleeId, sessionToken, callerName } = payload;
                    console.log(`[CALL REQUEST] ${callerId} calling ${calleeId} (Session: ${sessionToken})`);
                    
                    const calleeWs = connectedUsers.get(calleeId);
                    if (calleeWs && calleeWs.readyState === 1) {
                        // Register session tentatively
                        activeSessions.set(sessionToken, { callerId, calleeId });
                        userToSession.set(callerId, sessionToken);
                        userToSession.set(calleeId, sessionToken);
                        
                        // Forward to callee
                        calleeWs.send(JSON.stringify({
                            type: 'incoming_call',
                            payload: { callerId, callerName, sessionToken }
                        }));
                    } else {
                        // Callee offline
                        ws.send(JSON.stringify({
                            type: 'call_rejected',
                            payload: { sessionToken, reason: 'user_offline' }
                        }));
                    }
                    break;

                case 'call_answer':
                    const { sessionToken: st, accepted } = payload;
                    const session = activeSessions.get(st);
                    if (session) {
                        const callerWs = connectedUsers.get(session.callerId);
                        if (callerWs && callerWs.readyState === 1) {
                            if (accepted) {
                                console.log(`[CALL ACCEPTED] Session: ${st}`);
                                callerWs.send(JSON.stringify({
                                    type: 'call_accepted',
                                    payload: { sessionToken: st }
                                }));
                            } else {
                                console.log(`[CALL REJECTED] Session: ${st}`);
                                callerWs.send(JSON.stringify({
                                    type: 'call_rejected',
                                    payload: { sessionToken: st, reason: 'declined' }
                                }));
                                // Cleanup
                                activeSessions.delete(st);
                                userToSession.delete(session.callerId);
                                userToSession.delete(session.calleeId);
                            }
                        }
                    }
                    break;

                case 'call_end':
                    const { sessionToken: endSt } = payload;
                    const endSession = activeSessions.get(endSt);
                    if (endSession) {
                        console.log(`[CALL ENDED] Session: ${endSt}`);
                        const otherId = endSession.callerId === currentUserId ? endSession.calleeId : endSession.callerId;
                        const otherWs = connectedUsers.get(otherId);
                        
                        if (otherWs && otherWs.readyState === 1) {
                            otherWs.send(JSON.stringify({
                                type: 'call_ended',
                                payload: { sessionToken: endSt }
                            }));
                        }
                        
                        // Cleanup
                        activeSessions.delete(endSt);
                        userToSession.delete(endSession.callerId);
                        userToSession.delete(endSession.calleeId);
                    }
                    break;

                case 'transcription':
                    const { text, targetLanguage } = payload;
                    console.log(`[TRANSCRIPTION] From ${currentUserId}: "${text}" translating to ${targetLanguage}`);
                    
                    if (!currentUserId) {
                        console.warn('[TRANSCRIPTION] Unregistered user tried to translate.');
                        break;
                    }
                    
                    const transSessionToken = userToSession.get(currentUserId);
                    if (!transSessionToken) {
                        console.warn(`[TRANSCRIPTION] User ${currentUserId} is not in an active call.`);
                        break;
                    }
                    
                    const transSession = activeSessions.get(transSessionToken);
                    if (!transSession) {
                        console.warn(`[TRANSCRIPTION] Active session not found for token ${transSessionToken}.`);
                        break;
                    }
                    
                    // Determine Utente B
                    const recipientId = transSession.callerId === currentUserId ? transSession.calleeId : transSession.callerId;
                    const recipientWs = connectedUsers.get(recipientId);
                    
                    if (!recipientWs || recipientWs.readyState !== 1) {
                        console.warn(`[TRANSCRIPTION] Recipient ${recipientId} is not connected or open.`);
                        break;
                    }

                    if (!genAI) {
                        console.error('[TRANSCRIPTION] Gemini API is not configured (GEMINI_API_KEY env var missing).');
                        // Fallback: send original text
                        recipientWs.send(JSON.stringify({
                            type: 'transcription',
                            payload: {
                                text: text,
                                originalText: text,
                                targetLanguage: targetLanguage,
                                error: 'Gemini API not configured'
                            }
                        }));
                        break;
                    }

                    try {
                        const systemPrompt = `Sei un interprete telefonico simultaneo professionale in tempo reale.
Traduci il testo fornito dall'utente nella seguente lingua target: ${targetLanguage}.

Regole di traduzione:
1. Rileva ed emula automaticamente il livello di formalità del parlante (usa il 'Lei'/formale o 'Tu'/informale a seconda del contesto).
2. Rimuovi automaticamente balbettii, esitazioni, parole riempitive e ripetizioni (es. 'ehm', 'ah', 'cioè').
3. Mantieni un tono di parlato naturale, scorrevole ed emotivamente coerente con la frase originale.

REGOLE FONDAMENTALI: Restituisci ESCLUSIVAMENTE la frase tradotta. Non aggiungere mai commenti, introduzioni, spiegazioni, virgolette o prefissi come 'Ecco la traduzione:'.`;

                        const model = genAI.getGenerativeModel({
                            model: "gemini-2.0-flash",
                            systemInstruction: systemPrompt
                        });

                        const result = await model.generateContent(text);
                        const response = await result.response;
                        const translatedText = response.text().trim();
                        
                        console.log(`[TRANSCRIPTION] Translation success: "${translatedText}"`);
                        
                        recipientWs.send(JSON.stringify({
                            type: 'transcription',
                            payload: {
                                text: translatedText,
                                originalText: text,
                                targetLanguage: targetLanguage
                            }
                        }));
                    } catch (err) {
                        console.error(`[TRANSCRIPTION] Error calling Gemini: ${err.message}`);
                        // Fallback: send original text
                        recipientWs.send(JSON.stringify({
                            type: 'transcription',
                            payload: {
                                text: text,
                                originalText: text,
                                targetLanguage: targetLanguage,
                                error: err.message
                            }
                        }));
                    }
                    break;
            }
        } catch (e) {
            console.error(`[ERROR] Failed to parse message: ${e.message}`);
        }
    });

    ws.on('close', () => {
        if (currentUserId) {
            console.log(`[DISCONNECT] User ${currentUserId} disconnected.`);
            connectedUsers.delete(currentUserId);
            
            // End active session if any
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

server.listen(PORT, '0.0.0.0', () => { console.log(`Server running on port ${PORT}`); });

