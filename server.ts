import express from 'express';
import type { Request, Response } from 'express';
import http from 'http';
import path from 'path';
import fs from 'fs';

interface EmergencyCallPayload {
  callId: string;
  callerDeviceId?: string;
  familyCircleId?: string;
  callerName: string;
  callerPhone: string;
  recipientName: string;
  recipientPhone: string;
  priority: number;
  status: 'ringing' | 'connected' | 'declined' | 'ended';
  coordinates: {
    latitude: number;
    longitude: number;
    address?: string;
    accuracy?: number;
  };
  batteryLevel: number;
  attemptNumber: number;
  cycleNumber: number;
  startedAt: number;
  updatedAt: number;
  connectedByName?: string;
}

interface WebRtcSignal {
  callId: string;
  sender: 'caller' | 'recipient';
  type: 'offer' | 'answer' | 'candidate';
  payload: any;
  timestamp: number;
}

interface RegisteredDevice {
  deviceId: string;
  role: 'caller' | 'contact';
  name: string;
  phone: string;
  familyCircleId: string;
  googleEmail?: string;
  lastSeen: number;
}

interface DangerAlertPayload {
  alertId: string;
  callerDeviceId?: string;
  familyCircleId: string;
  callerName: string;
  coordinates: {
    latitude: number;
    longitude: number;
    address?: string;
    accuracy?: number;
  };
  batteryLevel: number;
  message: string;
  timestamp: number;
  isResolved?: boolean;
}

interface PushSubscriptionPayload {
  deviceId: string;
  subscription: any;
  platform: 'web' | 'android' | 'ios';
  familyCircleId: string;
  fcmToken?: string;
  voipToken?: string;
  timestamp: number;
}

async function startServer() {
  const app = express();
  const server = http.createServer(app);
  const PORT = Number(process.env.PORT) || 3000;
  const isProd = process.env.NODE_ENV === 'production';
  const isHmrDisabled = process.env.DISABLE_HMR === 'true';

  app.use(express.json({ limit: '10mb' }));

  // Health check endpoint for Cloud Run and monitoring
  app.get('/api/health', (_req: Request, res: Response) => {
    res.json({ status: 'ok', service: 'safeher', timestamp: Date.now() });
  });

  // In-memory state for real-time in-app calls, push notifications and alerts
  let activeEmergencyCall: EmergencyCallPayload | null = null;
  let activeDangerAlert: DangerAlertPayload | null = null;
  let callHistory: EmergencyCallPayload[] = [];
  const sseClients: Set<Response> = new Set();
  const webrtcSignals: WebRtcSignal[] = [];
  const registeredDevices: Map<string, RegisteredDevice> = new Map();
  const pushSubscriptions: Map<string, PushSubscriptionPayload> = new Map();

  // Helper: Broadcast state change to all active SSE subscribers (e.g. Mom's phone, Dad's phone, Caller)
  const broadcastCallUpdate = (event: string, data: any) => {
    const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
    sseClients.forEach((client) => {
      try {
        client.write(payload);
      } catch {
        sseClients.delete(client);
      }
    });
  };

  // Helper: Broadcast background call push to registered devices in family circle
  const dispatchSystemWideCallPush = (call: EmergencyCallPayload) => {
    const pushPayload = {
      type: 'EMERGENCY_CALL',
      callId: call.callId,
      callerName: call.callerName,
      callerPhone: call.callerPhone,
      recipientName: call.recipientName,
      priority: call.priority,
      status: call.status,
      coordinates: call.coordinates,
      timestamp: Date.now(),
      title: `🚨 SOS EMERGENCY — ${call.callerName} MAY BE IN DANGER`,
      body: `Incoming emergency call from ${call.callerName} (${call.callerPhone}). Location: ${call.coordinates.address || 'Live GPS'}. Tap to answer immediately.`,
    };

    // Broadcast SSE push notification trigger for background tabs / Service Workers
    broadcastCallUpdate('system_call_push', pushPayload);
  };

  // --- API Endpoints ---

  // 1. SSE Real-Time Event Stream for Instant In-App Call Ringing on Primary Contacts' Devices
  app.get('/api/emergency/events', (req: Request, res: Response) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache, no-transform');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders?.();

    sseClients.add(res);

    // Send immediate initial sync
    const initData = {
      activeCall: activeEmergencyCall,
      activeAlert: activeDangerAlert,
      timestamp: Date.now(),
    };
    res.write(`event: sync\ndata: ${JSON.stringify(initData)}\n\n`);

    // Keep-alive heartbeat every 15 seconds
    const heartbeat = setInterval(() => {
      try {
        res.write(`event: ping\ndata: {"time":${Date.now()}}\n\n`);
      } catch {
        clearInterval(heartbeat);
        sseClients.delete(res);
      }
    }, 15000);

    req.on('close', () => {
      clearInterval(heartbeat);
      sseClients.delete(res);
    });
  });

  // 2. Register Device Role & Family Circle (e.g. Maya / Mom / Dad)
  app.post('/api/emergency/device/register', (req: Request, res: Response) => {
    const { deviceId, role, name, phone, familyCircleId, googleEmail } = req.body;
    if (deviceId) {
      registeredDevices.set(deviceId, {
        deviceId,
        role: role || 'contact',
        name: name || 'Primary Contact',
        phone: phone || '',
        familyCircleId: (familyCircleId || 'CIRCLE-7788').trim().toUpperCase(),
        googleEmail: googleEmail || '',
        lastSeen: Date.now(),
      });
    }
    res.json({
      success: true,
      registeredCount: registeredDevices.size,
      activeCall: activeEmergencyCall,
      devices: Array.from(registeredDevices.values()),
    });
  });

  // 2b. Register Web Push / FCM / APNs Subscription for Outside-App Emergency Calling
  app.post('/api/emergency/push/subscribe', (req: Request, res: Response) => {
    const { deviceId, subscription, platform, familyCircleId, fcmToken, voipToken } = req.body;
    if (deviceId) {
      pushSubscriptions.set(deviceId, {
        deviceId,
        subscription: subscription || null,
        platform: platform || 'web',
        familyCircleId: (familyCircleId || 'CIRCLE-7788').trim().toUpperCase(),
        fcmToken: fcmToken || '',
        voipToken: voipToken || '',
        timestamp: Date.now(),
      });
    }
    res.json({
      success: true,
      totalSubscribers: pushSubscriptions.size,
    });
  });

  // 2c. Test Outside-App Call Alert (allows contact to simulate locked phone or background app)
  app.post('/api/emergency/push/test', (req: Request, res: Response) => {
    const { callerName, callerPhone, coordinates, familyCircleId } = req.body;
    const testCall: EmergencyCallPayload = {
      callId: `test-call-${Date.now()}`,
      familyCircleId: (familyCircleId || 'CIRCLE-7788').trim().toUpperCase(),
      callerName: callerName || 'Maya',
      callerPhone: callerPhone || '+91 99999 11111',
      recipientName: 'Mom',
      recipientPhone: '+91 98765 43210',
      priority: 1,
      status: 'ringing',
      coordinates: coordinates || { latitude: 28.6139, longitude: 77.209, address: 'Test Location (Live GPS)' },
      batteryLevel: 92,
      attemptNumber: 1,
      cycleNumber: 1,
      startedAt: Date.now(),
      updatedAt: Date.now(),
    };

    dispatchSystemWideCallPush(testCall);
    broadcastCallUpdate('incoming_call', testCall);
    res.json({ success: true, testCall });
  });

  // 3. Get Current In-App Emergency Call Status & Circle Members
  app.get('/api/emergency/status', (_req: Request, res: Response) => {
    res.json({
      activeCall: activeEmergencyCall,
      activeAlert: activeDangerAlert,
      serverTime: Date.now(),
      registeredDevices: Array.from(registeredDevices.values()),
      pushSubscribersCount: pushSubscriptions.size,
    });
  });

  // 4. Initiate or Update In-App Emergency Call
  app.post('/api/emergency/call', (req: Request, res: Response) => {
    const payload: Partial<EmergencyCallPayload> = req.body;
    const now = Date.now();

    activeEmergencyCall = {
      callId: payload.callId || `call-${now}`,
      callerDeviceId: payload.callerDeviceId || '',
      familyCircleId: (payload.familyCircleId || 'CIRCLE-7788').trim().toUpperCase(),
      callerName: payload.callerName || 'Maya',
      callerPhone: payload.callerPhone || '+91 99999 11111',
      recipientName: payload.recipientName || 'Mom',
      recipientPhone: payload.recipientPhone || '+91 98765 43210',
      priority: payload.priority ?? 1,
      status: payload.status || 'ringing',
      coordinates: payload.coordinates || { latitude: 28.6139, longitude: 77.209 },
      batteryLevel: payload.batteryLevel ?? 85,
      attemptNumber: payload.attemptNumber ?? 1,
      cycleNumber: payload.cycleNumber ?? 1,
      startedAt: payload.startedAt || now,
      updatedAt: now,
    };

    broadcastCallUpdate('incoming_call', activeEmergencyCall);
    dispatchSystemWideCallPush(activeEmergencyCall);
    res.json({ success: true, call: activeEmergencyCall });
  });

  // 4b. Broadcast Danger Alert & Live GPS (Without opening native SMS dialer)
  app.post('/api/emergency/alert', (req: Request, res: Response) => {
    const payload = req.body;
    const now = Date.now();

    if (payload.isResolved || payload.clearAlert) {
      activeDangerAlert = null;
      broadcastCallUpdate('danger_alert_cleared', { timestamp: now });
      return res.json({ success: true, alert: null });
    }

    activeDangerAlert = {
      alertId: payload.alertId || `alert-${now}`,
      callerDeviceId: payload.callerDeviceId || '',
      familyCircleId: (payload.familyCircleId || 'CIRCLE-7788').trim().toUpperCase(),
      callerName: payload.callerName || 'Maya',
      coordinates: payload.coordinates || { latitude: 28.6139, longitude: 77.209 },
      batteryLevel: payload.batteryLevel ?? 85,
      message: payload.message || 'Immediate Danger Alert',
      timestamp: now,
      isResolved: false,
    };

    broadcastCallUpdate('danger_alert', activeDangerAlert);
    res.json({ success: true, alert: activeDangerAlert });
  });

  // 4c. Clear Active Danger Alert (When user confirms "I Am Safe" or cancels SOS)
  app.post('/api/emergency/alert/clear', (_req: Request, res: Response) => {
    activeDangerAlert = null;
    broadcastCallUpdate('danger_alert_cleared', { timestamp: Date.now() });
    res.json({ success: true });
  });

  // 5. Recipient Answers In-App Emergency Call (Stops ringing, starts audio)
  app.post('/api/emergency/call/accept', (req: Request, res: Response) => {
    if (!activeEmergencyCall) {
      return res.status(404).json({ error: 'No active call to accept' });
    }

    const { responderName } = req.body;
    activeEmergencyCall.status = 'connected';
    activeEmergencyCall.connectedByName = responderName || activeEmergencyCall.recipientName || 'Primary Contact';
    activeEmergencyCall.updatedAt = Date.now();

    broadcastCallUpdate('call_answered', activeEmergencyCall);
    res.json({ success: true, call: activeEmergencyCall });
  });

  // 6. Recipient Declines In-App Emergency Call (Persistent Emergency Escalation: Keep calling!)
  app.post('/api/emergency/call/decline', (req: Request, res: Response) => {
    if (!activeEmergencyCall) {
      return res.status(404).json({ error: 'No active call to decline' });
    }

    activeEmergencyCall.status = 'declined';
    activeEmergencyCall.updatedAt = Date.now();

    broadcastCallUpdate('call_declined', activeEmergencyCall);

    // Persistent safety escalation: If emergency is active, keep calling!
    setTimeout(() => {
      if (activeEmergencyCall && activeEmergencyCall.status !== 'ended' && activeEmergencyCall.status !== 'connected') {
        activeEmergencyCall.status = 'ringing';
        activeEmergencyCall.attemptNumber = (activeEmergencyCall.attemptNumber || 1) + 1;
        activeEmergencyCall.updatedAt = Date.now();
        broadcastCallUpdate('incoming_call', activeEmergencyCall);
        dispatchSystemWideCallPush(activeEmergencyCall);
      }
    }, 1200);

    res.json({ success: true, call: activeEmergencyCall });
  });

  // 6b. Recipient Swiped or Dismissed Call Notification (Persistently recall immediately)
  app.post('/api/emergency/call/swipe_dismiss', (_req: Request, res: Response) => {
    if (activeEmergencyCall && activeEmergencyCall.status !== 'ended' && activeEmergencyCall.status !== 'connected') {
      setTimeout(() => {
        if (activeEmergencyCall && activeEmergencyCall.status !== 'ended' && activeEmergencyCall.status !== 'connected') {
          activeEmergencyCall.status = 'ringing';
          activeEmergencyCall.attemptNumber = (activeEmergencyCall.attemptNumber || 1) + 1;
          activeEmergencyCall.updatedAt = Date.now();
          broadcastCallUpdate('incoming_call', activeEmergencyCall);
          dispatchSystemWideCallPush(activeEmergencyCall);
        }
      }, 1000);
    }
    res.json({ success: true });
  });

  // 7. End or Cancel In-App Emergency Call
  app.post('/api/emergency/call/end', (req: Request, res: Response) => {
    if (activeEmergencyCall) {
      activeEmergencyCall.status = 'ended';
      activeEmergencyCall.updatedAt = Date.now();
      callHistory.unshift({ ...activeEmergencyCall });
      if (callHistory.length > 20) callHistory.pop();

      broadcastCallUpdate('call_ended', activeEmergencyCall);
      activeEmergencyCall = null;
    }
    // Also clear active danger alert and signals when emergency call ends!
    activeDangerAlert = null;
    broadcastCallUpdate('danger_alert_cleared', { timestamp: Date.now() });
    webrtcSignals.length = 0;
    res.json({ success: true });
  });

  // 8. WebRTC Signaling Relay for Real-Time Peer Audio Call
  app.post('/api/emergency/webrtc/signal', (req: Request, res: Response) => {
    const { callId, sender, type, payload } = req.body;
    const signal: WebRtcSignal = {
      callId,
      sender,
      type,
      payload,
      timestamp: Date.now(),
    };
    webrtcSignals.push(signal);
    if (webrtcSignals.length > 50) webrtcSignals.shift();

    broadcastCallUpdate('webrtc_signal', signal);
    res.json({ success: true });
  });

  app.get('/api/emergency/webrtc/signals', (req: Request, res: Response) => {
    const callId = req.query.callId as string;
    const after = Number(req.query.after || 0);
    const relevant = webrtcSignals.filter(
      (s) => (!callId || s.callId === callId) && s.timestamp > after
    );
    res.json({ signals: relevant });
  });

  // --- Frontend Integration ---
  if (!isProd) {
    const { createServer: createViteServer } = await import('vite');
    const vite = await createViteServer({
      server: {
        middlewareMode: true,
        hmr: isHmrDisabled ? false : true,
        watch: isHmrDisabled ? null : {},
      },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.resolve(process.cwd(), 'dist');
    if (fs.existsSync(distPath)) {
      app.use(express.static(distPath));
      app.get('*', (_req: Request, res: Response) => {
        res.sendFile(path.join(distPath, 'index.html'));
      });
    } else {
      app.get('*', (_req: Request, res: Response) => {
        res.status(200).send('SafeHer Server is running.');
      });
    }
  }

  server.listen(PORT, '0.0.0.0', () => {
    console.log(`[SafeHer Server] In-App Emergency Calling running on http://0.0.0.0:${PORT}`);
  });
}

startServer().catch((err) => {
  console.error('[SafeHer Server] Startup error:', err);
  process.exit(1);
});
