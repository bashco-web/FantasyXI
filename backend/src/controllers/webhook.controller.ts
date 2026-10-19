import { Request, Response } from "express";
import crypto from "crypto";
import { webhookDispatcher, WebhookEndpoint } from "../services/webhooks/webhookDispatcher.js";
import { webhookRetryEngine } from "../services/webhooks/webhookRetryEngine.js";

// In-memory endpoints registry (backed by DB when available)
const registeredEndpoints: Map<string, WebhookEndpoint> = new Map();

export async function createEndpoint(req: Request, res: Response): Promise<void> {
  try {
    const { url, events, leagueId } = req.body;
    const userId = (req as any).user?.id || "user-anon";

    if (!url || !Array.isArray(events) || events.length === 0) {
      res.status(400).json({ error: "url and non-empty events array are required" });
      return;
    }

    try {
      new URL(url);
    } catch {
      res.status(400).json({ error: "Invalid URL provided" });
      return;
    }

    const endpointId = `ep_${Date.now()}_${crypto.randomBytes(4).toString("hex")}`;
    const secret = `whsec_${crypto.randomBytes(24).toString("hex")}`;

    const endpoint: WebhookEndpoint = {
      id: endpointId,
      url,
      secret,
      leagueId: leagueId || null,
      userId,
      events,
      isActive: true,
    };

    registeredEndpoints.set(endpointId, endpoint);
    webhookRetryEngine.registerEndpoint(endpoint);

    res.status(201).json({
      success: true,
      endpoint: {
        id: endpoint.id,
        url: endpoint.url,
        secret: endpoint.secret,
        events: endpoint.events,
        leagueId: endpoint.leagueId,
        isActive: endpoint.isActive,
      },
    });
  } catch (error: any) {
    res.status(500).json({ error: error.message || "Failed to create webhook endpoint" });
  }
}

export async function listEndpoints(req: Request, res: Response): Promise<void> {
  const leagueId = req.query.leagueId as string | undefined;
  let endpoints = Array.from(registeredEndpoints.values());

  if (leagueId) {
    endpoints = endpoints.filter((ep) => ep.leagueId === leagueId);
  }

  res.json({
    success: true,
    endpoints: endpoints.map((ep) => ({
      id: ep.id,
      url: ep.url,
      events: ep.events,
      leagueId: ep.leagueId,
      isActive: ep.isActive,
    })),
  });
}

export async function testDispatch(req: Request, res: Response): Promise<void> {
  const id = Array.isArray(req.params.id) ? req.params.id[0] : req.params.id;
  const endpoint = registeredEndpoints.get(id);

  if (!endpoint) {
    res.status(404).json({ error: "Webhook endpoint not found" });
    return;
  }

  const samplePayload = {
    test: true,
    message: "FantasyXI Webhook Ping",
    timestamp: new Date().toISOString(),
  };

  const delivery = await webhookRetryEngine.dispatch(endpoint, "ping", samplePayload);
  res.json({ success: true, delivery });
}

export async function listDeliveries(req: Request, res: Response): Promise<void> {
  const id = Array.isArray(req.params.id) ? req.params.id[0] : req.params.id;
  const deliveries = webhookRetryEngine.listDeliveries(id);
  res.json({ success: true, deliveries });
}

export async function getDeadLetterDeliveries(req: Request, res: Response): Promise<void> {
  const endpointId = req.query.endpointId as string | undefined;
  const deadLetters = webhookRetryEngine.getDeadLetterQueue(endpointId);
  res.json({ success: true, deadLetters });
}

export async function retryFailedDelivery(req: Request, res: Response): Promise<void> {
  try {
    const deliveryId = req.body.deliveryId || req.body.id;
    if (!deliveryId) {
      res.status(400).json({ error: "deliveryId is required in request body" });
      return;
    }

    const delivery = await webhookRetryEngine.retryDelivery(deliveryId);
    res.json({ success: true, delivery });
  } catch (error: any) {
    res.status(400).json({ error: error.message || "Failed to retry delivery" });
  }
}

export async function reactivateEndpointController(req: Request, res: Response): Promise<void> {
  try {
    const id = Array.isArray(req.params.id) ? req.params.id[0] : req.params.id;
    const endpoint = webhookRetryEngine.reactivateEndpoint(id);
    if (registeredEndpoints.has(id)) {
      registeredEndpoints.get(id)!.isActive = true;
    }
    res.json({ success: true, endpoint });
  } catch (error: any) {
    res.status(404).json({ error: error.message || "Failed to reactivate endpoint" });
  }
}
