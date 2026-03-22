import { z } from "zod";

export const eventsSearchSchema = z.object({
  startDate: z.string().min(1, "startDate is required"),
  endDate: z.string().min(1, "endDate is required"),
  types: z.array(z.string()).optional(),
  bbox: z.array(z.number()).length(4).optional(),
  polygon: z.array(z.array(z.number()).length(2)).optional(),
  limit: z.number().int().positive().max(500).optional(),
  offset: z.number().int().nonnegative().optional(),
});

export const agentQuerySchema = z.object({
  query: z.string().min(1, "Query is required"),
  apiKey: z.string().optional(),
  beemapsApiKey: z.string().optional(),
});

export const analyzeSchema = z.object({
  eventId: z.string().min(1, "eventId is required"),
  anthropicApiKey: z.string().optional(),
  beemapsApiKey: z.string().optional(),
  mapboxToken: z.string().optional(),
  forceRefresh: z.boolean().optional(),
});

export const pipelineRunCreateSchema = z.object({
  day: z.string().regex(/^\d{4}-\d{2}-\d{2}$/, "day must be YYYY-MM-DD"),
  batchSize: z.number().int().positive().max(500),
  beeMapsApiKey: z.string().min(1, "Bee Maps API key is required"),
  modelName: z.string().min(1).optional(),
});

export const visionScanSchema = z.object({
  query: z.string().min(1),
  events: z
    .array(
      z.object({
        eventId: z.string().min(1),
        lat: z.number(),
        lon: z.number(),
        eventType: z.string().optional(),
      })
    )
    .min(1)
    .max(50),
  model: z.union([z.literal("sonnet"), z.literal("haiku")]),
  anthropicApiKey: z.string().optional(),
  mapboxToken: z.string().optional(),
});

export const detectActorsSchema = z.object({
  eventId: z.string().min(1, "eventId is required"),
  videoUrl: z.string().min(1, "videoUrl is required"),
  timestamp: z.number(),
  cameraLat: z.number(),
  cameraLon: z.number(),
  cameraBearing: z.number(),
  fovDegrees: z.number(),
  cameraIntrinsics: z
    .object({
      focal: z.number(),
      k1: z.number(),
      k2: z.number(),
    })
    .optional(),
  anthropicApiKey: z.string().optional(),
});
