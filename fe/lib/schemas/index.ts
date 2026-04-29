import { z } from 'zod';

export const countrySchema = z.object({
  country_code: z.string(),
  country_name: z.string(),
  region: z.string().optional(),
});

export const anomalySchema = z.object({
  country_code: z.string(),
  country_name: z.string(),
  year: z.number(),
  indicator: z.string(),
  actual_value: z.number().nullable(),
  anomaly_score: z.number().nullable(),
});

export const clusterSchema = z.object({
  year: z.number(),
  country_code: z.string(),
  cluster_id: z.number(),
  method: z.string(),
});

export const parseArray = <T>(schema: z.ZodType<T>, data: unknown): T[] => {
  if (!Array.isArray(data)) throw new Error('API response is not an array');
  return data.map((item) => schema.parse(item));
};