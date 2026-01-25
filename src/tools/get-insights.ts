import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import { PlanetScaleAPIError } from "../lib/planetscale-api.ts";
import { getAuthToken, getAuthHeader } from "../lib/auth.ts";

const API_BASE = "https://api.planetscale.com/v1";

// Available sort metrics for insights
const SORT_METRICS = [
  "totalTime",
  "rowsReadPerReturned",
  "rowsRead",
  "p99Latency",
  "rowsAffected",
] as const;

type SortMetric = (typeof SORT_METRICS)[number];

// Fields to include in the result for token efficiency
const RESULT_FIELDS = [
  "id",
  "normalized_sql",
  "query_count",
  "sum_total_duration_millis",
  "rows_read_per_returned",
  "sum_rows_read",
  "p99_latency",
  "sum_rows_affected",
  "tables",
  "index_usages",
  "keyspace",
  "last_run_at",
] as const;

export interface InsightsEntry {
  id: string;
  normalized_sql?: string;
  query_count?: number;
  sum_total_duration_millis?: number;
  rows_read_per_returned?: number;
  sum_rows_read?: number;
  p99_latency?: number;
  sum_rows_affected?: number;
  tables?: string[];
  index_usages?: unknown[];
  keyspace?: string;
  last_run_at?: string;
}

export interface InsightsResponse {
  data: InsightsEntry[];
}

/**
 * Fetch insights from the PlanetScale API with a specific sort order
 */
async function fetchInsights(
  organization: string,
  database: string,
  branch: string,
  sortBy: SortMetric,
  limit: number,
  authHeader: string
): Promise<InsightsEntry[]> {
  const url = `${API_BASE}/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/branches/${encodeURIComponent(branch)}/insights?per_page=${limit}&sort=${sortBy}&dir=desc`;

  const response = await fetch(url, {
    method: "GET",
    headers: {
      Authorization: authHeader,
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    let details: unknown;
    try {
      details = await response.json();
    } catch {
      details = await response.text();
    }

    if (response.status === 404) {
      throw new PlanetScaleAPIError(
        "Insights not found. Please check your organization, database, and branch names.",
        response.status,
        details
      );
    }

    if (response.status === 401 || response.status === 403) {
      throw new PlanetScaleAPIError(
        "Permission denied. Please check your API token has the required permissions.",
        response.status,
        details
      );
    }

    throw new PlanetScaleAPIError(
      `Failed to fetch insights: ${response.statusText}`,
      response.status,
      details
    );
  }

  const data = (await response.json()) as InsightsResponse;
  return data.data || [];
}

/**
 * Filter an insights entry to only include the fields we want
 */
function filterEntry(entry: InsightsEntry): Partial<InsightsEntry> {
  const filtered: Partial<InsightsEntry> = {};
  for (const field of RESULT_FIELDS) {
    if (field in entry && entry[field as keyof InsightsEntry] !== undefined) {
      (filtered as Record<string, unknown>)[field] = entry[field as keyof InsightsEntry];
    }
  }
  return filtered;
}

export const getInsightsGram = new Gram().tool({
  name: "get_insights",
  description:
    "Get query performance insights for a PlanetScale database branch. By default, aggregates the top queries across 5 different metrics (slowest, most time-consuming, most rows read, most inefficient, most rows affected) for a comprehensive view. Can also fetch queries sorted by a single metric.",
  inputSchema: {
    organization: z.string().describe("PlanetScale organization name"),
    database: z.string().describe("Database name"),
    branch: z.string().describe("Branch name (e.g., 'main')"),
    sort_by: z
      .enum(["all", ...SORT_METRICS])
      .optional()
      .describe(
        "Sort order: 'all' (default) aggregates 5 API calls for comprehensive view, or specify a single metric: 'totalTime', 'rowsRead', 'p99Latency', 'rowsReadPerReturned', 'rowsAffected'"
      ),
    limit: z
      .number()
      .optional()
      .describe("Number of results per metric (default: 5, max: 20)"),
  },
  async execute(ctx, input) {
    try {
      // Try ctx.env first, fall back to process.env for local development
      const env =
        Object.keys(ctx.env).length > 0
          ? (ctx.env as Record<string, string | undefined>)
          : process.env;

      // Check authentication
      const auth = getAuthToken(env);
      if (!auth) {
        return ctx.text("Error: No PlanetScale authentication configured.");
      }

      const organization = input["organization"];
      const database = input["database"];
      const branch = input["branch"];

      if (!organization || !database || !branch) {
        return ctx.text(
          "Error: organization, database, and branch are required"
        );
      }

      const sortBy = input["sort_by"] ?? "all";
      const limit = Math.min(input["limit"] ?? 5, 20); // Cap at 20

      const authHeader = getAuthHeader(env);

      if (sortBy === "all") {
        // Aggregate mode: fetch from all 5 metrics and deduplicate
        const uniqueEntries = new Map<string, Partial<InsightsEntry>>();

        for (const metric of SORT_METRICS) {
          const entries = await fetchInsights(
            organization,
            database,
            branch,
            metric,
            limit,
            authHeader
          );

          for (const entry of entries) {
            if (entry.id && !uniqueEntries.has(entry.id)) {
              uniqueEntries.set(entry.id, filterEntry(entry));
            }
          }
        }

        const results = Array.from(uniqueEntries.values());
        return ctx.json({
          mode: "aggregated",
          metrics_queried: SORT_METRICS,
          limit_per_metric: limit,
          total_unique_queries: results.length,
          queries: results,
        });
      } else {
        // Single metric mode
        const entries = await fetchInsights(
          organization,
          database,
          branch,
          sortBy as SortMetric,
          limit,
          authHeader
        );

        const results = entries.map(filterEntry);
        return ctx.json({
          mode: "single_metric",
          sort_by: sortBy,
          limit,
          total_queries: results.length,
          queries: results,
        });
      }
    } catch (error) {
      if (error instanceof PlanetScaleAPIError) {
        return ctx.text(`Error: ${error.message} (status: ${error.statusCode})`);
      }

      if (error instanceof Error) {
        return ctx.text(`Error: ${error.message}`);
      }

      return ctx.text(`Error: An unexpected error occurred`);
    }
  },
});
