import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import { PlanetScaleAPIError } from "../lib/planetscale-api.ts";
import { getAuthToken, getAuthHeader } from "../lib/auth.ts";

const API_BASE = "https://api.planetscale.com/v1";

interface SchemaRecommendationsResponse {
  data: Record<string, unknown>[];
  current_page: number;
  next_page: number | null;
  next_page_url: string | null;
  prev_page: number | null;
  prev_page_url: string | null;
}

/**
 * Fetch schema recommendations from the PlanetScale API with pagination
 */
async function fetchSchemaRecommendations(
  organization: string,
  database: string,
  state: string | undefined,
  page: number,
  perPage: number,
  authHeader: string
): Promise<SchemaRecommendationsResponse> {
  const params = new URLSearchParams();
  params.set("page", String(page));
  params.set("per_page", String(perPage));
  if (state) {
    params.set("state", state);
  }

  const url = `${API_BASE}/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/schema-recommendations?${params.toString()}`;

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
        "Schema recommendations not found. Please check your organization and database names.",
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
      `Failed to fetch schema recommendations: ${response.statusText}`,
      response.status,
      details
    );
  }

  return (await response.json()) as SchemaRecommendationsResponse;
}

export const listSchemaRecommendationsGram = new Gram().tool({
  name: "list_schema_recommendations",
  description:
    "List schema recommendations for a PlanetScale database. Schema recommendations suggest improvements such as missing indexes, redundant indexes, or other optimizations. Results can be filtered by state and are paginated.",
  inputSchema: {
    organization: z.string().describe("PlanetScale organization name"),
    database: z.string().describe("Database name"),
    state: z
      .string()
      .optional()
      .describe("Filter recommendations by state: 'open' or 'closed'"),
    page: z
      .number()
      .optional()
      .describe("Page number for pagination (default: 1)"),
    per_page: z
      .number()
      .optional()
      .describe("Number of results per page (default: 25)"),
  },
  async execute(ctx, input) {
    try {
      const env =
        Object.keys(ctx.env).length > 0
          ? (ctx.env as Record<string, string | undefined>)
          : process.env;

      const auth = getAuthToken(env);
      if (!auth) {
        return ctx.text("Error: No PlanetScale authentication configured.");
      }

      const organization = input["organization"];
      const database = input["database"];

      if (!organization || !database) {
        return ctx.text("Error: organization and database are required");
      }

      const page = input["page"] ?? 1;
      const perPage = input["per_page"] ?? 25;
      const state = input["state"];
      const authHeader = getAuthHeader(env);

      const response = await fetchSchemaRecommendations(
        organization,
        database,
        state,
        page,
        perPage,
        authHeader
      );

      return ctx.json({
        organization,
        database,
        data: response.data,
        pagination: {
          current_page: response.current_page,
          next_page: response.next_page,
          next_page_url: response.next_page_url,
          prev_page: response.prev_page,
          prev_page_url: response.prev_page_url,
        },
        total: response.data.length,
      });
    } catch (error) {
      if (error instanceof PlanetScaleAPIError) {
        return ctx.text(
          `Error: ${error.message} (status: ${error.statusCode})`
        );
      }

      if (error instanceof Error) {
        return ctx.text(`Error: ${error.message}`);
      }

      return ctx.text("Error: An unexpected error occurred");
    }
  },
});
