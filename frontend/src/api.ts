export interface UserCounts {
  user_id: number;
  followers: number;
  followees: number;
}

export interface UserPage {
  items: UserCounts[];
  total: number;
  limit: number;
  offset: number;
}

export type AnalysisJobStatus = "queued" | "running" | "succeeded" | "failed";

export interface AnalysisJob {
  job_id: string;
  status: AnalysisJobStatus;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  error: string | null;
  stats: Record<string, number> | null;
}

export interface AnalysisSubmission extends AnalysisJob {
  duplicate: boolean;
}

interface ApiErrorBody {
  detail?: unknown;
}

export class ApiError extends Error {
  readonly status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

async function request<T>(
  path: string,
  signal?: AbortSignal,
  init?: RequestInit,
): Promise<T> {
  let response: Response;

  try {
    response = await fetch(path, { ...init, signal });
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") {
      throw error;
    }
    throw new ApiError("The API could not be reached. Check that the backend is running.", 0);
  }

  if (!response.ok) {
    let message = `The API returned an error (${response.status}).`;
    try {
      const body = (await response.json()) as ApiErrorBody;
      if (typeof body.detail === "string" && body.detail.trim()) {
        message = body.detail;
      }
    } catch {
      // Keep the status-based fallback when the response is not JSON.
    }
    throw new ApiError(message, response.status);
  }

  return (await response.json()) as T;
}

export function getUser(userId: string | number, signal?: AbortSignal): Promise<UserCounts> {
  return request<UserCounts>(`/api/users/${encodeURIComponent(String(userId))}`, signal);
}

export function getUsers(limit: number, offset: number, signal?: AbortSignal): Promise<UserPage> {
  const params = new URLSearchParams({ limit: String(limit), offset: String(offset) });
  return request<UserPage>(`/api/users?${params.toString()}`, signal);
}

export function startAnalysis(signal?: AbortSignal): Promise<AnalysisSubmission> {
  return request<AnalysisSubmission>("/api/analysis/jobs", signal, { method: "POST" });
}

export function getAnalysisJob(jobId: string, signal?: AbortSignal): Promise<AnalysisJob> {
  return request<AnalysisJob>(
    `/api/analysis/jobs/${encodeURIComponent(jobId)}`,
    signal,
  );
}

export function getLatestAnalysisJob(signal?: AbortSignal): Promise<AnalysisJob> {
  return request<AnalysisJob>("/api/analysis/jobs/latest", signal);
}

export function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "An unexpected error occurred.";
}
