import assert from "node:assert/strict";
import test from "node:test";

import {
  ApiError,
  getAnalysisJob,
  getLatestAnalysisJob,
  getUser,
  getUsers,
  startAnalysis,
} from "../src/api.ts";

test("getUser uses a relative API URL and returns numeric counts", async (t) => {
  const originalFetch = globalThis.fetch;
  t.after(() => { globalThis.fetch = originalFetch; });

  globalThis.fetch = async (url) => {
    assert.equal(url, "/api/users/214328887");
    return Response.json({ user_id: 214328887, followers: 180, followees: 214 });
  };

  assert.deepEqual(await getUser("214328887"), {
    user_id: 214328887,
    followers: 180,
    followees: 214,
  });
});

test("getUsers sends the requested pagination parameters", async (t) => {
  const originalFetch = globalThis.fetch;
  t.after(() => { globalThis.fetch = originalFetch; });

  globalThis.fetch = async (url) => {
    assert.equal(url, "/api/users?limit=12&offset=24");
    return Response.json({ items: [], total: 0, limit: 12, offset: 24 });
  };

  assert.deepEqual(await getUsers(12, 24), { items: [], total: 0, limit: 12, offset: 24 });
});

test("API errors preserve FastAPI's useful detail message", async (t) => {
  const originalFetch = globalThis.fetch;
  t.after(() => { globalThis.fetch = originalFetch; });

  globalThis.fetch = async () =>
    Response.json({ detail: "User 999 not found" }, { status: 404 });

  await assert.rejects(
    getUser("999"),
    (error) => error instanceof ApiError && error.status === 404 && error.message === "User 999 not found",
  );
});

test("analysis submission uses POST and returns the persistent job ID", async (t) => {
  const originalFetch = globalThis.fetch;
  t.after(() => { globalThis.fetch = originalFetch; });

  globalThis.fetch = async (url, init) => {
    assert.equal(url, "/api/analysis/jobs");
    assert.equal(init.method, "POST");
    return Response.json({
      job_id: "abc123",
      status: "queued",
      created_at: "2026-10-14T00:00:00Z",
      started_at: null,
      finished_at: null,
      error: null,
      stats: null,
      duplicate: false,
    }, { status: 202 });
  };

  const result = await startAnalysis();
  assert.equal(result.job_id, "abc123");
  assert.equal(result.status, "queued");
  assert.equal(result.duplicate, false);
});

test("job status helpers use relative URLs", async (t) => {
  const originalFetch = globalThis.fetch;
  t.after(() => { globalThis.fetch = originalFetch; });
  const seen = [];
  globalThis.fetch = async (url) => {
    seen.push(url);
    return Response.json({
      job_id: "abc123",
      status: "running",
      created_at: "2026-10-14T00:00:00Z",
      started_at: "2026-10-14T00:00:01Z",
      finished_at: null,
      error: null,
      stats: null,
    });
  };

  await getLatestAnalysisJob();
  await getAnalysisJob("abc123");
  assert.deepEqual(seen, ["/api/analysis/jobs/latest", "/api/analysis/jobs/abc123"]);
});
