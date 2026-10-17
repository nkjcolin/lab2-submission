import { FormEvent, useEffect, useMemo, useRef, useState } from "react";
import {
  ApiError,
  errorMessage,
  getAnalysisJob,
  getLatestAnalysisJob,
  getUser,
  getUsers,
  startAnalysis,
  type AnalysisJob,
  type UserCounts,
  type UserPage,
} from "./api";

const FEATURED_USER_IDS = ["214328887", "107830991"] as const;
const PAGE_SIZE = 12;

type Loadable<T> =
  | { status: "loading" }
  | { status: "ready"; data: T }
  | { status: "error"; message: string };

type LookupState =
  | { status: "idle" }
  | { status: "loading" }
  | { status: "ready"; data: UserCounts }
  | { status: "error"; message: string };

const numberFormatter = new Intl.NumberFormat("en-US");

function formatNumber(value: number): string {
  return numberFormatter.format(value);
}

function UserCard({ user, featured = false }: { user: UserCounts; featured?: boolean }) {
  return (
    <article className={`user-card${featured ? " user-card--featured" : ""}`}>
      <div className="user-card__header">
        <span className="user-card__eyebrow">User ID</span>
        {featured && <span className="badge">Acceptance check</span>}
      </div>
      <h3>{user.user_id}</h3>
      <p>
        {user.user_id} has <strong>{formatNumber(user.followers)} followers</strong>. {user.user_id} has{" "}
        <strong>{formatNumber(user.followees)} followees</strong>.
      </p>
      <dl className="count-grid">
        <div>
          <dt>Followers</dt>
          <dd>{formatNumber(user.followers)}</dd>
        </div>
        <div>
          <dt>Followees</dt>
          <dd>{formatNumber(user.followees)}</dd>
        </div>
      </dl>
    </article>
  );
}

function SkeletonCard() {
  return (
    <div className="user-card skeleton-card" aria-hidden="true">
      <span />
      <span />
      <span />
    </div>
  );
}

function App() {
  const [featuredUsers, setFeaturedUsers] = useState<Record<string, Loadable<UserCounts>>>(() =>
    Object.fromEntries(FEATURED_USER_IDS.map((id) => [id, { status: "loading" }])),
  );
  const [userId, setUserId] = useState("");
  const [validationError, setValidationError] = useState("");
  const [lookup, setLookup] = useState<LookupState>({ status: "idle" });
  const [pageIndex, setPageIndex] = useState(0);
  const [page, setPage] = useState<Loadable<UserPage>>({ status: "loading" });
  const [dataRevision, setDataRevision] = useState(0);
  const [analysisJob, setAnalysisJob] = useState<AnalysisJob | null>(null);
  const [jobSubmitting, setJobSubmitting] = useState(false);
  const [jobError, setJobError] = useState("");
  const refreshedJob = useRef<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();

    FEATURED_USER_IDS.forEach((id) => {
      getUser(id, controller.signal)
        .then((data) => {
          setFeaturedUsers((current) => ({ ...current, [id]: { status: "ready", data } }));
        })
        .catch((error: unknown) => {
          if (error instanceof DOMException && error.name === "AbortError") return;
          setFeaturedUsers((current) => ({
            ...current,
            [id]: { status: "error", message: errorMessage(error) },
          }));
        });
    });

    return () => controller.abort();
  }, [dataRevision]);

  useEffect(() => {
    const controller = new AbortController();
    setPage({ status: "loading" });
    getUsers(PAGE_SIZE, pageIndex * PAGE_SIZE, controller.signal)
      .then((data) => setPage({ status: "ready", data }))
      .catch((error: unknown) => {
        if (error instanceof DOMException && error.name === "AbortError") return;
        setPage({ status: "error", message: errorMessage(error) });
      });
    return () => controller.abort();
  }, [dataRevision, pageIndex]);

  useEffect(() => {
    const controller = new AbortController();
    getLatestAnalysisJob(controller.signal)
      .then(setAnalysisJob)
      .catch((error: unknown) => {
        if (error instanceof DOMException && error.name === "AbortError") return;
        if (error instanceof ApiError && error.status === 404) return;
        setJobError(errorMessage(error));
      });
    return () => controller.abort();
  }, []);

  useEffect(() => {
    if (!analysisJob || !["queued", "running"].includes(analysisJob.status)) return;
    const controller = new AbortController();
    const poll = () => {
      getAnalysisJob(analysisJob.job_id, controller.signal)
        .then(setAnalysisJob)
        .catch((error: unknown) => {
          if (error instanceof DOMException && error.name === "AbortError") return;
          setJobError(errorMessage(error));
        });
    };
    const timer = window.setInterval(poll, 1000);
    poll();
    return () => {
      controller.abort();
      window.clearInterval(timer);
    };
  }, [analysisJob?.job_id, analysisJob?.status]);

  useEffect(() => {
    if (analysisJob?.status !== "succeeded" || refreshedJob.current === analysisJob.job_id) return;
    refreshedJob.current = analysisJob.job_id;
    setDataRevision((current) => current + 1);
    if (lookup.status === "ready") {
      const displayedUserId = lookup.data.user_id;
      setLookup({ status: "loading" });
      getUser(displayedUserId)
        .then((data) => setLookup({ status: "ready", data }))
        .catch((error: unknown) => setLookup({ status: "error", message: errorMessage(error) }));
    }
  }, [analysisJob]);

  const totalPages = useMemo(() => {
    if (page.status !== "ready") return 0;
    return Math.ceil(page.data.total / PAGE_SIZE);
  }, [page]);

  function handleLookup(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const normalizedId = userId.trim();
    if (!/^\d+$/.test(normalizedId)) {
      setValidationError("Enter a non-negative integer user ID.");
      setLookup({ status: "idle" });
      return;
    }

    setValidationError("");
    setLookup({ status: "loading" });
    getUser(normalizedId)
      .then((data) => setLookup({ status: "ready", data }))
      .catch((error: unknown) => setLookup({ status: "error", message: errorMessage(error) }));
  }

  function retryPage() {
    setPageIndex((current) => current);
    setPage({ status: "loading" });
    getUsers(PAGE_SIZE, pageIndex * PAGE_SIZE)
      .then((data) => setPage({ status: "ready", data }))
      .catch((error: unknown) => setPage({ status: "error", message: errorMessage(error) }));
  }

  async function handleRunAnalysis() {
    setJobSubmitting(true);
    setJobError("");
    try {
      const submission = await startAnalysis();
      setAnalysisJob(submission);
    } catch (error: unknown) {
      setJobError(errorMessage(error));
    } finally {
      setJobSubmitting(false);
    }
  }

  const jobActive = analysisJob?.status === "queued" || analysisJob?.status === "running";
  const jobMessage = analysisJob?.status === "queued"
    ? "Queued — waiting for the private analysis worker."
    : analysisJob?.status === "running"
      ? "Running — PySpark is processing the dataset."
      : analysisJob?.status === "succeeded"
        ? `Succeeded — ${formatNumber(analysisJob.stats?.imported_users ?? 0)} users loaded into SQLite.`
        : analysisJob?.status === "failed"
          ? `Failed — ${analysisJob.error ?? "see the analysis service log"}`
          : "No analysis job has been submitted yet.";

  return (
    <div className="app-shell">
      <header className="hero">
        <nav className="nav" aria-label="Primary navigation">
          <a className="brand" href="#top" aria-label="Twitter Network Explorer home">
            <span className="brand__mark" aria-hidden="true">N</span>
            <span>Network Explorer</span>
          </a>
          <a href="#directory">Browse all users</a>
        </nav>

        <div className="hero__content" id="top">
          <p className="kicker">CS5224 · Directed social graph</p>
          <h1>See the network, one user at a time.</h1>
          <p className="hero__lede">
            Explore precomputed follower and followee counts from a cleaned Twitter network dataset.
          </p>
          <section className="analysis-panel" aria-labelledby="analysis-title">
            <div>
              <p className="analysis-panel__eyebrow">On-demand processing</p>
              <h2 id="analysis-title">Refresh the dataset</h2>
              <p className={`job-status${analysisJob ? ` job-status--${analysisJob.status}` : ""}`} aria-live="polite">
                {jobMessage}
              </p>
              {analysisJob && <code className="job-id">Job {analysisJob.job_id}</code>}
              {jobError && <p className="field-error" role="alert">{jobError}</p>}
            </div>
            <button
              className="run-analysis"
              type="button"
              disabled={jobSubmitting || jobActive}
              onClick={handleRunAnalysis}
            >
              {jobSubmitting ? "Submitting…" : jobActive ? "Analysis in progress" : "Run analysis"}
            </button>
          </section>
          <form className="search-form" onSubmit={handleLookup} noValidate>
            <label htmlFor="user-id">Look up a user ID</label>
            <div className="search-form__controls">
              <input
                id="user-id"
                name="user-id"
                type="text"
                inputMode="numeric"
                pattern="[0-9]+"
                autoComplete="off"
                placeholder="e.g. 214328887"
                value={userId}
                onChange={(event) => {
                  setUserId(event.target.value);
                  if (validationError) setValidationError("");
                }}
                aria-describedby={validationError ? "lookup-error" : "lookup-help"}
                aria-invalid={Boolean(validationError)}
              />
              <button type="submit" disabled={lookup.status === "loading"}>
                {lookup.status === "loading" ? "Looking up…" : "Find user"}
              </button>
            </div>
            <span id="lookup-help" className="form-help">Use any non-negative integer ID.</span>
            {validationError && <span id="lookup-error" className="field-error">{validationError}</span>}
          </form>
        </div>
      </header>

      <main>
        <section className="lookup-result" aria-live="polite" aria-atomic="true">
          {lookup.status === "loading" && <p className="status">Looking up this user…</p>}
          {lookup.status === "error" && (
            <div className="notice notice--error" role="alert">
              <strong>We couldn’t find that user.</strong>
              <span>{lookup.message}</span>
            </div>
          )}
          {lookup.status === "ready" && <UserCard user={lookup.data} />}
        </section>

        <section className="section featured" aria-labelledby="featured-title">
          <div className="section-heading">
            <div>
              <p className="kicker">Required results</p>
              <h2 id="featured-title">Reference users</h2>
            </div>
            <p>These users are loaded directly from the API when the page opens.</p>
          </div>
          <div className="card-grid card-grid--featured" aria-live="polite">
            {FEATURED_USER_IDS.map((id) => {
              const result = featuredUsers[id];
              if (result.status === "loading") return <SkeletonCard key={id} />;
              if (result.status === "error") {
                return (
                  <div className="notice notice--error user-card" role="alert" key={id}>
                    <strong>User {id} could not be loaded.</strong>
                    <span>{result.message}</span>
                  </div>
                );
              }
              return <UserCard user={result.data} featured key={id} />;
            })}
          </div>
        </section>

        <section className="section directory" id="directory" aria-labelledby="directory-title">
          <div className="section-heading">
            <div>
              <p className="kicker">Complete directory</p>
              <h2 id="directory-title">Browse all users</h2>
            </div>
            {page.status === "ready" && (
              <p>{formatNumber(page.data.total)} users, ordered by numeric ID.</p>
            )}
          </div>

          <div aria-live="polite">
            {page.status === "loading" && (
              <div className="card-grid" aria-label="Loading users">
                {Array.from({ length: 6 }, (_, index) => <SkeletonCard key={index} />)}
              </div>
            )}
            {page.status === "error" && (
              <div className="notice notice--error" role="alert">
                <strong>The user directory could not be loaded.</strong>
                <span>{page.message}</span>
                <button className="button-secondary" type="button" onClick={retryPage}>Try again</button>
              </div>
            )}
            {page.status === "ready" && page.data.items.length === 0 && (
              <div className="notice">
                <strong>No users were returned.</strong>
                <span>The analysis results are empty or this page is past the end of the directory.</span>
              </div>
            )}
            {page.status === "ready" && page.data.items.length > 0 && (
              <div className="card-grid">
                {page.data.items.map((user) => <UserCard user={user} key={user.user_id} />)}
              </div>
            )}
          </div>

          {page.status === "ready" && page.data.total > 0 && (
            <nav className="pagination" aria-label="User directory pagination">
              <button
                type="button"
                className="button-secondary"
                disabled={pageIndex === 0}
                onClick={() => setPageIndex((current) => Math.max(0, current - 1))}
              >
                Previous
              </button>
              <span aria-live="polite">Page {pageIndex + 1} of {totalPages}</span>
              <button
                type="button"
                className="button-secondary"
                disabled={pageIndex + 1 >= totalPages}
                onClick={() => setPageIndex((current) => current + 1)}
              >
                Next
              </button>
            </nav>
          )}
        </section>
      </main>

      <footer>
        <span>Twitter Network Explorer</span>
        <span>Counts computed with PySpark · Served from SQLite</span>
      </footer>
    </div>
  );
}

export default App;
