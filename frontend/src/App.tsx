import { useQuery } from "@tanstack/react-query";
import { Link, Route, Routes, useLocation } from "react-router";
import { useEffect, useRef } from "react";
import { getLiveness, getReadiness } from "./api/client";
import { RunDetailPage } from "./components/RunDetailPage";
import { ReviewPage } from "./components/ReviewPage";
import { ServiceCheck } from "./components/ServiceCheck";
import { RepositoriesPage } from "./components/RepositoriesPage";
import { OperatorPage } from "./components/OperatorPage";

function FoundationPage() {
  const live = useQuery({
    queryKey: ["health", "live"],
    queryFn: getLiveness,
    retry: false,
    refetchInterval: 30_000,
  });
  const ready = useQuery({
    queryKey: ["health", "ready"],
    queryFn: getReadiness,
    retry: false,
    refetchInterval: 30_000,
  });

  return (
    <main id="main-content" tabIndex={-1} className="page-shell">
      <section className="intro" aria-labelledby="page-title">
        <p className="eyebrow">Developer service</p>
        <h1 id="page-title">The foundation is taking shape.</h1>
        <p className="intro-copy">
          Continumm is building a service that helps developers turn bounded coding tasks into
          reviewed pull requests. The public service has not launched.
        </p>
      </section>

      <section className="status-section" aria-labelledby="status-title">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Current service</p>
            <h2 id="status-title">Live checks</h2>
          </div>
          <span className="refresh-note">Refreshes every 30 seconds</span>
        </div>
        <div className="checks-grid">
          <ServiceCheck label="Process" query={live} />
          <ServiceCheck label="Readiness" query={ready} />
        </div>
      </section>

      <footer className="page-footer">
        <span>Continumm</span>
        <span>Foundation status · {new Date().getFullYear()}</span>
      </footer>
    </main>
  );
}

function UnknownPage() {
  return (
    <main id="main-content" tabIndex={-1} className="page-shell not-found">
      <p className="eyebrow">Page not found</p>
      <h1>This page is not part of the current foundation.</h1>
      <Link to="/" className="text-link">
        Return to service status
      </Link>
    </main>
  );
}

export function App() {
  const { pathname } = useLocation();
  const previousPath = useRef(pathname);
  useEffect(() => {
    const title = pathname.endsWith("/review") ? "Candidate review"
      : pathname.startsWith("/runs/") ? "Run"
      : pathname === "/settings/repositories" ? "Repositories"
      : pathname === "/operations" ? "Operations"
      : pathname === "/" ? "Service status" : "Page not found";
    document.title = `${title} · Continumm`;
    if (previousPath.current !== pathname) {
      document.getElementById("main-content")?.focus();
      previousPath.current = pathname;
    }
  }, [pathname]);
  return (
    <div className="app-frame">
      <a className="skip-link" href="#main-content">Skip to main content</a>
      <header className="app-header">
        <Link to="/" className="brand">
          Continumm
        </Link>
        <nav className="app-navigation" aria-label="Main navigation">
          <Link to="/settings/repositories">Repositories</Link>
          <span className="not-launched">Foundation · Not launched</span>
        </nav>
      </header>
      <Routes>
        <Route path="/" element={<FoundationPage />} />
        <Route path="/settings/repositories" element={<RepositoriesPage />} />
        <Route path="/operations" element={<OperatorPage />} />
        <Route path="/runs/:runId" element={<RunDetailPage />} />
        <Route path="/runs/:runId/review" element={<ReviewPage />} />
        <Route path="*" element={<UnknownPage />} />
      </Routes>
    </div>
  );
}
