import { useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  ApiError,
  checkRepositoryEligibility,
  getRepositories,
  getRepositoryCredentials,
  registerRepository,
  updateRepositoryDefaults,
  type RepositoryCredential,
  type RepositoryProfileId,
  type RepositoryProvider,
  type RepositoryRegistration,
} from "../api/client";
import { ErrorDisplay } from "./ErrorDisplay";

const repositoriesKey = ["repositories"] as const;
const credentialsKey = ["repository-credentials"] as const;
const profileOptions: Array<{ value: RepositoryProfileId; label: string }> = [
  { value: "python-pytest", label: "Python · pytest" },
  { value: "node-npm", label: "Node.js · npm test" },
];
const providerOptions: Array<{ value: RepositoryProvider | "none"; label: string }> = [
  { value: "none", label: "No default provider" },
  { value: "openai", label: "OpenAI" },
  { value: "anthropic", label: "Anthropic" },
];

function isUnauthenticated(error: unknown): boolean {
  return error instanceof ApiError && error.status === 401;
}

function formatDate(value: string, options: Intl.DateTimeFormatOptions): string {
  const date = new Date(value);
  return Number.isNaN(date.valueOf()) ? value : new Intl.DateTimeFormat(undefined, options).format(date);
}

function canRetry(error: unknown): boolean {
  return error instanceof ApiError && error.body.retryable;
}

function SignInNotice() {
  return (
    <section className="repository-signin" aria-labelledby="repository-signin-title">
      <h2 id="repository-signin-title">Sign in with GitHub to manage repositories</h2>
      <p>Repository settings belong to your connected GitHub account.</p>
      <a className="text-button" href="/auth/github/start">Sign in with GitHub</a>
    </section>
  );
}

function ProviderSelect({
  id,
  value,
  onChange,
}: {
  id: string;
  value: RepositoryProvider | "none";
  onChange: (value: RepositoryProvider | "none") => void;
}) {
  return (
    <select id={id} value={value} onChange={(event) => onChange(event.target.value as RepositoryProvider | "none")}>
      {providerOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
    </select>
  );
}

function CredentialSelect({
  id,
  provider,
  credentials,
  value,
  onChange,
}: {
  id: string;
  provider: RepositoryProvider | "none";
  credentials: RepositoryCredential[];
  value: string;
  onChange: (value: string) => void;
}) {
  const matching = provider === "none" ? [] : credentials.filter((credential) =>
    credential.provider === provider && credential.status === "active" &&
    (!credential.expires_at || Date.parse(credential.expires_at) > Date.now()),
  );
  return (
    <select id={id} value={value} onChange={(event) => onChange(event.target.value)} disabled={provider === "none"}>
      <option value="">{provider === "none" ? "Choose a provider first" : matching.length ? "No saved credential" : `No active ${provider} credentials`}</option>
      {matching.map((credential) => (
        <option key={credential.credential_id} value={credential.credential_id}>
          {credential.credential_id.slice(0, 8)}… · added {formatDate(credential.created_at, { dateStyle: "medium" })}
        </option>
      ))}
    </select>
  );
}

function eligibleCredential(credentials: RepositoryCredential[], provider: RepositoryProvider | "none", credentialId: string): string | undefined {
  if (provider === "none" || !credentialId) return undefined;
  const credential = credentials.find((item) => item.credential_id === credentialId && item.provider === provider && item.status === "active");
  return credential ? credentialId : undefined;
}

function RepositoryCard({
  repository,
  credentials,
}: {
  repository: RepositoryRegistration;
  credentials: RepositoryCredential[];
}) {
  const queryClient = useQueryClient();
  const [profile, setProfile] = useState(repository.profile_id);
  const [provider, setProvider] = useState<RepositoryProvider | "none">(repository.provider ?? "none");
  const [credentialId, setCredentialId] = useState(repository.credential_id ?? "");
  const [commitSha, setCommitSha] = useState("");
  const shaRef = useRef<HTMLInputElement>(null);
  const [shaError, setShaError] = useState("");
  const [notice, setNotice] = useState("");
  const saveMutation = useMutation({
    mutationFn: () => updateRepositoryDefaults(repository.id, {
      profile_id: profile,
      provider: provider === "none" ? null : provider,
      credential_id: eligibleCredential(credentials, provider, credentialId) ?? null,
    }),
    onSuccess: async () => {
      setNotice("Repository defaults saved.");
      await queryClient.invalidateQueries({ queryKey: repositoriesKey });
    },
  });
  const checkMutation = useMutation({
    mutationFn: () => checkRepositoryEligibility(repository.id, commitSha),
    onSuccess: async () => {
      setNotice("Commit check finished. Review the eligibility result below.");
      setCommitSha("");
      setShaError("");
      await queryClient.invalidateQueries({ queryKey: repositoriesKey });
    },
    onError: async () => {
      // Preflight failures are persisted by the API before it returns an error response.
      await queryClient.invalidateQueries({ queryKey: repositoriesKey });
    },
  });
  const savingError = isUnauthenticated(saveMutation.error) || isUnauthenticated(checkMutation.error);

  function submitCheck(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!/^(?:[0-9a-f]{40}|[0-9a-f]{64})$/.test(commitSha)) {
      setShaError("Enter a full 40- or 64-character lowercase commit SHA.");
      shaRef.current?.focus();
      return;
    }
    setShaError("");
    setNotice("");
    checkMutation.mutate();
  }

  return (
    <article className="repository-card" aria-labelledby={`repository-${repository.id}-title`}>
      <div className="repository-card-heading">
        <div>
          <h3 id={`repository-${repository.id}-title`}>{repository.full_name}</h3>
          <p>GitHub repository ID <code>{repository.github_repository_id}</code></p>
        </div>
        <span className={`eligibility-badge eligibility-${repository.eligibility.status}`}>
          {repository.eligibility.status === "unchecked" ? "Not checked" : repository.eligibility.status === "eligible" ? "Eligible" : repository.eligibility.status === "error" ? "Check error" : "Ineligible"}
        </span>
      </div>

      <form className="repository-defaults" onSubmit={(event) => { event.preventDefault(); setNotice(""); saveMutation.mutate(); }}>
        <fieldset disabled={saveMutation.isPending || checkMutation.isPending}>
          <legend>Defaults for future tasks</legend>
          <div className="repository-form-grid">
            <div className="field-group">
              <label htmlFor={`repository-${repository.id}-profile`}>Test profile</label>
              <select id={`repository-${repository.id}-profile`} value={profile} onChange={(event) => setProfile(event.target.value as RepositoryProfileId)}>
                {profileOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
              </select>
            </div>
            <div className="field-group">
              <label htmlFor={`repository-${repository.id}-provider`}>Default provider</label>
              <ProviderSelect id={`repository-${repository.id}-provider`} value={provider} onChange={(next) => { setProvider(next); if (next === "none" || next !== provider) setCredentialId(""); }} />
            </div>
            <div className="field-group repository-credential-field">
              <label htmlFor={`repository-${repository.id}-credential`}>Saved credential</label>
              <CredentialSelect id={`repository-${repository.id}-credential`} provider={provider} credentials={credentials} value={credentialId} onChange={setCredentialId} />
            </div>
          </div>
        </fieldset>
        <button className="text-button" type="submit" disabled={saveMutation.isPending || checkMutation.isPending}>
          {saveMutation.isPending ? "Saving defaults…" : "Save defaults"}
        </button>
      </form>

      <section className="repository-check" aria-labelledby={`repository-${repository.id}-check-title`}>
        <div>
          <h4 id={`repository-${repository.id}-check-title`}>Check one commit</h4>
          <p>This checks whether the source archive fits the selected test profile. It does not start a task or guarantee future eligibility.</p>
        </div>
        <form className="repository-check-form" onSubmit={submitCheck} noValidate>
          <div className="field-group repository-sha-field">
            <label htmlFor={`repository-${repository.id}-sha`}>Commit SHA</label>
            <input
              ref={shaRef}
              id={`repository-${repository.id}-sha`}
              name={`commit-sha-${repository.id}`}
              type="text"
              inputMode="text"
              autoComplete="off"
              spellCheck={false}
              placeholder="40 or 64 lowercase hex characters…"
              value={commitSha}
              aria-invalid={shaError ? "true" : undefined}
              aria-describedby={shaError ? `repository-${repository.id}-sha-error` : undefined}
              onChange={(event) => setCommitSha(event.target.value)}
              disabled={checkMutation.isPending || saveMutation.isPending}
            />
            {shaError ? <span className="field-error" id={`repository-${repository.id}-sha-error`} role="alert">{shaError}</span> : null}
          </div>
          <button className="text-button" type="submit" disabled={checkMutation.isPending || saveMutation.isPending}>
            {checkMutation.isPending ? "Checking commit…" : "Check eligibility"}
          </button>
        </form>
        {repository.eligibility.checked_sha ? (
          <dl className="eligibility-facts">
            <div><dt>Checked commit</dt><dd><code>{repository.eligibility.checked_sha}</code></dd></div>
            {repository.eligibility.checked_at ? <div><dt>Checked</dt><dd><time dateTime={repository.eligibility.checked_at}>{formatDate(repository.eligibility.checked_at, { dateStyle: "medium", timeStyle: "short" })}</time></dd></div> : null}
          </dl>
        ) : null}
        {repository.eligibility.error ? <p className="eligibility-error">Check result: <code>{repository.eligibility.error}</code></p> : null}
        {repository.eligibility.status === "ineligible" || repository.eligibility.status === "error" ? <p className="eligibility-followup">A failed check stays marked here. Fix the reported issue, then check the commit again to update its eligibility.</p> : null}
      </section>
      {notice ? <p className="repository-notice" role="status">{notice}</p> : null}
      {savingError ? <SignInNotice /> : null}
      {saveMutation.isError && !savingError ? <>
        <ErrorDisplay error={saveMutation.error} title="Could not save repository defaults" />
        {canRetry(saveMutation.error) ? <button className="text-button" type="button" onClick={() => saveMutation.mutate()}>Retry saving defaults</button> : null}
      </> : null}
      {checkMutation.isError && !savingError ? <>
        <ErrorDisplay error={checkMutation.error} title="Could not check this commit" />
        {canRetry(checkMutation.error) ? <button className="text-button" type="button" onClick={() => checkMutation.mutate()}>Retry checking commit</button> : null}
      </> : null}
    </article>
  );
}

export function RepositoriesPage() {
  const queryClient = useQueryClient();
  const repositoriesQuery = useQuery({
    queryKey: repositoriesKey,
    queryFn: ({ signal }) => getRepositories(signal),
    retry: false,
  });
  const credentialsQuery = useQuery({
    queryKey: credentialsKey,
    queryFn: ({ signal }) => getRepositoryCredentials(signal),
    retry: false,
  });
  const [githubId, setGithubId] = useState("");
  const [profile, setProfile] = useState<RepositoryProfileId>("python-pytest");
  const [provider, setProvider] = useState<RepositoryProvider | "none">("none");
  const [credentialId, setCredentialId] = useState("");
  const [formError, setFormError] = useState("");
  const [notice, setNotice] = useState("");
  const githubIdRef = useRef<HTMLInputElement>(null);
  const registerMutation = useMutation({
    mutationFn: (input: { github_repository_id: number; profile_id: RepositoryProfileId; provider?: RepositoryProvider; credential_id?: string }) => registerRepository(input),
    onSuccess: async () => {
      setGithubId("");
      setNotice("Repository registered. Check a commit separately when you are ready.");
      await queryClient.invalidateQueries({ queryKey: repositoriesKey });
    },
  });
  const credentials = credentialsQuery.data?.credentials ?? [];
  const queryUnauthenticated = isUnauthenticated(repositoriesQuery.error) || isUnauthenticated(credentialsQuery.error);
  const registerUnauthenticated = isUnauthenticated(registerMutation.error);

  function submitRegistration(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const parsed = Number(githubId);
    if (!/^[1-9][0-9]*$/.test(githubId) || !Number.isSafeInteger(parsed)) {
      setFormError("Enter a positive whole-number GitHub repository ID.");
      githubIdRef.current?.focus();
      return;
    }
    setFormError("");
    setNotice("");
    const input: { github_repository_id: number; profile_id: RepositoryProfileId; provider?: RepositoryProvider; credential_id?: string } = {
      github_repository_id: parsed,
      profile_id: profile,
    };
    if (provider !== "none") {
      input.provider = provider;
      const selectedCredential = eligibleCredential(credentials, provider, credentialId);
      if (selectedCredential) input.credential_id = selectedCredential;
    }
    registerMutation.mutate(input);
  }

  const loading = repositoriesQuery.isPending || credentialsQuery.isPending;
  const loadError = repositoriesQuery.isError || credentialsQuery.isError;

  return (
    <main id="main-content" tabIndex={-1} className="page-shell repositories-page">
      <section className="repositories-intro" aria-labelledby="repositories-title">
        <p className="eyebrow">Settings</p>
        <h1 id="repositories-title">Repositories</h1>
        <p className="intro-copy">Connect a GitHub repository and choose the test profile and provider defaults for future tasks. Repository registration does not start a task.</p>
      </section>

      {queryUnauthenticated ? <SignInNotice /> : null}
      {!queryUnauthenticated ? (
        <>
          <section className="repository-register-section" aria-labelledby="register-repository-title">
            <div className="repository-section-heading">
              <div><p className="eyebrow">GitHub</p><h2 id="register-repository-title">Register a repository</h2></div>
            </div>
            <p className="repository-section-copy">Use the numeric repository ID from GitHub. Choose the test profile and optional defaults; saved credentials are selected by metadata only.</p>
            <aside className="repository-data-note" aria-label="How task data is handled">
              <p>When you submit a task, its source and instructions go to the provider you choose. Authorized service administrators can access task data for support. Runs can stay active for up to 7 days; access to task content expires 7 days after a run ends. Provider retention rules vary.</p>
              <p>Read <a href="https://developers.openai.com/api/docs/guides/your-data" target="_blank" rel="noreferrer">OpenAI API data controls</a> and <a href="https://platform.claude.com/docs/en/manage-claude/api-and-data-retention" target="_blank" rel="noreferrer">Anthropic API data retention</a>.</p>
            </aside>
            <form className="repository-register-form" onSubmit={submitRegistration} noValidate>
              <div className="repository-form-grid">
                <div className="field-group">
                  <label htmlFor="github-repository-id">GitHub repository ID</label>
                  <input
                    ref={githubIdRef}
                    id="github-repository-id"
                    name="github-repository-id"
                    type="text"
                    inputMode="numeric"
                    autoComplete="off"
                    spellCheck={false}
                    placeholder="123456789…"
                    value={githubId}
                    aria-invalid={formError ? "true" : undefined}
                    aria-describedby={formError ? "github-repository-id-error" : undefined}
                    onChange={(event) => setGithubId(event.target.value)}
                    disabled={registerMutation.isPending}
                  />
                  {formError ? <span className="field-error" id="github-repository-id-error" role="alert">{formError}</span> : null}
                </div>
                <div className="field-group">
                  <label htmlFor="new-repository-profile">Test profile</label>
                  <select id="new-repository-profile" value={profile} onChange={(event) => setProfile(event.target.value as RepositoryProfileId)} disabled={registerMutation.isPending}>
                    {profileOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
                  </select>
                </div>
                <div className="field-group">
                  <label htmlFor="new-repository-provider">Default provider</label>
                  <ProviderSelect id="new-repository-provider" value={provider} onChange={(next) => { setProvider(next); if (next === "none" || next !== provider) setCredentialId(""); }} />
                </div>
                <div className="field-group">
                  <label htmlFor="new-repository-credential">Saved credential</label>
                  <CredentialSelect id="new-repository-credential" provider={provider} credentials={credentials} value={credentialId} onChange={setCredentialId} />
                  <span className="field-hint">Credential keys stay in the vault; this list shows metadata only.</span>
                </div>
              </div>
              <button className="text-button repository-primary-action" type="submit" disabled={registerMutation.isPending}>
                {registerMutation.isPending ? "Registering repository…" : "Register repository"}
              </button>
            </form>
            {registerUnauthenticated ? <SignInNotice /> : null}
            {registerMutation.isError && !registerUnauthenticated ? <>
              <ErrorDisplay error={registerMutation.error} title="Could not register repository" />
              {canRetry(registerMutation.error) && registerMutation.variables ? <button className="text-button" type="button" onClick={() => registerMutation.mutate(registerMutation.variables)}>Retry registering repository</button> : null}
            </> : null}
            {notice ? <p className="repository-notice" role="status">{notice}</p> : null}
          </section>

          <section className="repository-list-section" aria-labelledby="registered-repositories-title">
            <div className="repository-section-heading">
              <div><p className="eyebrow">Your account</p><h2 id="registered-repositories-title">Registered repositories</h2></div>
              <button className="text-button" type="button" onClick={() => void Promise.all([repositoriesQuery.refetch(), credentialsQuery.refetch()])} disabled={repositoriesQuery.isFetching || credentialsQuery.isFetching}>
                {repositoriesQuery.isFetching || credentialsQuery.isFetching ? "Refreshing…" : "Refresh list"}
              </button>
            </div>
            {credentialsQuery.isError && !isUnauthenticated(credentialsQuery.error) ? <ErrorDisplay error={credentialsQuery.error} title="Could not load saved credential metadata" /> : null}
            {loading ? <p className="repository-message" role="status">Loading repositories and saved credential metadata…</p> : null}
            {loadError && !queryUnauthenticated ? (
              <div className="repository-load-error">
                <ErrorDisplay error={repositoriesQuery.error ?? credentialsQuery.error} title="Could not load repository settings" />
                <button className="text-button" type="button" onClick={() => void Promise.all([repositoriesQuery.refetch(), credentialsQuery.refetch()])}>Retry loading settings</button>
              </div>
            ) : null}
            {repositoriesQuery.data && repositoriesQuery.data.repositories.length === 0 ? (
              <p className="repository-message">No repositories are registered yet. Add a numeric GitHub repository ID above to set up future tasks.</p>
            ) : null}
            {repositoriesQuery.data && repositoriesQuery.data.repositories.length > 0 ? (
              <ul className="repository-list" aria-label="Registered repositories">
                {repositoriesQuery.data.repositories.map((repository) => <li key={repository.id}><RepositoryCard repository={repository} credentials={credentials} /></li>)}
              </ul>
            ) : null}
          </section>
        </>
      ) : null}
    </main>
  );
}
