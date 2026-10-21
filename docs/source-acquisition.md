# Exact admitted source acquisition

`GitHubSourceFetcher` is the read-only source adapter for
`SourcePreparationActivityHandler`. It uses the admitted repository and full
commit SHA; it does not select a branch head or accept a browser-supplied URL.
The owner identity and current execution lease are checked before fresh GitHub
user/App authorization supplies a repository-scoped read token.

GitHub's [tar archive endpoint](https://docs.github.com/en/rest/repos/contents#download-a-repository-archive-tar)
returns a redirect. The adapter sends the token only to `api.github.com` and
requires a matching HTTPS `codeload.github.com` destination for the exact
repository and SHA. The download uses a separate client without authorization
or cookies. Automatic redirects and environment proxy settings are disabled.
An unexpected status or destination fails closed; there is no download retry.

The default remote-operation deadline is 45 seconds. Downloads are capped at
10 MiB compressed, with a separate `MAX_TAR_BYTES` expansion cap, 50 MiB of
source content, 5,000 files and 6,000 archive entries. Archive
parsing runs off the event loop and never extracts files to disk. The parser
requires a single root, bounded entries and source bytes, regular source files,
and safe relative paths. Links, sparse files and device entries are rejected.
Git's executable bit is preserved while archive permissions are normalized to
`0644` or `0755` for the canonical source checkpoint.

Acquisition alone does not authorize a baseline. The source-preparation handler
independently verifies the complete Git tree, manifest blobs and supported
Python/Node profile against the immutable eligibility receipt before committing
the checkpoint under the current lease. If Git archive attributes omit or alter
tracked content, the tree comparison rejects it. The adapter does not weaken that
comparison or substitute its downloaded file list for the admitted tree.

This is a server-side adapter. The explicit `--execute-workflow` mode registers the source/model/tool workflow;
`--execute-publication` remains restricted to approved publication. Local fixture tests exercise its
HTTP and archive boundaries; they do not establish a live authenticated GitHub
run or complete the deferred T34 demonstration.
