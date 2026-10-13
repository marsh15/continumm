The image copies `continuum_api.artifacts.archives` here as a standalone,
stdlib-only verifier. It is imported only by `/opt/continuum/node_bootstrap.py`
using this fixed, root-owned path while the trusted base Python runs with
`-I -S`. Never copy repository content, npm packages, or mutable helper files
into this directory.
