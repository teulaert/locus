# Security assumptions

* The transport is the WireGuard link, and on top of it peers authenticate
  each other: each install has an ed25519 identity, every sync request carries
  a signature over method, path and query, timestamp and body hash (for a
  put, the declared content hash, so it is checked before the body is read),
  and `authorized_peers` can pin which peers are discovered and served.
* Files are not encrypted at rest; on-device confidentiality is the operating
  system's job.
* Paths are strictly validated against configured roots — `..`, absolute paths
  and symlink escapes are rejected for both sync and API operations.
* Uploads are size-capped, hash-verified and installed atomically.
* Pushes of files of 1 MiB or more are resumable: an interrupted put leaves a
  partial object in `<data_dir>/partials` (keyed by content hash, never inside
  a root), the sender asks `GET /v1/put/status?hash=` how much arrived and
  continues with `offset=N` in the signed put query. The size cap applies to
  the whole object across continuations, the complete object is re-hashed
  before it is installed (a mismatch discards the partial), and partials
  untouched for 24 hours are removed. Peers without the status endpoint get a
  plain full put, as before. See [LOCUS_DESIGN.md](../LOCUS_DESIGN.md),
  "Resumable puts".
* Received content (pushed to this peer or pulled by it) is staged in the
  data directory and verified before the file's lock is taken, so a peer that
  stalls mid-transfer cannot block other transfers of the file. A transfer
  that moves no bytes for 60 seconds is abandoned on either side.
* Binaries are installed only from releases signed with the release key
  compiled into the running binary, which only CI holds, and only when they
  match the signed manifest; see [Keeping peers up to
  date](updates.md#keeping-peers-up-to-date).
* The API is token-authenticated and loopback-only unless `api.addr` says
  otherwise; the token is generated on first start and stored in the data
  directory.
