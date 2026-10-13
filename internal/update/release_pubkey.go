package update

// ReleasePublicKey is the hex-encoded ed25519 public key whose signature a
// release manifest must carry before any binary it lists is installed
// (LOC-86). Its private half exists only where CI signs releases (a key
// file on the self-hosted release runner, or the LOCUS_RELEASE_KEY
// repository secret); `make release-keygen` creates the pair.
//
// Empty means this build trusts no release: `locus update` refuses to run
// and the daemon never checks for updates. Fill it with the public key that
// `make release-keygen` printed and commit it; every binary built after
// that verifies releases with it.
const ReleasePublicKey = ""
