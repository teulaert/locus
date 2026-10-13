// Command locus-release generates the release signing key and writes,
// signs and verifies release manifests (LOC-86). CI runs it to publish a
// release; `make release-keygen`, `release-sign` and `release-verify` wrap
// it. It is a separate binary so the locus daemon carries no signing code.
//
//	locus-release keygen
//	locus-release manifest -dir dist -version V -commit SHA -commit-time T -channel main|stable
//	locus-release sign -in dist/manifest.json [-out dist/manifest.sig]
//	locus-release verify -dir dist [-pubkey HEX]
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/teulaert/locus/internal/release"
	"github.com/teulaert/locus/internal/update"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, os.Getenv))
}

const usageText = `usage:
  locus-release keygen
      print a new signing key pair: the public key (hex) for
      internal/update/release_pubkey.go, the private key (base64) for the
      key file or the LOCUS_RELEASE_KEY secret
  locus-release manifest -dir <dist> -version <v> -commit <sha> -commit-time <rfc3339> -channel main|stable
      write <dist>/manifest.json and <dist>/SHA256SUMS for the binaries
      <dist>/locus-<os>-<arch>
  locus-release sign -in <manifest.json> [-out <manifest.sig>]
      sign with the key in $LOCUS_RELEASE_KEY_FILE (a file), else $LOCUS_RELEASE_KEY
  locus-release verify -dir <dist> [-pubkey <hex>]
      verify the signature and every binary, with the key compiled into
      locus unless -pubkey is given
`

func run(args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usageText)
		return 2
	}
	fail := func(err error) int {
		fmt.Fprintln(stderr, "locus-release:", err)
		return 1
	}
	fs := flag.NewFlagSet("locus-release "+args[0], flag.ContinueOnError)
	fs.SetOutput(stderr)
	switch args[0] {
	case "keygen":
		if err := fs.Parse(args[1:]); err != nil {
			return 2
		}
		pub, priv, err := release.GenerateKey()
		if err != nil {
			return fail(err)
		}
		fmt.Fprintf(stdout, "public  %s\nprivate %s\n", pub, priv)
		fmt.Fprintln(stderr, "commit the public key as ReleasePublicKey in internal/update/release_pubkey.go;")
		fmt.Fprintln(stderr, "store the private key (one line, mode 600) in the release runner's key file or the LOCUS_RELEASE_KEY secret, and nowhere else")
		return 0
	case "manifest":
		dir := fs.String("dir", "dist", "directory holding the locus-<os>-<arch> binaries")
		version := fs.String("version", "", "version string the binaries report")
		commit := fs.String("commit", "", "full commit hash")
		commitTime := fs.String("commit-time", "", "commit time, RFC 3339")
		channel := fs.String("channel", update.ChannelMain, "release channel: main or stable")
		if err := fs.Parse(args[1:]); err != nil {
			return 2
		}
		ct, err := time.Parse(time.RFC3339, *commitTime)
		if err != nil {
			return fail(fmt.Errorf("-commit-time: %w", err))
		}
		m, data, err := release.BuildManifest(*dir, release.Meta{Version: *version, Commit: *commit, CommitTime: ct, Created: time.Now(), Channel: *channel})
		if err != nil {
			return fail(err)
		}
		if err := os.WriteFile(filepath.Join(*dir, update.ManifestFile), data, 0o644); err != nil {
			return fail(err)
		}
		if err := os.WriteFile(filepath.Join(*dir, update.SumsFile), release.Sums(m), 0o644); err != nil {
			return fail(err)
		}
		fmt.Fprintf(stdout, "wrote %s (%d artifacts, channel %s, version %s)\n", filepath.Join(*dir, update.ManifestFile), len(m.Artifacts), m.Channel, m.Version)
		return 0
	case "sign":
		in := fs.String("in", "dist/"+update.ManifestFile, "manifest to sign")
		out := fs.String("out", "", "signature file (default: manifest.sig next to -in)")
		if err := fs.Parse(args[1:]); err != nil {
			return 2
		}
		if *out == "" {
			*out = filepath.Join(filepath.Dir(*in), update.SignatureFile)
		}
		priv, source, err := release.LoadPrivateKey(getenv)
		if err != nil {
			return fail(err)
		}
		data, err := os.ReadFile(*in)
		if err != nil {
			return fail(err)
		}
		if err := os.WriteFile(*out, release.Sign(priv, data), 0o644); err != nil {
			return fail(err)
		}
		fmt.Fprintf(stdout, "signed %s with the key from %s -> %s\n", *in, source, *out)
		return 0
	case "verify":
		dir := fs.String("dir", "dist", "release directory")
		pubHex := fs.String("pubkey", "", "public key, hex (default: the key compiled into locus)")
		if err := fs.Parse(args[1:]); err != nil {
			return 2
		}
		pub, err := update.TrustedKey()
		if *pubHex != "" {
			pub, err = update.ParsePublicKey(*pubHex)
		}
		if err != nil {
			return fail(err)
		}
		m, err := release.VerifyDir(pub, *dir)
		if err != nil {
			return fail(err)
		}
		fmt.Fprintf(stdout, "verified %s: version %s, commit %s, channel %s, %d artifacts\n", *dir, m.Version, m.Commit, m.Channel, len(m.Artifacts))
		return 0
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usageText)
		return 0
	}
	fmt.Fprintf(stderr, "locus-release: unknown command %q\n\n%s", args[0], usageText)
	return 2
}
