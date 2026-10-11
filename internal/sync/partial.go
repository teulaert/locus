package sync

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	gosync "sync"
	"time"
)

// ResumableMinSize is the smallest declared size for which a put is received
// into a resumable partial object, and for which a sender asks the receiver
// how much of the object it already holds. Smaller files stream straight
// into place: re-sending them costs less than the extra round trip and copy.
const ResumableMinSize = 1 << 20

// DefaultPartialMaxAge is how long an interrupted upload is kept for the
// sender to resume before it is garbage-collected.
const DefaultPartialMaxAge = 24 * time.Hour

// Errors PartialStore.Receive reports besides ErrTooLarge and
// ErrContentMismatch.
var (
	// ErrPartialOffset: a continuation's offset is not the length of the
	// partial object this peer holds (or it holds none). The sender starts
	// again from zero.
	ErrPartialOffset = errors.New("sync: upload offset does not match the partial object")
	// ErrIncomplete: the body ended before the declared size. The partial
	// object is kept so the sender can resume it.
	ErrIncomplete = errors.New("sync: upload incomplete")
)

// PartialStore keeps interrupted uploads (puts) in the data directory, keyed
// by the content hash they declare, so a sender can continue one from the
// length received instead of from zero (LOC-67). It never lives inside a
// root: a partial object is not a document, and the reconciler, watcher and
// index never see it.
//
// A partial is only ever installed after the whole object hashes to its
// name; one that does not is removed. Files older than MaxAge are removed
// by Sweep.
type PartialStore struct {
	Dir    string
	MaxAge time.Duration

	mu   gosync.Mutex
	busy map[string]bool
}

// partialName is the file of the resumable partial object for hash.
func (p *PartialStore) partialName(hash string) string {
	return filepath.Join(p.Dir, hash+".part")
}

// lock claims hash for one upload at a time, reporting whether it could.
func (p *PartialStore) lock(hash string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.busy[hash] {
		return false
	}
	if p.busy == nil {
		p.busy = map[string]bool{}
	}
	p.busy[hash] = true
	return true
}

func (p *PartialStore) unlock(hash string) {
	p.mu.Lock()
	delete(p.busy, hash)
	p.mu.Unlock()
}

// Offset returns how many bytes of the object with content hash hash this
// peer holds from an interrupted upload: 0 when it holds none.
func (p *PartialStore) Offset(hash string) (int64, error) {
	if _, err := DeclaredContentHash(hash); err != nil {
		return 0, err
	}
	fi, err := os.Stat(p.partialName(hash))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// Staged is received content waiting outside the roots to be installed:
// complete, and verified against the content hash it was declared with,
// before anything takes a path lock (LOC-69). Install moves it into place
// with a rename when the data directory and the root share a filesystem,
// and copies it otherwise. It reads as its content, from the start.
type Staged struct {
	f    *os.File
	path string
	// Hash is the content's verified SHA-256 (lowercase hex), Size its
	// length.
	Hash string
	Size int64
	// moved is set once Install has renamed the file into a root.
	moved bool
}

func (s *Staged) Read(p []byte) (int, error) { return s.f.Read(p) }

// stageFile receives body into f, the bytes of the object from offset on
// (any bytes before offset are already in f), and verifies the complete
// object: size (negative when unknown) and maxBytes (unlimited when <= 0)
// bound the object as a whole and reading stops one byte past the tighter,
// and wantHash, when non-empty, must be its SHA-256. It returns the object
// size and hash, or the read error, ErrIncomplete, ErrTooLarge or
// ErrContentMismatch; for the last two f's content is worthless.
func stageFile(f *os.File, offset, size, maxBytes int64, wantHash string, body io.Reader) (int64, string, error) {
	if maxBytes <= 0 {
		maxBytes = 1 << 62
	}
	h := sha256.New()
	if offset > 0 {
		fi, err := f.Stat()
		if err != nil {
			return 0, "", err
		}
		if fi.Size() != offset {
			return 0, "", fmt.Errorf("%w: holds %d bytes, upload continues at %d", ErrPartialOffset, fi.Size(), offset)
		}
		// The prefix arrived in an earlier request; it is hashed again so
		// the whole object is verified, not only the bytes of this one.
		if _, err := io.Copy(h, io.NewSectionReader(f, 0, offset)); err != nil {
			return 0, "", err
		}
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return 0, "", err
		}
	}
	limit := maxBytes
	if size >= 0 && size < limit {
		limit = size
	}
	n, err := io.Copy(io.MultiWriter(f, h), io.LimitReader(body, limit-offset+1))
	total := offset + n
	switch {
	case total > maxBytes:
		return 0, "", fmt.Errorf("%w %d", ErrTooLarge, maxBytes)
	case size >= 0 && total > size:
		return 0, "", fmt.Errorf("%w: expected %d bytes, received more", ErrContentMismatch, size)
	case err != nil:
		return 0, "", err
	case size >= 0 && total < size:
		return 0, "", fmt.Errorf("%w: received %d of %d bytes", ErrIncomplete, total, size)
	}
	sum := hex.EncodeToString(h.Sum(nil))
	if wantHash != "" && sum != wantHash {
		return 0, "", fmt.Errorf("%w: content hashes to %s, declared %s", ErrContentMismatch, sum, wantHash)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, "", err
	}
	return total, sum, nil
}

// Stage receives body, the whole content declared with hash wantHash (any
// content when empty) and size (negative when unknown), into a private file
// in the store's directory and returns it verified. Nothing of a failed or
// interrupted transfer is kept. release closes and removes the file unless
// Install has moved it into a root.
func (p *PartialStore) Stage(wantHash string, size, maxBytes int64, body io.Reader) (_ *Staged, release func(), err error) {
	if err := os.MkdirAll(p.Dir, 0o700); err != nil {
		return nil, nil, err
	}
	prefix := "stage"
	if _, err := DeclaredContentHash(wantHash); err == nil {
		prefix = wantHash
	}
	f, err := os.CreateTemp(p.Dir, prefix+".*.tmp")
	if err != nil {
		return nil, nil, err
	}
	discard := func() {
		f.Close()
		os.Remove(f.Name())
	}
	n, sum, err := stageFile(f, 0, size, maxBytes, wantHash, body)
	if err != nil {
		discard()
		return nil, nil, err
	}
	st := &Staged{f: f, path: f.Name(), Hash: sum, Size: n}
	return st, st.release(nil), nil
}

// release returns the cleanup for a staged file: close it, remove it unless
// it was moved into a root, then run after.
func (s *Staged) release(after func()) func() {
	return func() {
		s.f.Close()
		if !s.moved {
			os.Remove(s.path)
		}
		if after != nil {
			after()
		}
	}
}

// Receive appends body, the bytes of the object with content hash hash from
// offset on, to the partial object for hash, and returns the complete
// object, verified against hash. The caller must call release when done
// with it, which closes and removes it.
//
// size is the declared size of the whole object (negative when unknown) and
// maxBytes the upload cap (unlimited when <= 0); both bound the object as a
// whole, across every continuation, and reading stops one byte past the
// tighter. An offset of 0 starts the object afresh. A continuation (offset >
// 0) must start exactly where the partial object ends, else ErrPartialOffset.
//
// When the body breaks off, or ends before size, what arrived is flushed
// and kept for the next continuation and Receive returns the read error or
// ErrIncomplete. Content that turns out too large (ErrTooLarge) or does not
// hash to hash (ErrContentMismatch) is removed.
//
// While another upload of the same object is in progress, a fresh upload is
// staged into a private file instead (which is not resumable), and a
// continuation is refused with ErrPartialOffset.
func (p *PartialStore) Receive(hash string, offset, size, maxBytes int64, body io.Reader) (_ *Staged, release func(), err error) {
	if _, err := DeclaredContentHash(hash); err != nil {
		return nil, nil, err
	}
	if offset < 0 || (size >= 0 && offset > size) {
		return nil, nil, fmt.Errorf("%w: offset %d of %d bytes", ErrPartialOffset, offset, size)
	}
	if maxBytes > 0 && offset > maxBytes {
		return nil, nil, fmt.Errorf("%w %d", ErrTooLarge, maxBytes)
	}
	if err := os.MkdirAll(p.Dir, 0o700); err != nil {
		return nil, nil, err
	}
	if !p.lock(hash) {
		if offset == 0 {
			return p.Stage(hash, size, maxBytes, body)
		}
		return nil, nil, fmt.Errorf("%w: another upload of it is in progress", ErrPartialOffset)
	}
	unlock := func() { p.unlock(hash) }
	flags := os.O_RDWR
	if offset == 0 {
		flags |= os.O_CREATE | os.O_TRUNC
	}
	f, err := os.OpenFile(p.partialName(hash), flags, 0o600)
	if errors.Is(err, os.ErrNotExist) {
		err = fmt.Errorf("%w: no partial object", ErrPartialOffset)
	}
	if err != nil {
		unlock()
		return nil, nil, err
	}
	n, sum, err := stageFile(f, offset, size, maxBytes, hash, body)
	if err != nil {
		if errors.Is(err, ErrTooLarge) || errors.Is(err, ErrContentMismatch) {
			f.Close()
			os.Remove(f.Name())
		} else {
			// Keep what arrived for a continuation.
			_ = f.Sync()
			f.Close()
		}
		unlock()
		return nil, nil, err
	}
	st := &Staged{f: f, path: f.Name(), Hash: sum, Size: n}
	return st, st.release(unlock), nil
}

// SweepStaged removes every private staging file (*.tmp): they belong to a
// transfer of this process, so at startup any left are orphans of a killed
// one. Resumable partial objects (*.part) are kept.
func (p *PartialStore) SweepStaged() (int, error) {
	ents, err := os.ReadDir(p.Dir)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, ent := range ents {
		if ent.Type().IsRegular() && strings.HasSuffix(ent.Name(), ".tmp") && os.Remove(filepath.Join(p.Dir, ent.Name())) == nil {
			removed++
		}
	}
	return removed, nil
}

// Sweep removes partial objects (and private upload files) not written to
// for longer than MaxAge (DefaultPartialMaxAge when zero), except those an
// upload is writing now. It returns how many it removed.
func (p *PartialStore) Sweep(now time.Time) (int, error) {
	maxAge := p.MaxAge
	if maxAge <= 0 {
		maxAge = DefaultPartialMaxAge
	}
	ents, err := os.ReadDir(p.Dir)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, ent := range ents {
		name := ent.Name()
		if !ent.Type().IsRegular() || !(strings.HasSuffix(name, ".part") || strings.HasSuffix(name, ".tmp")) {
			continue
		}
		info, err := ent.Info()
		if err != nil || now.Sub(info.ModTime()) < maxAge {
			continue
		}
		hash, _, _ := strings.Cut(name, ".")
		if !p.lock(hash) {
			continue // being written right now
		}
		if os.Remove(filepath.Join(p.Dir, name)) == nil {
			removed++
		}
		p.unlock(hash)
	}
	return removed, nil
}
