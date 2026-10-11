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

// Receive appends body, the bytes of the object with content hash hash from
// offset on, to the partial object for hash, and returns the complete
// object, verified against hash, opened at its start. The caller must call
// release when done with it, which closes and removes it.
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
// received into a private temporary file instead (which is not resumable),
// and a continuation is refused with ErrPartialOffset.
func (p *PartialStore) Receive(hash string, offset, size, maxBytes int64, body io.Reader) (_ *os.File, release func(), err error) {
	if _, err := DeclaredContentHash(hash); err != nil {
		return nil, nil, err
	}
	if offset < 0 || (size >= 0 && offset > size) {
		return nil, nil, fmt.Errorf("%w: offset %d of %d bytes", ErrPartialOffset, offset, size)
	}
	if maxBytes <= 0 {
		maxBytes = 1 << 62
	}
	if offset > maxBytes {
		return nil, nil, fmt.Errorf("%w %d", ErrTooLarge, maxBytes)
	}
	if err := os.MkdirAll(p.Dir, 0o700); err != nil {
		return nil, nil, err
	}
	locked := p.lock(hash)
	unlock := func() {
		if locked {
			p.unlock(hash)
		}
	}
	var f *os.File
	switch {
	case locked && offset == 0:
		f, err = os.OpenFile(p.partialName(hash), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	case locked:
		f, err = os.OpenFile(p.partialName(hash), os.O_RDWR, 0)
		if errors.Is(err, os.ErrNotExist) {
			err = fmt.Errorf("%w: no partial object", ErrPartialOffset)
		}
	case offset == 0:
		f, err = os.CreateTemp(p.Dir, hash+".*.tmp")
	default:
		err = fmt.Errorf("%w: another upload of it is in progress", ErrPartialOffset)
	}
	if err != nil {
		unlock()
		return nil, nil, err
	}
	name := f.Name()
	discard := func() {
		f.Close()
		os.Remove(name)
		unlock()
	}
	// keep leaves what arrived for a continuation; a private temporary
	// file cannot be continued and is removed.
	keep := func() {
		if !locked {
			discard()
			return
		}
		_ = f.Sync()
		f.Close()
		unlock()
	}

	h := sha256.New()
	if offset > 0 {
		fi, err := f.Stat()
		if err != nil {
			keep()
			return nil, nil, err
		}
		if fi.Size() != offset {
			keep()
			return nil, nil, fmt.Errorf("%w: holds %d bytes, upload continues at %d", ErrPartialOffset, fi.Size(), offset)
		}
		// The prefix arrived in an earlier request; it is hashed again so
		// the whole object is verified, not only the bytes of this one.
		if _, err := io.Copy(h, io.NewSectionReader(f, 0, offset)); err != nil {
			keep()
			return nil, nil, err
		}
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			keep()
			return nil, nil, err
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
		discard()
		return nil, nil, fmt.Errorf("%w %d", ErrTooLarge, maxBytes)
	case size >= 0 && total > size:
		discard()
		return nil, nil, fmt.Errorf("%w: expected %d bytes, received more", ErrContentMismatch, size)
	case err != nil:
		keep()
		return nil, nil, err
	case size >= 0 && total < size:
		keep()
		return nil, nil, fmt.Errorf("%w: received %d of %d bytes", ErrIncomplete, total, size)
	}
	if sum := hex.EncodeToString(h.Sum(nil)); sum != hash {
		discard()
		return nil, nil, fmt.Errorf("%w: upload hashes to %s, declared %s", ErrContentMismatch, sum, hash)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		discard()
		return nil, nil, err
	}
	return f, discard, nil
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
