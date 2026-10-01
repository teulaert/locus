// Package jobs implements the persistent SQLite-backed work queue for
// expensive asynchronous index work.
package jobs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Kind enumerates queue work types.
const (
	// KindIndex performs extract → chunk → embed for one document version.
	KindIndex = "index"
	// KindUnindex removes derived state for a deleted document.
	KindUnindex = "unindex"
)

// State enumerates job states.
const (
	StatePending = "pending"
	StateRunning = "running"
	StateFailed  = "failed"
)

// Job is one queued unit of work for the newest known version of a path.
type Job struct {
	ID          int64
	Kind        string
	Root        string
	RelPath     string
	Hash        string
	State       string
	Attempts    int
	LastError   string
	Priority    int
	AvailableAt int64
	// Generation identifies one claim of the row. Claim bumps it, and
	// Complete and Fail only act on the row while it still carries the
	// generation the worker claimed, so a late or duplicate finish can never
	// delete or reset work a newer claim owns.
	Generation int64
}

// Queue is a SQLite-backed job queue. Work coalesces: the queue keeps at most
// one pending job per (kind, root, relpath), always for the newest version,
// so a burst of intermediate states never becomes real work.
type Queue struct {
	db *sql.DB
	// MaxAttempts bounds retries before a job is parked as failed.
	MaxAttempts int
}

// NewQueue returns a queue over the derived index database.
func NewQueue(db *sql.DB) *Queue { return &Queue{db: db, MaxAttempts: 3} }

// Enqueue records work for the newest version of a path, replacing any
// pending job for the same (kind, path). If the job is currently running it
// stays running, so no second worker picks up a path whose derived state the
// first is still writing; instead the row is flagged for a rerun, which
// Complete or Fail turns into fresh pending work for the newer version.
func (q *Queue) Enqueue(ctx context.Context, kind, root, relpath, hash string, priority int) error {
	_, err := q.db.ExecContext(ctx, `INSERT INTO jobs (kind, root, relpath, hash, state, priority, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (kind, root, relpath) DO UPDATE SET
			hash = excluded.hash,
			state = CASE WHEN jobs.state = 'running' THEN 'running' ELSE 'pending' END,
			rerun = CASE WHEN jobs.state = 'running' THEN 1 ELSE 0 END,
			attempts = 0,
			priority = MAX(jobs.priority, excluded.priority),
			available_at = 0,
			last_error = '',
			updated_at = excluded.updated_at`,
		kind, root, relpath, hash, StatePending, priority, time.Now().Unix())
	return err
}

// DropFor removes queued work for a path (obsolete desired state).
func (q *Queue) DropFor(ctx context.Context, root, relpath string) (int, error) {
	res, err := q.db.ExecContext(ctx, `DELETE FROM jobs WHERE root = ? AND relpath = ? AND state <> 'running'`, root, relpath)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// ErrPermanent marks a failure that retrying cannot fix (for example an
// extractor that panics on a malformed file). Fail parks such a job as
// failed at once instead of backing off and retrying.
var ErrPermanent = errors.New("permanent failure")

// Permanent wraps err so Fail parks the job without retrying.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrPermanent, err)
}

// Claim atomically marks the next pending job as running. It returns nil when
// the queue is empty.
//
// The attempt is counted here rather than in Fail: a job that takes the whole
// process down never reaches Fail, so counting at claim time is what lets
// RequeueStale park it after MaxAttempts instead of crashing on every start.
func (q *Queue) Claim(ctx context.Context) (*Job, error) {
	now := time.Now().Unix()
	var j Job
	err := q.db.QueryRowContext(ctx, `UPDATE jobs SET state = 'running', rerun = 0,
			attempts = attempts + 1, generation = generation + 1, updated_at = ?
		WHERE id = (
			SELECT id FROM jobs
			WHERE state = 'pending' AND available_at <= ?
			ORDER BY priority DESC, id ASC
			LIMIT 1)
		RETURNING id, kind, root, relpath, hash, state, attempts, priority, generation`,
		now, now).Scan(&j.ID, &j.Kind, &j.Root, &j.RelPath, &j.Hash, &j.State, &j.Attempts, &j.Priority, &j.Generation)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &j, nil
}

// Complete finishes a claimed job. If newer work for the same path was
// enqueued while it ran, the row is made pending again for that version
// instead of being deleted.
func (q *Queue) Complete(ctx context.Context, job *Job) error {
	return q.finish(ctx, job, `DELETE FROM jobs
		WHERE id = ? AND generation = ? AND state = 'running' AND rerun = 0`,
		job.ID, job.Generation)
}

// Fail retries a job with backoff, parking it as failed after MaxAttempts
// (attempts were counted by Claim). A reason wrapping ErrPermanent parks the
// job immediately. If newer work was enqueued while it ran, the failure
// belongs to a superseded version: the newer work is made pending without
// penalty instead, even for a permanent failure.
func (q *Queue) Fail(ctx context.Context, job *Job, reason error) error {
	msg := reason.Error()
	if len(msg) > 500 {
		msg = msg[:500]
	}
	maxAttempts := q.MaxAttempts
	if errors.Is(reason, ErrPermanent) {
		maxAttempts = 0
	}
	now := time.Now().Unix()
	return q.finish(ctx, job, `UPDATE jobs SET
			state = CASE WHEN attempts >= ? THEN 'failed' ELSE 'pending' END,
			last_error = ?,
			available_at = CASE WHEN attempts >= ? THEN 0 ELSE ? END,
			updated_at = ?
		WHERE id = ? AND generation = ? AND state = 'running' AND rerun = 0`,
		maxAttempts, msg, maxAttempts, now+retryDelay, now, job.ID, job.Generation)
}

// finish ends the worker's claim on job: a row flagged for rerun becomes
// pending for its newer version, otherwise stmt (which must match only the
// claimed, non-rerun row) is applied. A finish from a claim that no longer
// owns the row is a no-op.
func (q *Queue) finish(ctx context.Context, job *Job, stmt string, args ...any) error {
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET state = 'pending', rerun = 0, available_at = 0, updated_at = ?
		WHERE id = ? AND generation = ? AND state = 'running' AND rerun = 1`,
		time.Now().Unix(), job.ID, job.Generation); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, stmt, args...); err != nil {
		return err
	}
	return tx.Commit()
}

const retryDelay = 15

// RequeueStale returns jobs left in running state (crash recovery). They are
// made pending again so a restart resumes work safely, unless they have
// already used up MaxAttempts: a job that keeps taking the process down is
// parked as failed rather than resumed into a crash loop. It returns the
// number of jobs resumed.
func (q *Queue) RequeueStale(ctx context.Context) (int, error) {
	now := time.Now().Unix()
	// A row flagged for rerun carries newer work (Enqueue reset its
	// attempts), so it is resumed rather than blamed for the crash.
	if _, err := q.db.ExecContext(ctx, `UPDATE jobs SET state = 'failed', rerun = 0, available_at = 0,
			last_error = 'interrupted ' || attempts || ' times while running (process exited)', updated_at = ?
		WHERE state = 'running' AND rerun = 0 AND attempts >= ?`, now, q.MaxAttempts); err != nil {
		return 0, err
	}
	res, err := q.db.ExecContext(ctx, `UPDATE jobs SET state = 'pending', rerun = 0, updated_at = ? WHERE state = 'running'`, now)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// Stats reports pending/running/failed counts per kind.
func (q *Queue) Stats(ctx context.Context) (map[string]map[string]int, error) {
	rows, err := q.db.QueryContext(ctx, `SELECT kind, state, COUNT(*) FROM jobs GROUP BY kind, state`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]map[string]int{}
	for rows.Next() {
		var kind, state string
		var n int
		if err := rows.Scan(&kind, &state, &n); err != nil {
			return nil, err
		}
		if out[kind] == nil {
			out[kind] = map[string]int{}
		}
		out[kind][state] = n
	}
	return out, rows.Err()
}

// PendingCount returns jobs still waiting for work.
func (q *Queue) PendingCount(ctx context.Context) (int, error) {
	var n int
	err := q.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs WHERE state IN ('pending','running')`).Scan(&n)
	return n, err
}

// List returns queued jobs, most recent work first.
func (q *Queue) List(ctx context.Context, limit int) ([]*Job, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := q.db.QueryContext(ctx, `SELECT id, kind, root, relpath, hash, state, attempts, COALESCE(last_error,''), priority
		FROM jobs ORDER BY priority DESC, id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Job
	for rows.Next() {
		var j Job
		if err := rows.Scan(&j.ID, &j.Kind, &j.Root, &j.RelPath, &j.Hash, &j.State, &j.Attempts, &j.LastError, &j.Priority); err != nil {
			return nil, err
		}
		out = append(out, &j)
	}
	return out, rows.Err()
}

// ParseKind validates a kind string.
func ParseKind(s string) (string, error) {
	switch s = strings.ToLower(strings.TrimSpace(s)); s {
	case KindIndex, KindUnindex:
		return s, nil
	default:
		return "", fmt.Errorf("jobs: unknown kind %q", s)
	}
}
