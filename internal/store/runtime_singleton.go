package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	runtimeSingletonLockName = "conductor:runtime-singleton"

	// DefaultRuntimeSingletonCheckInterval is how often Monitor proves that the
	// session owning the runtime lock is alive. The caller must terminate the
	// service when Monitor returns an error.
	DefaultRuntimeSingletonCheckInterval = time.Second
	// runtimeSingletonProbeTimeout tolerates a host stall that delays a single
	// round trip. pgx closes a connection whose read times out, which ends the
	// session and releases the lock, so a timed-out probe leads to reacquire.
	runtimeSingletonProbeTimeout = 5 * time.Second
	// runtimeSingletonReacquireWindow bounds how long a runtime keeps serving
	// after its lock session ended, while it tries to win the lock back on a
	// new session. Live streams keep flowing meanwhile; they do not need the
	// database. A memory stall on the host has ended the session this way
	// while nothing else was wrong.
	runtimeSingletonReacquireWindow  = time.Minute
	runtimeSingletonReacquireBackoff = time.Second
)

// ErrRuntimeAlreadyActive means another Conductor process owns this database.
// Conductor's stream pumps are process-local and startup reconciliation marks
// old active_stream rows dead, so overlapping processes are intentionally
// rejected rather than pretending their pumps can share database rows.
var ErrRuntimeAlreadyActive = errors.New("another conductor runtime is active")

// RuntimeSingleton owns a PostgreSQL session advisory lock on a dedicated
// pooled connection. It must be acquired before startup reconciliation and
// held until all process-local stream pumps and workers have stopped.
type RuntimeSingleton struct {
	// OnReacquire, when set before Monitor starts, is called after a lost lock
	// session has been replaced, with the probe error and how long ownership
	// was unproven.
	OnReacquire func(cause error, gap time.Duration)

	mu         sync.Mutex
	pool       *pgxpool.Pool
	conn       *pgxpool.Conn
	backendPID int32
	closed     bool
}

// AcquireRuntimeSingleton attempts to become the sole Conductor runtime for
// this database. It never waits behind another runtime: a busy lock fails
// closed so a second process cannot reconcile or attach to foreign pumps.
func (db *DB) AcquireRuntimeSingleton(ctx context.Context) (*RuntimeSingleton, error) {
	if db == nil || db.Pool == nil {
		return nil, errors.New("db not open")
	}
	conn, backendPID, err := tryRuntimeLock(ctx, db.Pool)
	if err != nil {
		return nil, err
	}
	return &RuntimeSingleton{pool: db.Pool, conn: conn, backendPID: backendPID}, nil
}

// tryRuntimeLock takes the runtime lock on a fresh pooled session. The
// connection is returned only when its session now owns the lock. When the
// lock query fails, the returned PID names the discarded session, which may
// hold the lock until its teardown reaches PostgreSQL.
func tryRuntimeLock(ctx context.Context, pool *pgxpool.Pool) (*pgxpool.Conn, int32, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("acquire runtime singleton connection: %w", err)
	}
	var acquired bool
	var backendPID int32
	err = conn.QueryRow(ctx, `
		SELECT pg_try_advisory_lock(hashtextextended($1, 0)),
		       pg_backend_pid()`, runtimeSingletonLockName).Scan(&acquired, &backendPID)
	if err != nil {
		// The lock may have been granted before the error: never pool it.
		pid := int32(conn.Conn().PgConn().PID())
		discardRuntimeConn(conn)
		return nil, pid, fmt.Errorf("acquire runtime singleton lock: %w", err)
	}
	if !acquired {
		conn.Release()
		return nil, 0, ErrRuntimeAlreadyActive
	}
	return conn, backendPID, nil
}

// discardRuntimeConn closes a session that may hold the runtime lock instead
// of returning it to the pool, where an idle connection could keep the lock.
func discardRuntimeConn(conn *pgxpool.Conn) {
	raw := conn.Hijack()
	closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = raw.Close(closeCtx)
	cancel()
}

// Monitor verifies that the exact PostgreSQL session owning the advisory lock
// remains alive. A pgxpool.Conn cannot transparently swap its underlying
// session while acquired; checking the backend PID also makes that invariant
// explicit. When the probe fails, Monitor wins the lock back on a new session
// (see reacquire) and returns an error only when that is impossible.
func (s *RuntimeSingleton) Monitor(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = DefaultRuntimeSingletonCheckInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	lastOK := time.Now()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// A shutdown must not cancel an in-flight probe: pgx would close the
			// session and release the lock before the stream pumps have stopped.
			probeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), runtimeSingletonProbeTimeout)
			err := s.check(probeCtx)
			cancel()
			if err == nil {
				lastOK = time.Now()
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// The window and the reported gap both count from the last proof of
			// ownership, which is up to a probe timeout before the failure.
			if rerr := s.reacquire(ctx, lastOK.Add(runtimeSingletonReacquireWindow)); rerr != nil {
				return errors.Join(err, rerr)
			}
			if s.OnReacquire != nil {
				s.OnReacquire(err, time.Since(lastOK))
			}
			lastOK = time.Now()
		}
	}
}

// reacquire replaces a lock session that failed its probe. The old session is
// closed first, so its backend exits and Postgres releases the lock. While any
// of our sessions still exists (the old one, or an attempt whose reply was
// lost after the lock was granted), a held lock may be our own, so the attempt
// is retried. Once none exists, a held lock belongs to another runtime and
// ownership is lost: Postgres releases a backend's advisory locks before its
// pg_stat_activity row disappears.
//
// Known gap: a runtime that takes and releases the lock entirely inside the
// window goes unnoticed. Only one Conductor runs per database in practice.
func (s *RuntimeSingleton) reacquire(ctx context.Context, deadline time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("runtime singleton is closed")
	}
	if s.conn != nil {
		discardRuntimeConn(s.conn)
		s.conn = nil
	}
	ours := []int32{s.backendPID}
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, runtimeSingletonProbeTimeout)
		var oursAlive bool
		err := s.pool.QueryRow(attemptCtx,
			`SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = ANY($1))`, ours).Scan(&oursAlive)
		if err == nil {
			var conn *pgxpool.Conn
			var pid int32
			conn, pid, err = tryRuntimeLock(attemptCtx, s.pool)
			if err == nil {
				cancel()
				s.conn, s.backendPID = conn, pid
				return nil
			}
			if pid != 0 {
				ours = append(ours, pid)
			}
		}
		cancel()
		if errors.Is(err, ErrRuntimeAlreadyActive) && !oursAlive {
			return ErrRuntimeAlreadyActive
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().Add(runtimeSingletonReacquireBackoff).After(deadline) {
			return fmt.Errorf("reacquire runtime singleton within %s: %w", runtimeSingletonReacquireWindow, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(runtimeSingletonReacquireBackoff):
		}
	}
}

func (s *RuntimeSingleton) check(ctx context.Context) error {
	if s == nil {
		return errors.New("runtime singleton is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.conn == nil {
		return errors.New("runtime singleton is closed")
	}
	var backendPID int32
	if err := s.conn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&backendPID); err != nil {
		return fmt.Errorf("runtime singleton connection lost: %w", err)
	}
	if backendPID != s.backendPID {
		return fmt.Errorf("runtime singleton session changed from backend %d to %d", s.backendPID, backendPID)
	}
	return nil
}

// Close explicitly unlocks before returning the dedicated connection to the
// pool. If unlock cannot be proven, the session is hijacked and closed so a
// pooled idle connection can never retain the singleton accidentally.
func (s *RuntimeSingleton) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.conn == nil {
		return nil
	}
	s.closed = true
	conn := s.conn
	s.conn = nil

	var unlocked bool
	err := conn.QueryRow(ctx,
		`SELECT pg_advisory_unlock(hashtextextended($1, 0))`,
		runtimeSingletonLockName).Scan(&unlocked)
	if err == nil && unlocked {
		conn.Release()
		return nil
	}

	// Release would put a healthy-but-still-locked session back into the pool.
	// Remove it from the pool and close the physical connection instead.
	raw := conn.Hijack()
	closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	closeErr := raw.Close(closeCtx)
	cancel()
	if err != nil {
		return errors.Join(fmt.Errorf("unlock runtime singleton: %w", err), closeErr)
	}
	if !unlocked {
		err = errors.New("runtime singleton advisory lock was not held")
	}
	return errors.Join(err, closeErr)
}
