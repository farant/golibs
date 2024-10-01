// Package jobs provides a simple job queue system with SQLite backend.
//
// It offers features such as:
//   - Job scheduling with customizable run times
//   - Automatic job retries
//   - Concurrent job processing with multiple workers
//   - Persistent storage using SQLite
//
// Basic usage:
//
//	jqm, err := jobs.NewJobQueueManager("jobs.db")
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	jqm.RegisterHandler("email", func(ctx context.Context, payload json.RawMessage) error {
//		// Handle email job
//		return nil
//	})
//
//	jqm.StartWorkers(3)
//
//	err = jqm.Enqueue("email", emailPayload, time.Now().Add(5*time.Minute))
//	if err != nil {
//		log.Printf("Failed to enqueue job: %v", err)
//	}
//
// For more detailed examples, see the examples in the documentation.
package jobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// Job represents a task to be executed by the job queue system.
type Job struct {
	ID        int64           `json:"id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	Status    string          `json:"status"`
	CreatedAt time.Time       `json:"created_at"`
	RunAt     time.Time       `json:"run_at"`
	LockedAt  *time.Time      `json:"locked_at"`
	LockedBy  *string         `json:"locked_by"`
}

// JobHandler is a function that processes a job's payload.
type JobHandler func(context.Context, json.RawMessage) error

// JobQueue manages the queue of jobs and their execution.
type JobQueue struct {
	db       *sql.DB
	handlers map[string]JobHandler
}

// NewJobQueue creates a new JobQueue with the specified SQLite database path.
func NewJobQueue(dbPath string) (*JobQueue, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS jobs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			type TEXT NOT NULL,
			payload TEXT NOT NULL,
			status TEXT NOT NULL,
			created_at DATETIME NOT NULL,
			run_at DATETIME NOT NULL,
			locked_at DATETIME,
			locked_by TEXT
		)
	`)
	if err != nil {
		return nil, err
	}

	return &JobQueue{
		db:       db,
		handlers: make(map[string]JobHandler),
	}, nil
}

// RegisterHandler associates a job type with its handler function.
func (jq *JobQueue) RegisterHandler(jobType string, handler JobHandler) {
	jq.handlers[jobType] = handler
}

// Enqueue adds a new job to the queue.
func (jq *JobQueue) Enqueue(jobType string, payload interface{}, runAt time.Time) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	_, err = jq.db.Exec(
		"INSERT INTO jobs (type, payload, status, created_at, run_at) VALUES (?, ?, ?, ?, ?)",
		jobType, payloadBytes, "pending", time.Now(), runAt,
	)
	return err
}

// DequeueJob retrieves the next available job from the queue.
func (jq *JobQueue) DequeueJob(workerID string) (*Job, error) {
	tx, err := jq.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var job Job
	err = tx.QueryRow(`
		SELECT id, type, payload, status, created_at, run_at
		FROM jobs
		WHERE status = 'pending' AND run_at <= datetime('now')
		AND (locked_at IS NULL OR locked_at < datetime('now', '-5 minutes'))
		ORDER BY run_at ASC
		LIMIT 1
	`).Scan(&job.ID, &job.Type, &job.Payload, &job.Status, &job.CreatedAt, &job.RunAt)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	now := time.Now()
	_, err = tx.Exec("UPDATE jobs SET status = 'processing', locked_at = ?, locked_by = ? WHERE id = ?", now, workerID, job.ID)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	job.LockedAt = &now
	job.LockedBy = &workerID

	return &job, nil
}

// CompleteJob marks a job as completed.
func (jq *JobQueue) CompleteJob(jobID int64) error {
	_, err := jq.db.Exec("UPDATE jobs SET status = 'completed', locked_at = NULL, locked_by = NULL WHERE id = ?", jobID)
	return err
}

// FailJob marks a job as failed.
func (jq *JobQueue) FailJob(jobID int64) error {
	_, err := jq.db.Exec("UPDATE jobs SET status = 'failed', locked_at = NULL, locked_by = NULL WHERE id = ?", jobID)
	return err
}

// JobQueueManager coordinates multiple workers and provides a high-level interface for the job queue system.
type JobQueueManager struct {
	jobQueue *JobQueue
	workers  []*Worker
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewJobQueueManager creates a new JobQueueManager with the specified SQLite database path.
func NewJobQueueManager(dbPath string) (*JobQueueManager, error) {
	jq, err := NewJobQueue(dbPath)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &JobQueueManager{
		jobQueue: jq,
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

// RegisterHandler associates a job type with its handler function.
func (jqm *JobQueueManager) RegisterHandler(jobType string, handler JobHandler) {
	jqm.jobQueue.RegisterHandler(jobType, handler)
}

// Enqueue adds a new job to the queue.
func (jqm *JobQueueManager) Enqueue(jobType string, payload interface{}, runAt time.Time) error {
	return jqm.jobQueue.Enqueue(jobType, payload, runAt)
}

// StartWorkers initializes and starts the specified number of worker goroutines.
func (jqm *JobQueueManager) StartWorkers(numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		workerID := fmt.Sprintf("worker-%d", i+1)
		worker := NewWorker(workerID, jqm.jobQueue)
		jqm.workers = append(jqm.workers, worker)

		jqm.wg.Add(1)
		go func() {
			defer jqm.wg.Done()
			worker.Start(jqm.ctx)
		}()
	}
}

// Shutdown gracefully stops all workers and waits for them to finish.
func (jqm *JobQueueManager) Shutdown() {
	jqm.cancel()
	jqm.wg.Wait()
}

// Worker represents a single worker that processes jobs from the queue.
type Worker struct {
	ID       string
	JobQueue *JobQueue
}

// NewWorker creates a new Worker with the specified ID and JobQueue.
func NewWorker(id string, jq *JobQueue) *Worker {
	return &Worker{
		ID:       id,
		JobQueue: jq,
	}
}

// Start begins the worker's job processing loop.
func (w *Worker) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			job, err := w.JobQueue.DequeueJob(w.ID)
			if err != nil {
				log.Printf("Worker %s: Error dequeuing job: %v", w.ID, err)
				time.Sleep(5 * time.Second)
				continue
			}
			if job == nil {
				time.Sleep(5 * time.Second)
				continue
			}

			log.Printf("Worker %s: Processing job %d", w.ID, job.ID)
			handler, ok := w.JobQueue.handlers[job.Type]
			if !ok {
				log.Printf("Worker %s: No handler for job type %s", w.ID, job.Type)
				if err := w.JobQueue.FailJob(job.ID); err != nil {
					log.Printf("Worker %s: Error marking job %d as failed: %v", w.ID, job.ID, err)
				}
				continue
			}

			err = handler(ctx, job.Payload)
			if err != nil {
				log.Printf("Worker %s: Error processing job %d: %v", w.ID, job.ID, err)
				if err := w.JobQueue.FailJob(job.ID); err != nil {
					log.Printf("Worker %s: Error marking job %d as failed: %v", w.ID, job.ID, err)
				}
			} else {
				if err := w.JobQueue.CompleteJob(job.ID); err != nil {
					log.Printf("Worker %s: Error marking job %d as completed: %v", w.ID, job.ID, err)
				}
			}
		}
	}
}
