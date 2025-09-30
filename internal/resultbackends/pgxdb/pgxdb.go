// Package pgxdb is an optimized PostgreSQL backend implementation using pgx
// for high-performance bulk operations with COPY protocol support.
package pgxdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/zerodha/dungbeetle/v2/models"
)

// Opt represents PostgreSQL backend options.
type Opt struct {
	ResultsTable    string
	UnloggedTables  bool
	BatchInsert     bool // Enable bulk inserts
	BatchSize       int  // Batch size for COPY operations
	MaxConns        int
	MaxConnIdleTime int
}

// PgxDB represents the optimized PostgreSQL backend.
type PgxDB struct {
	pool   *pgxpool.Pool
	opt    Opt
	logger *slog.Logger

	// Cache for result schemas
	resTableSchemas map[string]insertSchema
	schemaMutex     sync.RWMutex
}

// PgxResultSet represents a writer that saves results using pgx.
type PgxResultSet struct {
	jobID       string
	taskName    string
	colsWritten bool
	cols        []string
	colTypes    []*sql.ColumnType

	// For COPY protocol
	rows      [][]interface{}
	rowBuffer sync.Pool

	// For standard inserts
	tx  pgx.Tx
	tbl string

	backend *PgxDB
	ctx     context.Context
	cancel  context.CancelFunc
}

// insertSchema contains the generated SQL for creating tables and inserting rows.
type insertSchema struct {
	dropTable   string
	createTable string
	insertRow   string
	copyColumns []string
}

// NewPgxBackend returns a new pgx result backend instance.
func NewPgxBackend(connString string, opt Opt, lo *slog.Logger) (*PgxDB, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}
	if opt.MaxConns == 0 {
		opt.MaxConns = 5
	}
	// Optimize connection pool settings
	config.MaxConns = int32(opt.MaxConns)
	config.MinConns = 5
	config.MaxConnLifetime = time.Hour
	config.MaxConnIdleTime = time.Minute * time.Duration(opt.MaxConnIdleTime)

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	s := &PgxDB{
		pool:            pool,
		opt:             opt,
		resTableSchemas: make(map[string]insertSchema),
		logger:          lo,
	}

	if opt.BatchSize == 0 {
		s.opt.BatchSize = 5000
	}

	return s, nil
}

// NewResultSet returns a new instance of a pgx result writer.
func (p *PgxDB) NewResultSet(jobID, taskName string, ttl time.Duration) (models.ResultSet, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ttl)

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	rs := &PgxResultSet{
		jobID:    jobID,
		taskName: taskName,
		backend:  p,
		tbl:      fmt.Sprintf(p.opt.ResultsTable, jobID),
		tx:       tx,
		ctx:      ctx,
		cancel:   cancel,
		rows:     make([][]interface{}, 0, p.opt.BatchSize),
	}

	// Initialize row buffer pool for memory efficiency
	rs.rowBuffer = sync.Pool{
		New: func() interface{} {
			return make([]interface{}, 0, 20) // Adjust based on typical column count
		},
	}

	return rs, nil
}

// RegisterColTypes registers the column types for a task's result set.
func (w *PgxResultSet) RegisterColTypes(cols []string, colTypes []*sql.ColumnType) error {
	if w.IsColTypesRegistered() {
		return errors.New("column types are already registered")
	}

	w.cols = make([]string, len(cols))
	copy(w.cols, cols)
	w.colTypes = colTypes

	// Create schema and cache it
	w.backend.schemaMutex.Lock()
	w.backend.resTableSchemas[w.taskName] = w.backend.createTableSchema(cols, colTypes)
	w.backend.schemaMutex.Unlock()

	return nil
}

// IsColTypesRegistered checks if column types are registered.
func (w *PgxResultSet) IsColTypesRegistered() bool {
	w.backend.schemaMutex.RLock()
	_, ok := w.backend.resTableSchemas[w.taskName]
	w.backend.schemaMutex.RUnlock()
	return ok
}

// WriteCols creates the results table.
func (w *PgxResultSet) WriteCols(cols []string) error {
	if w.colsWritten {
		return fmt.Errorf("columns for '%s' are already written", w.taskName)
	}

	w.backend.schemaMutex.RLock()
	rSchema, ok := w.backend.resTableSchemas[w.taskName]
	w.backend.schemaMutex.RUnlock()

	if !ok {
		return fmt.Errorf("column types for '%s' have not been registered", w.taskName)
	}

	// Create table in a separate transaction for DDL
	ddlTx, err := w.backend.pool.Begin(w.ctx)
	if err != nil {
		return fmt.Errorf("failed to begin DDL transaction: %w", err)
	}
	defer ddlTx.Rollback(w.ctx)

	// Drop existing table
	fmt.Println("Creating results table:", w.tbl)
	if _, err := ddlTx.Exec(w.ctx, fmt.Sprintf(rSchema.dropTable, w.tbl)); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	// Create new table
	if _, err := ddlTx.Exec(w.ctx, fmt.Sprintf(rSchema.createTable, w.tbl)); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	if err := ddlTx.Commit(w.ctx); err != nil {
		return fmt.Errorf("failed to commit DDL transaction: %w", err)
	}

	w.colsWritten = true
	return nil
}

// WriteRow writes a row to the result set.
func (w *PgxResultSet) WriteRow(row []interface{}) error {
	if !w.colsWritten {
		return errors.New("columns must be written before rows")
	}

	if w.backend.opt.BatchInsert {
		// Buffer rows for COPY protocol
		rowCopy := make([]interface{}, len(row))
		copy(rowCopy, row)
		w.rows = append(w.rows, rowCopy)

		// Flush if we've reached batch size
		if len(w.rows) >= w.backend.opt.BatchSize {
			fmt.Println("Flushing batch of rows using COPY protocol	", len(w.rows))
			return w.flushCopy()
		}
		return nil
	}

	// Standard insert for non-COPY mode
	w.backend.schemaMutex.RLock()
	rSchema, ok := w.backend.resTableSchemas[w.taskName]
	w.backend.schemaMutex.RUnlock()

	if !ok {
		return fmt.Errorf("schema not found for task '%s'", w.taskName)
	}

	_, err := w.tx.Exec(w.ctx, fmt.Sprintf(rSchema.insertRow, w.tbl), row...)
	return err
}

// flushCopy performs bulk insert using COPY protocol.
func (w *PgxResultSet) flushCopy() error {
	if len(w.rows) == 0 {
		return nil
	}

	w.backend.schemaMutex.RLock()
	rSchema, ok := w.backend.resTableSchemas[w.taskName]
	w.backend.schemaMutex.RUnlock()

	if !ok {
		return fmt.Errorf("schema not found for task '%s'", w.taskName)
	}

	// Use COPY protocol for bulk insert
	conn, err := w.backend.pool.Acquire(w.ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	copyCount, err := conn.Conn().CopyFrom(
		w.ctx,
		pgx.Identifier{w.tbl},
		rSchema.copyColumns,
		pgx.CopyFromRows(w.rows),
	)

	if err != nil {
		return fmt.Errorf("COPY failed: %w", err)
	}

	if int(copyCount) != len(w.rows) {
		return fmt.Errorf("expected to copy %d rows, but copied %d", len(w.rows), copyCount)
	}

	// Clear the buffer
	w.rows = w.rows[:0]

	return nil
}

// Flush flushes any buffered rows and commits the transaction.
func (w *PgxResultSet) Flush() error {
	// Flush any remaining buffered rows if using COPY
	if w.backend.opt.BatchInsert && len(w.rows) > 0 {
		if err := w.flushCopy(); err != nil {
			return err
		}
	}

	// Commit transaction for standard inserts
	if w.tx != nil {
		return w.tx.Commit(w.ctx)
	}

	return nil
}

// Close closes the result set and releases resources.
func (w *PgxResultSet) Close() error {
	defer w.cancel()

	if w.tx != nil {
		return w.tx.Rollback(w.ctx)
	}

	return nil
}

// createTableSchema generates SQL schemas for table operations.
func (p *PgxDB) createTableSchema(cols []string, colTypes []*sql.ColumnType) insertSchema {
	var (
		colNameHolder = make([]string, len(cols))
		colValHolder  = make([]string, len(cols))
		copyColumns   = make([]string, len(cols))
		fields        = make([]string, len(cols))
	)

	for i, col := range cols {
		quotedCol := fmt.Sprintf(`"%s"`, col)
		colNameHolder[i] = quotedCol
		colValHolder[i] = fmt.Sprintf("$%d", i+1)
		copyColumns[i] = col // Unquoted for COPY

		// Map SQL types to PostgreSQL types
		typ := mapColumnType(colTypes[i])

		// Add NOT NULL constraint if applicable
		if nullable, ok := colTypes[i].Nullable(); ok && !nullable {
			typ += " NOT NULL"
		}

		fields[i] = fmt.Sprintf("%s %s", quotedCol, typ)
	}

	// Build unlogged table modifier if requested
	unlogged := ""
	if p.opt.UnloggedTables {
		unlogged = "UNLOGGED"
	}

	return insertSchema{
		dropTable: `DROP TABLE IF EXISTS "%s" CASCADE`,
		createTable: fmt.Sprintf(`CREATE %s TABLE IF NOT EXISTS "%%s" (%s)`,
			unlogged, strings.Join(fields, ", ")),
		insertRow: fmt.Sprintf(`INSERT INTO "%%s" (%s) VALUES (%s)`,
			strings.Join(colNameHolder, ", "),
			strings.Join(colValHolder, ", ")),
		copyColumns: copyColumns,
	}
}

// mapColumnType maps database type names to PostgreSQL types.
func mapColumnType(colType *sql.ColumnType) string {
	typeName := colType.DatabaseTypeName()

	switch strings.ToUpper(typeName) {
	case "INT2", "INT4", "INT8", "TINYINT", "SMALLINT", "INT", "MEDIUMINT", "BIGINT":
		return "BIGINT"
	case "FLOAT4", "FLOAT8", "DECIMAL", "FLOAT", "DOUBLE", "NUMERIC", "FLOAT32", "FLOAT64":
		return "DECIMAL"
	case "TIMESTAMP", "DATETIME":
		return "TIMESTAMP"
	case "DATE":
		return "DATE"
	case "BOOLEAN":
		return "BOOLEAN"
	case "JSON", "JSONB":
		return "JSONB"
	case "VARCHAR":
		return "VARCHAR(255)"
	default:
		return "TEXT"
	}
}

// Close closes the database connection pool.
func (p *PgxDB) Close() error {
	p.pool.Close()
	return nil
}
