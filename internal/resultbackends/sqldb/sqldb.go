// Package sqldb is a general SQL DB backend implementation that takes an stdlib
// sql.DB connection and creates tables and writes results to it.
// It has explicit support for MySQL and PostGres for handling differences in
// SQL dialects, but should ideally work with any standard SQL backend.
package sqldb

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/zerodha/dungbeetle/v2/models"
)

const (
	dbTypePostgres   = "postgres"
	dbTypeMysql      = "mysql"
	dbTypeClickHouse = "clickhouse"
)

// Opt represents SQL DB backend's options.
type Opt struct {
	DBType         string
	ResultsTable   string
	UnloggedTables bool
}

// SqlDB represents the SqlDB backend.
type SqlDB struct {
	db     *sql.DB
	opt    Opt
	logger *slog.Logger

	// The result schemas (CREATE TABLE ...) are dynamically
	// generated everytime queries are executed based on their result columns.
	// They're cached here so as to avoid repetetive generation.
	resTableSchemas map[string]insertSchema
	schemaMutex     sync.RWMutex
}

// SQLDBResultSet represents a writer that saves results
// to a sqlDB backend.
type SQLDBResultSet struct {
	jobID       string
	taskName    string
	colsWritten bool
	cols        []string
	tx          *sql.Tx
	tbl         string

	backend *SqlDB
}

// insertSchema contains the generated SQL for creating tables
// and inserting rows.
type insertSchema struct {
	dropTable   string
	createTable string
	insertRow   string
	insertRows  string // Base query for batch inserts (without value placeholders)
	colCount    int
}

type columnHolders struct {
	names  []string
	values []string
}

// NewSQLBackend returns a new sqlDB result backend instance.
func NewSQLBackend(db *sql.DB, opt Opt, lo *slog.Logger) (*SqlDB, error) {
	s := SqlDB{
		db:              db,
		opt:             opt,
		resTableSchemas: make(map[string]insertSchema),
		schemaMutex:     sync.RWMutex{},
		logger:          lo,
	}

	// Config.
	if opt.ResultsTable != "" {
		s.opt.ResultsTable = opt.ResultsTable
	} else {
		s.opt.ResultsTable = "results_%s"
	}

	return &s, nil
}

// NewResultSet returns a new instance of an sqlDB result writer.
// A new instance should be acquired for every individual job result
// to be written to the backend and then thrown away.
func (s *SqlDB) NewResultSet(jobID, taskName string, ttl time.Duration) (models.ResultSet, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}

	return &SQLDBResultSet{
		jobID:    jobID,
		taskName: taskName,
		backend:  s,
		tbl:      fmt.Sprintf(s.opt.ResultsTable, jobID),
		tx:       tx,
	}, nil
}

// RegisterColTypes registers the column types of a particular taskName's result set.
// Internally, it translates sql types into the simpler sqlDB (SQLite 3) types,
// creates a CREATE TABLE() schema for the results table with the structure of the
// particular taskName, and caches it be used for every subsequent result db creation
// and population. This should only be called once for each kind of taskName.
func (w *SQLDBResultSet) RegisterColTypes(cols []string, colTypes []*sql.ColumnType) error {
	if w.IsColTypesRegistered() {
		return errors.New("column types are already registered")
	}

	w.cols = make([]string, len(cols))
	copy(w.cols, cols)

	// Create the insert statement.
	// INSERT INTO xxx (col1, col2...) VALUES.
	var (
		colNameHolder = make([]string, len(cols))
		colValHolder  = make([]string, len(cols))
	)
	for i := range w.cols {
		colNameHolder[i] = fmt.Sprintf(`"%s"`, w.cols[i])

		// This will be filled by the driver.
		if w.backend.opt.DBType == dbTypePostgres {
			// Postgres placeholders are $1, $2 ...
			colValHolder[i] = fmt.Sprintf("$%d", i+1)
		} else {
			colValHolder[i] = "?"
		}
	}

	ins := fmt.Sprintf(`INSERT INTO "%%s" (%s) `, strings.Join(colNameHolder, ","))
	ins += fmt.Sprintf("VALUES (%s)", strings.Join(colValHolder, ","))

	w.backend.schemaMutex.Lock()
	w.backend.resTableSchemas[w.taskName] = w.backend.createTableSchema(cols, colTypes)
	w.backend.schemaMutex.Unlock()

	return nil
}

// IsColTypesRegistered checks whether the column types for a particular taskName's
// structure is registered in the backend.
func (w *SQLDBResultSet) IsColTypesRegistered() bool {
	w.backend.schemaMutex.RLock()
	_, ok := w.backend.resTableSchemas[w.taskName]
	w.backend.schemaMutex.RUnlock()

	return ok
}

// WriteCols writes the column (headers) of a result set to the backend.
// Internally, it creates a sqlDB database and creates a results table
// based on the schema RegisterColTypes() would've created and cached.
// This should only be called once on a ResultWriter instance.
func (w *SQLDBResultSet) WriteCols(cols []string) error {
	if w.colsWritten {
		return fmt.Errorf("columns for '%s' are already written", w.taskName)
	}

	w.backend.schemaMutex.RLock()
	rSchema, ok := w.backend.resTableSchemas[w.taskName]
	w.backend.schemaMutex.RUnlock()

	if !ok {
		return fmt.Errorf("column types for '%s' have not been registered", w.taskName)
	}

	// Create the results table.
	tx, err := w.backend.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(fmt.Sprintf(rSchema.dropTable, w.tbl)); err != nil {
		return err
	}

	if _, err := tx.Exec(fmt.Sprintf(rSchema.createTable, w.tbl)); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

// WriteRow writes an individual row from a result set to the backend.
// Internally, it INSERT()s the given row into the sqlDB results table.
func (w *SQLDBResultSet) WriteRow(row []interface{}) error {
	w.backend.schemaMutex.RLock()
	rSchema, ok := w.backend.resTableSchemas[w.taskName]
	w.backend.schemaMutex.RUnlock()

	if !ok {
		return fmt.Errorf("column types for '%s' have not been registered", w.taskName)
	}

	_, err := w.tx.Exec(fmt.Sprintf(rSchema.insertRow, w.tbl), row...)

	return err
}

func (w *SQLDBResultSet) WriteRows(rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	// Get the schema exactly like WriteRow does
	w.backend.schemaMutex.RLock()
	rSchema, ok := w.backend.resTableSchemas[w.taskName]
	w.backend.schemaMutex.RUnlock()
	if !ok {
		return fmt.Errorf("column types for '%s' have not been registered", w.taskName)
	}

	// Get optimal batch size based on database type and column count
	batchSize := w.backend.GetOptimalBatchSize(rSchema.colCount)

	// Process rows in batches
	for start := 0; start < len(rows); start += batchSize {
		end := start + batchSize
		if end > len(rows) {
			end = len(rows)
		}

		if err := w.insertBatch(rows[start:end], rSchema); err != nil {
			return fmt.Errorf("batch %d-%d failed: %w", start, end, err)
		}
	}

	return nil
}

var stringBuilderPool = sync.Pool{
	New: func() interface{} {
		return &strings.Builder{}
	},
}

// Modified insertBatch function
func (w *SQLDBResultSet) insertBatch(batchRows [][]interface{}, rSchema insertSchema) error {
	baseQuery := fmt.Sprintf(rSchema.insertRows, w.tbl)

	// Get builder from pool
	queryBuilder := stringBuilderPool.Get().(*strings.Builder)
	defer func() {
		queryBuilder.Reset()
		stringBuilderPool.Put(queryBuilder)
	}()

	// Pre-allocate capacity
	estimatedSize := len(baseQuery) + (len(batchRows) * rSchema.colCount * 10)
	queryBuilder.Grow(estimatedSize)

	queryBuilder.WriteString(baseQuery)

	// Pre-allocate values slice with exact capacity
	values := make([]interface{}, 0, len(batchRows)*rSchema.colCount)

	if w.backend.opt.DBType == dbTypePostgres {
		paramNum := 1
		for i, row := range batchRows {
			if i > 0 {
				queryBuilder.WriteString(",")
			}
			queryBuilder.WriteString(" (")
			for j := 0; j < rSchema.colCount; j++ {
				if j > 0 {
					queryBuilder.WriteString(", ")
				}
				queryBuilder.WriteString(fmt.Sprintf("$%d", paramNum))
				paramNum++
				if j < len(row) {
					values = append(values, row[j])
				} else {
					values = append(values, nil)
				}
			}
			queryBuilder.WriteString(")")
		}
	} else {
		// MySQL/ClickHouse logic remains same but uses pooled builder
		for i, row := range batchRows {
			if i > 0 {
				queryBuilder.WriteString(",")
			}
			queryBuilder.WriteString(" (")
			for j := 0; j < rSchema.colCount; j++ {
				if j > 0 {
					queryBuilder.WriteString(", ")
				}
				queryBuilder.WriteString("?")
				if j < len(row) {
					values = append(values, row[j])
				} else {
					values = append(values, nil)
				}
			}
			queryBuilder.WriteString(")")
		}
	}

	query := queryBuilder.String()

	_, err := w.tx.Exec(query, values...)
	if err != nil {
		return fmt.Errorf("exec failed: %v (had %d values for %d rows of %d columns)",
			err, len(values), len(batchRows), rSchema.colCount)
	}

	return nil
}

// Flush flushes the rows written into the sqlDB pipe.
func (w *SQLDBResultSet) Flush() error {
	err := w.tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

// Close closes the active sqlDB connection.
func (w *SQLDBResultSet) Close() error {
	if w.tx != nil {
		return w.tx.Rollback()
	}

	return nil
}

// createTableSchema takes an SQL query results, gets its column names and types,
// and generates a sqlDB CREATE TABLE() schema for the results.
func (s *SqlDB) createTableSchema(cols []string, colTypes []*sql.ColumnType) insertSchema {
	// Pool for column holders used in schema creation
	var colHolderPool = sync.Pool{
		New: func() interface{} {
			return &columnHolders{
				names:  make([]string, 0, 50),
				values: make([]string, 0, 50),
			}
		},
	}
	holders := colHolderPool.Get().(*columnHolders)

	// Resize if needed
	if cap(holders.names) < len(cols) {
		holders.names = make([]string, len(cols))
		holders.values = make([]string, len(cols))
	} else {
		holders.names = holders.names[:len(cols)]
		holders.values = holders.values[:len(cols)]
	}

	// Build column name holders and value placeholders
	for i := range cols {
		holders.names[i] = s.quoteIdentifier(cols[i])
		if s.opt.DBType == dbTypePostgres {
			holders.values[i] = fmt.Sprintf("$%d", i+1)
		} else {
			holders.values[i] = "?"
		}
	}

	// Copy to local variables before returning to pool
	colNameHolder := make([]string, len(holders.names))
	colValHolder := make([]string, len(holders.values))
	copy(colNameHolder, holders.names)
	copy(colValHolder, holders.values)

	// Return to pool early
	holders.names = holders.names[:0]
	holders.values = holders.values[:0]
	colHolderPool.Put(holders)

	// Continue with the rest of the function using local copies
	var (
		fields   = make([]string, len(cols))
		typ      string
		unlogged string
	)

	for i := 0; i < len(cols); i++ {
		typ = colTypes[i].DatabaseTypeName()
		switch typ {
		case "INT2", "INT4", "INT8", "TINYINT", "SMALLINT", "INT", "MEDIUMINT", "BIGINT":
			typ = "BIGINT"
		case "FLOAT4", "FLOAT8", "DECIMAL", "FLOAT", "DOUBLE", "NUMERIC":
			typ = "DECIMAL"
		case "TIMESTAMP", "DATETIME":
			typ = "TIMESTAMP"
		case "DATE":
			typ = "DATE"
		case "BOOLEAN":
			typ = "BOOLEAN"
		case "JSON", "JSONB":
			if s.opt.DBType == dbTypePostgres {
				typ = "JSONB"
			} else {
				typ = "JSON"
			}
		case "_INT4", "_INT8", "_TEXT":
			if s.opt.DBType != dbTypePostgres {
				typ = "TEXT"
			}
		case "VARCHAR":
			typ = "VARCHAR(255)"
		default:
			typ = "TEXT"
		}

		if nullable, ok := colTypes[i].Nullable(); ok && !nullable {
			typ += " NOT NULL"
		}

		fields[i] = fmt.Sprintf("%s %s", s.quoteIdentifier(cols[i]), typ)
	}

	if s.opt.DBType == dbTypePostgres && s.opt.UnloggedTables {
		unlogged = "UNLOGGED"
	}

	baseInsert := fmt.Sprintf("INSERT INTO %s (%s) VALUES",
		s.quoteIdentifier("%s"),
		strings.Join(colNameHolder, ","))

	return insertSchema{
		dropTable: fmt.Sprintf("DROP TABLE IF EXISTS %s;", s.quoteIdentifier("%s")),
		createTable: fmt.Sprintf("CREATE %s TABLE IF NOT EXISTS %s (%s);",
			unlogged,
			s.quoteIdentifier("%s"),
			strings.Join(fields, ",")),
		insertRow: fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			s.quoteIdentifier("%s"),
			strings.Join(colNameHolder, ","),
			strings.Join(colValHolder, ",")),
		insertRows: baseInsert,
		colCount:   len(cols),
	}
}

func (s *SqlDB) GetOptimalBatchSize(columnCount int) int {
	switch s.opt.DBType {
	case dbTypePostgres:
		const (
			pgMaxParams = 65535
			pgOptimal   = 3000
			pgMax       = 10000
		)

		maxPossible := (pgMaxParams - 100) / columnCount

		// For financial data with many columns (contract_note_items)
		if columnCount > 50 {
			maxPossible = maxPossible / 2 // Be conservative
		}

		switch {
		case maxPossible < 100:
			return 100
		case maxPossible < pgOptimal:
			return maxPossible
		case maxPossible > pgMax:
			return pgMax
		default:
			return pgOptimal
		}

	case dbTypeMysql:
		const (
			mysqlOptimal = 1000
			mysqlMax     = 5000
			mysqlParams  = 65535
		)

		maxPossible := mysqlParams / columnCount

		// Consider max_allowed_packet
		if columnCount > 100 {
			return 500 // Conservative for wide tables
		}

		if maxPossible < mysqlOptimal {
			return maxPossible
		} else if maxPossible > mysqlMax {
			return mysqlMax
		}
		return mysqlOptimal

	case dbTypeClickHouse:
		// ClickHouse can handle massive batches
		if columnCount > 100 {
			return 10000
		} else if columnCount > 50 {
			return 25000
		}
		return 50000

	default:
		// Conservative default
		return 500
	}
}

// quoteIdentifier quotes an identifier (table or column name) based on the database type
func (s *SqlDB) quoteIdentifier(name string) string {
	if s.opt.DBType == dbTypePostgres {
		return fmt.Sprintf(`"%s"`, name)
	}
	// MySQL uses backticks
	return fmt.Sprintf("`%s`", name)
}
