// Package iceberg adds StreamForge's committed output to an Apache Iceberg table
// (spec Phase 8). It USES iceberg-go as a transactional sink: each completed
// checkpoint's committed Parquet rows become one Iceberg snapshot (append), so
// the table grows one snapshot per checkpoint and supports time-travel —
// querying the table "as of" any prior snapshot/checkpoint. We use Iceberg here;
// we do not build it.
package iceberg

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	ice "github.com/apache/iceberg-go"
	icecatalog "github.com/apache/iceberg-go/catalog"
	icesql "github.com/apache/iceberg-go/catalog/sql"
	icetable "github.com/apache/iceberg-go/table"
	_ "github.com/uptrace/bun/driver/sqliteshim"

	"github.com/Mxs8513/StreamForge/internal/event"
)

// tableSchema is the Iceberg schema of the aggregates table (matches
// event.OutputRecord). Field IDs are mirrored on the Arrow side so iceberg-go
// maps columns by id when it writes the data files.
var tableSchema = ice.NewSchema(0,
	ice.NestedField{ID: 1, Name: "key", Type: ice.PrimitiveTypes.String, Required: true},
	ice.NestedField{ID: 2, Name: "window_start", Type: ice.PrimitiveTypes.Int64, Required: true},
	ice.NestedField{ID: 3, Name: "window_end", Type: ice.PrimitiveTypes.Int64, Required: true},
	ice.NestedField{ID: 4, Name: "count", Type: ice.PrimitiveTypes.Int64, Required: true},
	ice.NestedField{ID: 5, Name: "sum_amount", Type: ice.PrimitiveTypes.Float64, Required: true},
	ice.NestedField{ID: 6, Name: "min_amount", Type: ice.PrimitiveTypes.Float64, Required: true},
	ice.NestedField{ID: 7, Name: "max_amount", Type: ice.PrimitiveTypes.Float64, Required: true},
	ice.NestedField{ID: 8, Name: "distinct_types", Type: ice.PrimitiveTypes.Int64, Required: true},
	ice.NestedField{ID: 9, Name: "checkpoint_id", Type: ice.PrimitiveTypes.Int64, Required: true},
)

func fieldID(id int) arrow.Metadata {
	return arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{fmt.Sprint(id)})
}

var arrowSchema = arrow.NewSchema([]arrow.Field{
	{Name: "key", Type: arrow.BinaryTypes.String, Metadata: fieldID(1)},
	{Name: "window_start", Type: arrow.PrimitiveTypes.Int64, Metadata: fieldID(2)},
	{Name: "window_end", Type: arrow.PrimitiveTypes.Int64, Metadata: fieldID(3)},
	{Name: "count", Type: arrow.PrimitiveTypes.Int64, Metadata: fieldID(4)},
	{Name: "sum_amount", Type: arrow.PrimitiveTypes.Float64, Metadata: fieldID(5)},
	{Name: "min_amount", Type: arrow.PrimitiveTypes.Float64, Metadata: fieldID(6)},
	{Name: "max_amount", Type: arrow.PrimitiveTypes.Float64, Metadata: fieldID(7)},
	{Name: "distinct_types", Type: arrow.PrimitiveTypes.Int64, Metadata: fieldID(8)},
	{Name: "checkpoint_id", Type: arrow.PrimitiveTypes.Int64, Metadata: fieldID(9)},
}, nil)

// Sink is an Iceberg table writer backed by a SQL (SQLite) catalog and a
// local-filesystem warehouse.
type Sink struct {
	cat   *icesql.Catalog
	ident icetable.Identifier
	tbl   *icetable.Table
}

// Open creates (or loads) the catalog and the aggregates table. catalogURI is a
// sqlite file (file://...); warehouse is a local directory for data files.
func Open(ctx context.Context, catalogURI, warehouse, namespace, tableName string) (*Sink, error) {
	c, err := icecatalog.Load(ctx, "streamforge", ice.Properties{
		"type":                "sql",
		"uri":                 catalogURI,
		icesql.DriverKey:      "sqlite",
		icesql.DialectKey:     string(icesql.SQLite),
		"init_catalog_tables": "true",
		"warehouse":           "file://" + warehouse,
	})
	if err != nil {
		return nil, fmt.Errorf("load catalog: %w", err)
	}
	cat := c.(*icesql.Catalog)

	ns := icetable.Identifier{namespace}
	_ = cat.CreateNamespace(ctx, ns, nil) // ignore "already exists"

	ident := icetable.Identifier{namespace, tableName}
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		tbl, err = cat.CreateTable(ctx, ident, tableSchema)
		if err != nil {
			return nil, fmt.Errorf("create table: %w", err)
		}
	}
	return &Sink{cat: cat, ident: ident, tbl: tbl}, nil
}

// AppendCheckpoint appends one checkpoint's rows as a new Iceberg snapshot.
func (s *Sink) AppendCheckpoint(ctx context.Context, checkpointID int64, rows []event.OutputRecord) error {
	if len(rows) == 0 {
		return nil
	}
	arr := buildArrowTable(rows)
	defer arr.Release()
	tbl, err := s.tbl.AppendTable(ctx, arr, arr.NumRows(), ice.Properties{
		"streamforge.checkpoint_id": fmt.Sprint(checkpointID),
	})
	if err != nil {
		return fmt.Errorf("append checkpoint %d: %w", checkpointID, err)
	}
	s.tbl = tbl
	return nil
}

// SnapshotInfo summarizes one Iceberg snapshot for the time-travel report.
type SnapshotInfo struct {
	ID           int64
	TimestampMs  int64
	TotalRecords string
}

// Snapshots returns the table's snapshot history, oldest first.
func (s *Sink) Snapshots() []SnapshotInfo {
	var out []SnapshotInfo
	for _, sn := range s.tbl.Metadata().Snapshots() {
		total := ""
		if sn.Summary != nil {
			total = sn.Summary.Properties["total-records"]
		}
		out = append(out, SnapshotInfo{ID: sn.SnapshotID, TimestampMs: sn.TimestampMs, TotalRecords: total})
	}
	return out
}

// CountAsOf returns the row count of the table as of a given snapshot id —
// a time-travel read.
func (s *Sink) CountAsOf(ctx context.Context, snapshotID int64) (int64, error) {
	scan := s.tbl.Scan(icetable.WithSnapshotID(snapshotID))
	arr, err := scan.ToArrowTable(ctx)
	if err != nil {
		return 0, err
	}
	defer arr.Release()
	return arr.NumRows(), nil
}

// CurrentSnapshotID returns the latest snapshot id (0 if none).
func (s *Sink) CurrentSnapshotID() int64 {
	if sn := s.tbl.CurrentSnapshot(); sn != nil {
		return sn.SnapshotID
	}
	return 0
}

func buildArrowTable(rows []event.OutputRecord) arrow.Table {
	pool := memory.DefaultAllocator
	key := array.NewStringBuilder(pool)
	ws := array.NewInt64Builder(pool)
	we := array.NewInt64Builder(pool)
	cnt := array.NewInt64Builder(pool)
	sum := array.NewFloat64Builder(pool)
	mn := array.NewFloat64Builder(pool)
	mx := array.NewFloat64Builder(pool)
	dt := array.NewInt64Builder(pool)
	cp := array.NewInt64Builder(pool)
	defer func() {
		for _, b := range []array.Builder{key, ws, we, cnt, sum, mn, mx, dt, cp} {
			b.Release()
		}
	}()
	for _, r := range rows {
		key.Append(r.Key)
		ws.Append(r.WindowStart)
		we.Append(r.WindowEnd)
		cnt.Append(r.Count)
		sum.Append(r.SumAmount)
		mn.Append(r.MinAmount)
		mx.Append(r.MaxAmount)
		dt.Append(r.DistinctTypes)
		cp.Append(r.CheckpointID)
	}
	cols := []arrow.Array{key.NewArray(), ws.NewArray(), we.NewArray(), cnt.NewArray(),
		sum.NewArray(), mn.NewArray(), mx.NewArray(), dt.NewArray(), cp.NewArray()}
	rec := array.NewRecordBatch(arrowSchema, cols, int64(len(rows)))
	defer rec.Release()
	for _, c := range cols {
		c.Release()
	}
	return array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{rec})
}
