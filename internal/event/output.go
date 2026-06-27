package event

// OutputRecord is the result row written to object storage as Parquet.
// One row per (key, tumbling window).
type OutputRecord struct {
	Key           string  `parquet:"key"`
	WindowStart   int64   `parquet:"window_start"`
	WindowEnd     int64   `parquet:"window_end"`
	Count         int64   `parquet:"count"`
	SumAmount     float64 `parquet:"sum_amount"`
	MinAmount     float64 `parquet:"min_amount"`
	MaxAmount     float64 `parquet:"max_amount"`
	DistinctTypes int64   `parquet:"distinct_types"`
	CheckpointID  int64   `parquet:"checkpoint_id"` // 0 until Phase 4 wires checkpoints
}
