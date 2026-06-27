package storage

import (
	"bytes"

	"github.com/parquet-go/parquet-go"

	"github.com/Mxs8513/StreamForge/internal/event"
)

// EncodeParquet serializes output records into a Parquet file in memory.
// Columnar Parquet is the on-disk/object format every data platform reads.
func EncodeParquet(rows []event.OutputRecord) ([]byte, error) {
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[event.OutputRecord](&buf)
	if _, err := w.Write(rows); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeParquet reads output records back from a Parquet file. Used by the
// reconciliation/aggregation tests to verify committed output.
func DecodeParquet(data []byte) ([]event.OutputRecord, error) {
	rows, err := parquet.Read[event.OutputRecord](bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}
	return rows, nil
}
