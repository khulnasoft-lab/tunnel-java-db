package dbtest

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/khulnasoft-lab/tunnel-java-db/pkg/db"
	"github.com/khulnasoft-lab/tunnel-java-db/pkg/types"
)

func InitDB(t *testing.T, indexes []types.Index) (db.DB, error) {
	tmpDir := db.Dir(t.TempDir())
	dbc, err := db.New(tmpDir)
	require.NoError(t, err)

	err = dbc.Init()
	require.NoError(t, err)

	err = dbc.InsertIndexes(indexes)
	require.NoError(t, err)
	return dbc, nil
}
