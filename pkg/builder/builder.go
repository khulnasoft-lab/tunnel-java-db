package builder

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/cheggaaa/pb/v3"
	"k8s.io/utils/clock"

	"github.com/khulnasoft-lab/tunnel-java-db/pkg/crawler"
	"github.com/khulnasoft-lab/tunnel-java-db/pkg/db"
	"github.com/khulnasoft-lab/tunnel-java-db/pkg/fileutil"
	"github.com/khulnasoft-lab/tunnel-java-db/pkg/types"
)

const updateInterval = time.Hour * 72 // 3 days

type Builder struct {
	db    db.DB
	meta  db.Client
	clock clock.Clock
}

func NewBuilder(db db.DB, meta db.Client, clock clock.Clock) Builder {
	if clock == nil {
		clock = clock.RealClock{} // fallback to real clock if none is provided
	}
	return Builder{
		db:    db,
		meta:  meta,
		clock: clock,
	}
}

func (b *Builder) Build(cacheDir string) error {
	indexDir := filepath.Join(cacheDir, "indexes")
	count, err := fileutil.Count(indexDir)
	if err != nil {
		return fmt.Errorf("count error: %w", err)
	}
	bar := pb.StartNew(count)
	defer func() {
		slog.Info("Build completed")
		bar.Finish()
	}()

	var indexes []types.Index
	if err := fileutil.Walk(indexDir, func(r io.Reader, path string) error {
		index := &crawler.Index{}
		if err := json.NewDecoder(r).Decode(index); err != nil {
			return fmt.Errorf("failed to decode index: %w", err)
		}
		for _, ver := range index.Versions {
			indexes = append(indexes, types.Index{
				GroupID:     index.GroupID,
				ArtifactID:  index.ArtifactID,
				Version:     ver.Version,
				SHA1:        ver.SHA1,
				ArchiveType: index.ArchiveType,
			})
		}
		bar.Increment()

		// Insert in batches of 1000 indexes
		if len(indexes) > 1000 {
			if err := b.db.InsertIndexes(indexes); err != nil {
				return fmt.Errorf("failed to insert index to db: %w", err)
			}
			indexes = []types.Index{} // reset the batch
		}
		return nil
	}); err != nil {
		return fmt.Errorf("walk error: %w", err)
	}

	// Insert any remaining indexes after walking all files
	if len(indexes) > 0 {
		if err := b.db.InsertIndexes(indexes); err != nil {
			return fmt.Errorf("failed to insert remaining indexes to db: %w", err)
		}
	}

	// Vacuum the database to optimize performance
	if err := b.db.VacuumDB(); err != nil {
		return fmt.Errorf("failed to vacuum db: %w", err)
	}

	// Save metadata with updated timestamp
	metaDB := db.Metadata{
		Version:    db.SchemaVersion,
		NextUpdate: b.clock.Now().UTC().Add(updateInterval),
		UpdatedAt:  b.clock.Now().UTC(),
	}
	if err := b.meta.Update(metaDB); err != nil {
		return fmt.Errorf("failed to update metadata: %w", err)
	}

	return nil
}
