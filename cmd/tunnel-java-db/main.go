package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"golang.org/x/xerrors"

	"github.com/khulnasoft-lab/tunnel-java-db/pkg/builder"
	"github.com/khulnasoft-lab/tunnel-java-db/pkg/crawler"
	"github.com/khulnasoft-lab/tunnel-java-db/pkg/db"

	_ "modernc.org/sqlite"
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		slog.Error(fmt.Sprintf("%+v", err))
		os.Exit(1)
	}
}

var (
	// Used for flags.
	cacheDir string
	limit    int

	rootCmd = &cobra.Command{
		Use:   "tunnel-java-db",
		Short: "Build Java DB to store maven indexes",
	}
	crawlCmd = &cobra.Command{
		Use:   "crawl",
		Short: "Crawl maven indexes and save them into files",
		RunE: func(cmd *cobra.Command, args []string) error {
			return crawl(cmd.Context())
		},
	}
	buildCmd = &cobra.Command{
		Use:   "build",
		Short: "Build Java DB",
		RunE: func(cmd *cobra.Command, args []string) error {
			return build()
		},
	}
)

func init() {
	userCacheDir, err := os.UserCacheDir()
	if err != nil {
		panic(err)
	}

	rootCmd.PersistentFlags().StringVar(&cacheDir, "cache-dir", filepath.Join(userCacheDir, "tunnel-java-db"),
		"cache dir")
	rootCmd.PersistentFlags().IntVar(&limit, "limit", 300, "max parallelism")

	rootCmd.AddCommand(crawlCmd)
	rootCmd.AddCommand(buildCmd)

	slog.SetLogLoggerLevel(slog.LevelInfo) // TODO: add --debug
}

func crawl(ctx context.Context) error {
	c, err := crawler.NewCrawler(crawler.Option{
		Limit:    int64(limit),
		CacheDir: cacheDir,
	})
	if err != nil {
		return xerrors.Errorf("unable to create new Crawler: %w", err)
	}
	if err := c.Crawl(ctx); err != nil {
		return xerrors.Errorf("crawl error: %w", err)
	}
	return nil
}

func build() error {
	dbDir := db.Dir(cacheDir)
	slog.Info("Database", slog.String("path", dbDir))
	dbc, err := db.New(dbDir)
	if err != nil {
		return xerrors.Errorf("db create error: %w", err)
	}
	if !db.Exists(dbDir) {
		if err = dbc.Init(); err != nil {
			return xerrors.Errorf("db init error: %w", err)
		}
	}

	meta := db.NewMetadata(dbDir)
	b := builder.NewBuilder(dbc, meta)
	if err = b.Build(cacheDir); err != nil {
		return xerrors.Errorf("db build error: %w", err)
	}
	return nil
}
