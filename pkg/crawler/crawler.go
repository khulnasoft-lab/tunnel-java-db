package crawler

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/samber/lo"
	"golang.org/x/sync/semaphore"
)

const mavenRepoURL = "https://repo.maven.apache.org/maven2/"
const userAgent = "TunnelJavaDB/1.0 (+https://github.com/khulnasoft-lab/tunnel-java-db)"

// Rate limiting constants
const (
	defaultSleepMin           = 200 * time.Millisecond
	defaultSleepMax           = 500 * time.Millisecond
	rateLimit429SleepMin      = 2 * time.Second
	rateLimit429SleepMax      = 10 * time.Second
	rateLimit429BackoffFactor = 1.5
	maxBackoffMultiplier      = 20
)

type Crawler struct {
	dir  string
	http *retryablehttp.Client
	dbc  *db.DB

	rootUrl         string
	wg              sync.WaitGroup
	urlCh           chan string
	limit           *semaphore.Weighted
	wrongSHA1Values []string
	closeOnce       sync.Once
	errChClosed     atomic.Bool
	rateLimitHits   int
}

type Option struct {
	Limit    int64
	RootUrl  string
	CacheDir string
}

func NewCrawler(opt Option) (Crawler, error) {
	client := retryablehttp.NewClient()
	client.RetryMax = 15
	client.Logger = slog.Default()
	client.RetryWaitMin = 2 * time.Minute
	client.RetryWaitMax = 15 * time.Minute
	client.CheckRetry = retryableRateLimitPolicy
	client.Backoff = retryablehttp.LinearJitterBackoff

	if opt.RootUrl == "" {
		opt.RootUrl = mavenRepoURL
	}

	indexDir := filepath.Join(opt.CacheDir, "indexes")
	slog.Info("Index dir", slog.String("path", indexDir))

	var dbc db.DB
	dbDir := db.Dir(opt.CacheDir)
	if db.Exists(dbDir) {
		var err error
		dbc, err = db.New(dbDir)
		if err != nil {
			return Crawler{}, fmt.Errorf("unable to open DB: %w", err)
		}
		slog.Info("DB is used for crawler", slog.String("path", opt.CacheDir))
	}

	return Crawler{
		dir:  indexDir,
		http: client,
		dbc:  &dbc,
		rootUrl: opt.RootUrl,
		urlCh:   make(chan string, opt.Limit*10),
		limit:   semaphore.NewWeighted(opt.Limit),
	}, nil
}

func (c *Crawler) Crawl(ctx context.Context) error {
	slog.Info("Crawl maven repository and save indexes")
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	c.errChClosed.Store(false)

	doneCh := make(chan struct{})
	defer close(doneCh)

	// Start root URL crawl
	c.urlCh <- c.rootUrl
	c.wg.Add(1)

	go func() {
		c.wg.Wait()
		c.closeOnce.Do(func() {
			close(c.urlCh)
		})
		c.errChClosed.Store(true)
		close(errCh)
	}()

	crawlDone := make(chan struct{})
	go func() {
		defer close(crawlDone)
		c.handleURLs(ctx, errCh, doneCh)
	}()

loop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-crawlDone:
			break loop
		case err, ok := <-errCh:
			if !ok {
				break loop
			}
			c.errChClosed.Store(true)
			cancel()
			return err
		}
	}

	slog.Info("Crawl completed")
	for _, wrongSHA1 := range c.wrongSHA1Values {
		slog.Warn("Wrong SHA1 file", slog.String("error", wrongSHA1))
	}
	return nil
}

func (c *Crawler) handleURLs(ctx context.Context, errCh chan error, doneCh chan struct{}) {
	var count int
	for url := range c.urlCh {
		count++
		if count%1000 == 0 {
			slog.Info("Indexed digests", slog.Int("count", count))
		}

		if err := c.limit.Acquire(ctx, 1); err != nil {
			c.handleError(ctx, errCh, err, doneCh)
			return
		}
		go c.processURL(ctx, url, errCh, doneCh)
	}
}

func (c *Crawler) processURL(ctx context.Context, url string, errCh chan error, doneCh chan struct{}) {
	defer c.limit.Release(1)
	defer c.wg.Done()

	if err := c.Visit(ctx, url); err != nil {
		select {
		case <-ctx.Done():
		case errCh <- err:
		case <-doneCh:
		default:
		}
	}
}

func (c *Crawler) handleError(ctx context.Context, errCh chan error, err error, doneCh chan struct{}) {
	if !c.errChClosed.Load() {
		select {
		case errCh <- fmt.Errorf("semaphore acquire error: %w", err):
		case <-ctx.Done():
		case <-doneCh:
		default:
		}
	}
}

func (c *Crawler) Visit(ctx context.Context, url string) error {
	resp, err := c.httpGet(ctx, url)
	if err != nil {
		return fmt.Errorf("http get error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil
	}

	d, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return fmt.Errorf("can't create new goquery doc: %w", err)
	}

	var children []string
	d.Find("a").Each(func(i int, selection *goquery.Selection) {
		link := linkFromSelection(selection)
		if link == "maven-metadata.xml" {
			return
		} else if link == "../" || !strings.HasSuffix(link, "/") {
			return
		}
		children = append(children, link)
	})

	if meta, err := c.parseMetadata(ctx, url+"maven-metadata.xml"); err == nil && meta != nil {
		if err := c.crawlSHA1(ctx, url, meta, children); err != nil {
			return err
		}
		return nil
	}

	c.wg.Add(len(children))

	go func() {
		for _, child := range children {
			select {
			case <-ctx.Done():
				return
			default:
				c.urlCh <- url + child
			}
		}
	}()

	return nil
}

// retryableRateLimitPolicy is a custom CheckRetry function that handles 429 responses
func retryableRateLimitPolicy(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	retry, err := retryablehttp.DefaultRetryPolicy(ctx, resp, err)

	if resp.StatusCode == http.StatusTooManyRequests {
		slog.Warn("Rate limited (429)", slog.String("url", resp.Request.URL.String()))
		return true, nil
	}

	return retry, err
}
