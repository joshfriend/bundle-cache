package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/alecthomas/errors"
)

// cachewClient stores and retrieves Gradle cache bundles via cachew's generic
// object API, removing the need for AWS credentials on the client.
type cachewClient struct {
	baseURL string
	http    *http.Client
}

func newCachewClient(baseURL string) *cachewClient {
	return &cachewClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{},
	}
}

func (c *cachewClient) objectURL(commit, cacheKey string) string {
	return fmt.Sprintf("%s/api/v1/object/%s/%s",
		c.baseURL,
		url.PathEscape(cacheKey),
		url.PathEscape(commit),
	)
}

// stat returns (0, nil) when the bundle exists. Size is not used by this
// backend since cachew does not expose Content-Length on HEAD and parallel
// range downloads are handled server-side.
func (c *cachewClient) stat(ctx context.Context, commit, cacheKey string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, c.objectURL(commit, cacheKey), nil)
	if err != nil {
		return 0, err
	}
	resp, err := c.http.Do(req) //nolint:gosec
	if err != nil {
		return 0, err
	}
	io.Copy(io.Discard, resp.Body) //nolint:errcheck,gosec
	resp.Body.Close()              //nolint:errcheck,gosec
	if resp.StatusCode == http.StatusNotFound {
		return 0, errors.Errorf("cachew: not found for %.8s", commit)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, errors.Errorf("cachew HEAD %.8s: status %d", commit, resp.StatusCode)
	}
	return 0, nil
}

func (c *cachewClient) get(ctx context.Context, commit, cacheKey string, _ int64) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.objectURL(commit, cacheKey), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req) //nolint:gosec
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close() //nolint:errcheck,gosec
		return nil, errors.Errorf("cachew GET %.8s: status %d: %s", commit, resp.StatusCode, msg)
	}
	return resp.Body, nil
}

func (c *cachewClient) put(ctx context.Context, commit, cacheKey string, r io.ReadSeeker, size int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.objectURL(commit, cacheKey), r)
	if err != nil {
		return err
	}
	req.ContentLength = size
	req.Header.Set("Content-Type", "application/zstd")
	req.Header.Set("Time-To-Live", "168h") // 7 days
	resp, err := c.http.Do(req)            //nolint:gosec
	if err != nil {
		return err
	}
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	resp.Body.Close() //nolint:errcheck,gosec
	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("cachew POST %.8s: status %d: %s", commit, resp.StatusCode, msg)
	}
	return nil
}
