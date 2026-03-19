package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

// metricsClient emits timing and gauge metrics to a backend.
type metricsClient interface {
	// timing records a duration metric in milliseconds.
	timing(name string, ms int64, tags ...string)
	// gauge records a point-in-time value.
	gauge(name string, value int64, tags ...string)
	// close flushes any pending data.
	close()
}

// metricsFlags are CLI flags for configuring metrics emission.
type metricsFlags struct {
	StatsdAddr    string   `help:"DogStatsD address (host:port) for emitting metrics. Auto-detected from DD_AGENT_HOST if not set."`
	DatadogAPIKey string   `help:"DataDog API key for direct metric submission (no agent required)." env:"DATADOG_API_KEY"`
	MetricsTags   []string `help:"Additional metric tags in key:value format. May be repeated." name:"metrics-tag"`
}

// detectStatsdAddr returns the DogStatsD address from the environment, or empty
// if DD_AGENT_HOST is not set.
func detectStatsdAddr() string {
	host := os.Getenv("DD_AGENT_HOST")
	if host == "" {
		return ""
	}
	port := os.Getenv("DD_DOGSTATSD_PORT")
	if port == "" {
		port = "8125"
	}
	return net.JoinHostPort(host, port)
}

// newMetricsClient returns a metricsClient based on the configured flags.
// If no explicit backend is configured, auto-detects a local DD agent.
// Returns nil if no metrics backend is available.
func (f *metricsFlags) newMetricsClient() metricsClient {
	if f.StatsdAddr != "" {
		if c := newStatsdClient(f.StatsdAddr, f.MetricsTags); c != nil {
			return c
		}
		slog.Warn("failed to connect to DogStatsD, metrics disabled", "addr", f.StatsdAddr)
		return nil
	}
	if f.DatadogAPIKey != "" {
		return newDatadogAPIClient(f.DatadogAPIKey, f.MetricsTags)
	}
	// Auto-detect local DD agent.
	if addr := detectStatsdAddr(); addr != "" {
		if c := newStatsdClient(addr, f.MetricsTags); c != nil {
			slog.Debug("auto-detected DogStatsD agent", "addr", addr)
			return c
		}
	}
	return nil
}

// ── DogStatsD (UDP) ─────────────────────────────────────────────────────────

type statsdClient struct {
	conn net.Conn
	tags []string
}

func newStatsdClient(addr string, baseTags []string) *statsdClient {
	conn, err := net.DialTimeout("udp", addr, 2*time.Second)
	if err != nil {
		return nil
	}
	return &statsdClient{conn: conn, tags: baseTags}
}

func (s *statsdClient) timing(name string, ms int64, tags ...string) {
	s.send(fmt.Sprintf("%s:%d|d", name, ms), tags) // |d = distribution for DD percentile support
}

func (s *statsdClient) gauge(name string, value int64, tags ...string) {
	s.send(fmt.Sprintf("%s:%d|g", name, value), tags)
}

func (s *statsdClient) send(stat string, extraTags []string) {
	allTags := append(s.tags, extraTags...)
	if len(allTags) > 0 {
		stat += "|#" + strings.Join(allTags, ",")
	}
	s.conn.Write([]byte(stat)) //nolint:errcheck,gosec
}

func (s *statsdClient) close() {
	s.conn.Close() //nolint:errcheck,gosec
}

// ── DataDog HTTP API ────────────────────────────────────────────────────────

const datadogSeriesURL = "https://api.datadoghq.com/api/v2/series"

type datadogAPIClient struct {
	apiKey string
	tags   []string
	http   *http.Client
}

func newDatadogAPIClient(apiKey string, baseTags []string) *datadogAPIClient {
	return &datadogAPIClient{
		apiKey: apiKey,
		tags:   baseTags,
		http:   &http.Client{Timeout: 5 * time.Second},
	}
}

func (d *datadogAPIClient) timing(name string, ms int64, tags ...string) {
	d.submit(name, float64(ms), "distribution", tags)
}

func (d *datadogAPIClient) gauge(name string, value int64, tags ...string) {
	d.submit(name, float64(value), "gauge", tags)
}

func (d *datadogAPIClient) submit(name string, value float64, metricType string, extraTags []string) {
	allTags := append(d.tags, extraTags...)
	now := time.Now().Unix()

	payload := map[string]interface{}{
		"series": []map[string]interface{}{
			{
				"metric": name,
				"type":   metricType,
				"points": []map[string]interface{}{
					{
						"timestamp": now,
						"value":     value,
					},
				},
				"tags": allTags,
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return
	}

	req, err := http.NewRequest("POST", datadogSeriesURL, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("DD-API-KEY", d.apiKey)

	resp, err := d.http.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close() //nolint:errcheck,gosec
}

func (d *datadogAPIClient) close() {}

// emitTiming sends a timing metric if m is non-nil.
func emitTiming(m metricsClient, name string, ms int64, tags ...string) {
	if m != nil {
		m.timing(name, ms, tags...)
	}
}

// emitGauge sends a gauge metric if m is non-nil.
func emitGauge(m metricsClient, name string, value int64, tags ...string) {
	if m != nil {
		m.gauge(name, value, tags...)
	}
}
