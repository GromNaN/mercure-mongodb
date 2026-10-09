// Command load is a small SSE load generator for a Mercure hub.
//
// It opens many concurrent SSE subscriptions, publishes a stream of updates,
// and reports the end to end latency from publish to client. It is meant to
// measure a specific deployment on specific hardware. The numbers it prints
// are not a guarantee for other setups.
//
// Example:
//
//	go run ./load \
//	  -hub http://localhost:3000/hub \
//	  -publish-token "$PUBLISHER_JWT" \
//	  -subscribe-token "$SUBSCRIBER_JWT" \
//	  -clients 2000 -messages 2000
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	hub := flag.String("hub", "http://localhost:3000/hub", "hub URL")
	publishToken := flag.String("publish-token", "", "publisher JWT")
	subscribeToken := flag.String("subscribe-token", "", "subscriber JWT")
	topic := flag.String("topic", "https://example.com/load", "topic")
	clients := flag.Int("clients", 1000, "number of concurrent SSE connections")
	messages := flag.Int("messages", 1000, "number of updates to publish")
	rate := flag.Int("rate", 0, "updates per second, 0 for as fast as possible")
	timeout := flag.Duration("timeout", 2*time.Minute, "overall timeout")
	flag.Parse()

	if err := run(*hub, *publishToken, *subscribeToken, *topic, *clients, *messages, *rate, *timeout); err != nil {
		fmt.Fprintln(os.Stderr, "load:", err)
		os.Exit(1)
	}
}

func run(hub, publishToken, subscribeToken, topic string, clients, messages, rate int, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	latencies := &collector{}

	var (
		received  atomic.Int64
		connected sync.WaitGroup
	)

	connected.Add(clients)

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: clients + 10,
			DisableCompression:  true,
		},
	}

	target := int64(clients) * int64(messages)

	clientCtx, stopClients := context.WithCancel(ctx)
	defer stopClients()

	for range clients {
		go subscribe(clientCtx, client, hub, subscribeToken, topic, latencies, &received, &connected, target, stopClients)
	}

	// Wait for every client to be connected before publishing, so the measured
	// latency is not polluted by connection setup.
	waitWithTimeout(&connected, 30*time.Second)

	start := time.Now()
	if err := publish(ctx, client, hub, publishToken, topic, messages, rate); err != nil {
		return fmt.Errorf("publishing: %w", err)
	}
	publishDone := time.Since(start)

	// Wait for the clients to drain, or for the timeout.
	deadline := time.Now().Add(timeout)
	for received.Load() < target && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	elapsed := time.Since(start)
	got := received.Load()

	fmt.Printf("clients:            %d\n", clients)
	fmt.Printf("messages published: %d\n", messages)
	fmt.Printf("deliveries:         %d of %d expected\n", got, target)
	fmt.Printf("publish duration:   %s\n", publishDone.Round(time.Millisecond))
	fmt.Printf("elapsed:            %s\n", elapsed.Round(time.Millisecond))

	if got == 0 {
		return nil
	}

	p50, p90, p99, max := latencies.percentiles()
	fmt.Printf("latency p50:        %s\n", p50.Round(time.Millisecond))
	fmt.Printf("latency p90:        %s\n", p90.Round(time.Millisecond))
	fmt.Printf("latency p99:        %s\n", p99.Round(time.Millisecond))
	fmt.Printf("latency max:        %s\n", max.Round(time.Millisecond))

	return nil
}

func waitWithTimeout(wg *sync.WaitGroup, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
	}
}

func subscribe(
	ctx context.Context,
	client *http.Client,
	hub, token, topic string,
	latencies *collector,
	received *atomic.Int64,
	connected *sync.WaitGroup,
	target int64,
	stop func(),
) {
	defer connected.Done()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, hub+"?topic="+topic, nil)
	if err != nil {
		return
	}

	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := client.Do(req)
	if err != nil {
		return
	}

	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return
	}

	reader := bufio.NewReaderSize(resp.Body, 64*1024)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		if !strings.HasPrefix(line, "data:") {
			continue
		}

		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		parts := strings.SplitN(payload, " ", 2)
		if len(parts) != 2 {
			continue
		}

		sentAt, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			continue
		}

		latencies.add(time.Since(time.Unix(0, sentAt)))

		if received.Add(1) >= target {
			stop()

			return
		}
	}
}

func publish(ctx context.Context, client *http.Client, hub, token, topic string, messages, rate int) error {
	var interval time.Duration
	if rate > 0 {
		interval = time.Second / time.Duration(rate)
	}

	for i := range messages {
		if interval > 0 && i > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err() //nolint:wrapcheck
			case <-time.After(interval):
			}
		}

		body := fmt.Sprintf("topic=%s&data=%d %d", topic, i, time.Now().UnixNano())

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, hub, strings.NewReader(body))
		if err != nil {
			return err
		}

		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}

		resp, err := client.Do(req)
		if err != nil {
			return err
		}

		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()

		if resp.StatusCode >= 300 {
			return fmt.Errorf("unexpected status %d", resp.StatusCode)
		}
	}

	return nil
}

type collector struct {
	mu   sync.Mutex
	data []time.Duration
}

func (c *collector) add(d time.Duration) {
	c.mu.Lock()
	c.data = append(c.data, d)
	c.mu.Unlock()
}

func (c *collector) percentiles() (time.Duration, time.Duration, time.Duration, time.Duration) {
	c.mu.Lock()
	data := make([]time.Duration, len(c.data))
	copy(data, c.data)
	c.mu.Unlock()

	sort.Slice(data, func(i, j int) bool { return data[i] < data[j] })

	at := func(p float64) time.Duration {
		if len(data) == 0 {
			return 0
		}

		idx := int(math.Ceil(p*float64(len(data)))) - 1
		if idx < 0 {
			idx = 0
		}

		if idx >= len(data) {
			idx = len(data) - 1
		}

		return data[idx]
	}

	return at(0.50), at(0.90), at(0.99), data[len(data)-1]
}
