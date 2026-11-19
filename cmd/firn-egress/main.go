// firn-egress forwards selected Firn topics/queues to external systems
// (ADR-017, docs/plans/008-egress-integrations.md): file/object-stage drops,
// HTTP(S) webhooks, Databricks UC Volumes, and S3/MinIO. It runs outside
// firnd and consumes Firn only through the client API. Delivery is
// at-least-once; offsets/acks advance only after the sink confirms.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof" // optional debug endpoints when -debug is set
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"

	"github.com/rahulkj/firn/internal/egress"
	"github.com/rahulkj/firn/internal/integration"
	"github.com/rahulkj/firn/sdk/firngo"
)

func main() {
	cfgPath := flag.String("config", "", "path to egress config (routes)")
	route := flag.String("route", "", "run (or -lag) only this route (default: all)")
	validate := flag.Bool("validate", false, "validate config and exit")
	dumpRoutes := flag.Bool("routes", false, "list routes and exit")
	metricsAddr := flag.String("metrics-addr", "", "if set, serve Prometheus metrics on this address")
	lag := flag.Bool("lag", false, "print per-partition lag (committed offset to scanned HWM, capped 10000) and exit")
	debug := flag.String("debug", "", "if set, expose pprof on this address (e.g. 127.0.0.1:6060)")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("firn-egress ")

	if *cfgPath == "" {
		fmt.Fprintln(os.Stderr, "usage: firn-egress -config egress.json [-route name] [-validate|-routes|-lag] [-metrics-addr]")
		os.Exit(2)
	}
	cfg, err := egress.LoadConfig(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	if *validate {
		fmt.Printf("config OK: %d route(s)\n", len(cfg.Routes))
		for _, r := range cfg.Routes {
			fmt.Printf("  - %s: sink=%s dlq=%q\n", r.Name, r.Sink.Type, r.DLQ)
		}
		return
	}
	if *dumpRoutes {
		for _, r := range cfg.Routes {
			fmt.Printf("%s\n", r.Name)
		}
		return
	}

	fc := integration.FirnConn{Addr: cfg.Firn.Addr, APIKey: os.Getenv(cfg.Firn.APIKeyEnv), Insecure: cfg.Firn.Insecure}
	if cfg.Firn.CAEnv != "" {
		fc.CAFile = os.Getenv(cfg.Firn.CAEnv)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	client, err := integration.DialFirn(ctx, fc)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	if *lag {
		printLag(ctx, client, cfg, *route)
		return
	}

	metrics := integration.NewMetrics("egress")
	if *metricsAddr != "" {
		go func() {
			log.Printf("metrics listening on %s/metrics", *metricsAddr)
			if err := http.ListenAndServe(*metricsAddr, metrics.Handler()); err != nil {
				log.Printf("metrics listener failed: %v", err)
			}
		}()
	}
	if *debug != "" {
		go func() { _ = http.ListenAndServe(*debug, nil) }()
	}

	var wg sync.WaitGroup
	for _, r := range cfg.Routes {
		r := r
		if *route != "" && r.Name != *route {
			continue
		}
		src, err := buildSource(ctx, client, r)
		if err != nil {
			log.Fatalf("route %s: %v", r.Name, err)
		}
		sink, err := egress.NewSink(r)
		if err != nil {
			log.Fatalf("route %s: %v", r.Name, err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			runner := &egress.Runner{Route: r, Src: src, Sink: sink, Client: client, Metrics: metrics, Logger: log.Printf}
			log.Printf("route %q running: source=%s sink=%s group=%s", r.Name, src.Name(), r.Sink.Type, r.Group)
			if err := runner.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("route %q stopped with error: %v", r.Name, err)
			}
		}()
	}
	wg.Wait()
}

func buildSource(ctx context.Context, client *firngo.Client, r egress.Route) (egress.Source, error) {
	src := r.Sources[0]
	if src.Topic != "" {
		return egress.NewTopicSource(ctx, client, src.Topic, src.Partitions, r.Group)
	}
	return egress.NewQueueSource(client, src.Queue, r.Batch.MaxRecords, 30000), nil
}

// printLag estimates, per topic source, the backlog from the group's
// committed offset to the scanned HWM (capped at 10k records per partition).
// Queue sources report nothing (position is lease-managed).
func printLag(ctx context.Context, client *firngo.Client, cfg *egress.Config, routeFilter string) {
	for _, r := range cfg.Routes {
		if routeFilter != "" && r.Name != routeFilter {
			continue
		}
		src := r.Sources[0]
		if src.Topic == "" {
			fmt.Printf("%s\tqueue:%s\n", r.Name, src.Queue)
			continue
		}
		m, err := scanLag(ctx, client, src.Topic, src.Partitions, r.Group, 10000)
		if err != nil {
			fmt.Printf("%s\ttopic:%s\terror: %v\n", r.Name, src.Topic, err)
			continue
		}
		parts := make([]int, 0, len(m))
		for p := range m {
			parts = append(parts, int(p))
		}
		sort.Ints(parts)
		for _, p := range parts {
			suffix := ""
			if m[int32(p)] >= 10000 {
				suffix = "+"
			}
			fmt.Printf("%s\ttopic:%s\tp%d\t%d%s\n", r.Name, src.Topic, p, m[int32(p)], suffix)
		}
	}
}

func scanLag(ctx context.Context, client *firngo.Client, topic string, partitions []int32, group string, cap int64) (map[int32]int64, error) {
	meta, err := client.TopicMetadata(ctx, topic)
	if err != nil {
		return nil, err
	}
	n := int(meta.Partitions)
	if len(partitions) == 0 {
		partitions = make([]int32, 0, n)
		for i := 0; i < n; i++ {
			partitions = append(partitions, int32(i))
		}
	}
	out := map[int32]int64{}
	for _, p := range partitions {
		off, found, err := client.FetchOffset(ctx, group, topic, p)
		if err != nil {
			return nil, err
		}
		if !found {
			off = 0
		}
		count := int64(0)
		cur := off
		for count < cap {
			recs, err := client.Fetch(ctx, topic, p, cur, 500)
			if err != nil {
				break
			}
			if len(recs) == 0 {
				break
			}
			count += int64(len(recs))
			cur = recs[len(recs)-1].Offset + 1
			if len(recs) < 500 {
				break
			}
		}
		out[p] = count
	}
	return out, nil
}
