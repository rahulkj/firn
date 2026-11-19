// firn-egress forwards selected Firn topics/queues to external systems
// (ADR-017, docs/plans/008-egress-integrations.md): file/object-stage drops
// and HTTP(S) webhooks (IoT endpoints included). It runs outside firnd and
// consumes Firn only through the client API. Delivery is at-least-once;
// offsets/acks advance only after the sink confirms.
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
	"sync"
	"syscall"

	"github.com/rahulkj/firn/internal/egress"
	"github.com/rahulkj/firn/internal/integration"
	"github.com/rahulkj/firn/sdk/firngo"
)

func main() {
	cfgPath := flag.String("config", "", "path to egress config (routes)")
	route := flag.String("route", "", "run only this route (default: all)")
	validate := flag.Bool("validate", false, "validate config and exit")
	dumpRoutes := flag.Bool("routes", false, "list routes and exit")
	debug := flag.String("debug", "", "if set, expose pprof on this address (e.g. 127.0.0.1:6060)")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("firn-egress ")

	if *cfgPath == "" {
		fmt.Fprintln(os.Stderr, "usage: firn-egress -config egress.json [-route name] [-validate]")
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

	if *debug != "" {
		go func() {
			_ = http.ListenAndServe(*debug, nil)
		}()
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
			runner := &egress.Runner{Route: r, Src: src, Sink: sink, Client: client, Logger: log.Printf}
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
