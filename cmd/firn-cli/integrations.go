package main

// firn-cli integrations: operator surface for the integration binaries
// (ADR-016/017/018). `validate` checks an egress config; `lag` prints
// committed-offset to scanned-HWM backlog per route/partition. Neither
// mutates anything.

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/rahulkj/firn/internal/api/firnpb"
	"github.com/rahulkj/firn/internal/egress"
)

func cmdIntegrations(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("subcommands: validate -config egress.json | lag -config egress.json [-route name]")
	}
	switch args[0] {
	case "validate":
		return cmdIntegrationsValidate(args[1:])
	case "lag":
		return cmdIntegrationsLag(args[1:])
	default:
		return fmt.Errorf("unknown integrations subcommand %q (validate|lag)", args[0])
	}
}

func cmdIntegrationsValidate(args []string) error {
	fs := flag.NewFlagSet("integrations validate", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "egress config JSON (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *cfgPath == "" {
		return fmt.Errorf("-config is required")
	}
	cfg, err := egress.LoadConfig(*cfgPath)
	if err != nil {
		return err
	}
	fmt.Printf("config OK: %d route(s)\n", len(cfg.Routes))
	for _, r := range cfg.Routes {
		fmt.Printf("  - %s: source=%s sink=%s group=%s dlq=%q\n", r.Name, sourceString(r), r.Sink.Type, r.Group, r.DLQ)
	}
	return nil
}

func sourceString(r egress.Route) string {
	s := r.Sources[0]
	if s.Topic != "" {
		if len(s.Partitions) > 0 {
			return fmt.Sprintf("topic:%s partitions=%v", s.Topic, s.Partitions)
		}
		return "topic:" + s.Topic
	}
	return "queue:" + s.Queue
}

func cmdIntegrationsLag(args []string) error {
	fs := flag.NewFlagSet("integrations lag", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "egress config JSON (required)")
	routeFilter := fs.String("route", "", "only this route")
	maxScan := fs.Int64("max-scan", 10000, "cap records scanned per partition ('+' suffix when capped)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *cfgPath == "" {
		return fmt.Errorf("-config is required")
	}
	cfg, err := egress.LoadConfig(*cfgPath)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "ROUTE\tTOPIC\tPARTITION\tLAG")
	for _, r := range cfg.Routes {
		if *routeFilter != "" && r.Name != *routeFilter {
			continue
		}
		src := r.Sources[0]
		if src.Topic == "" {
			_, _ = fmt.Fprintf(tw, "%s\tqueue:%s\t-\t(lease-managed)\n", r.Name, src.Queue)
			continue
		}
		// Partition count from metadata.
		var meta *firnpb.GetMetadataResponse
		if m, err := metadata(ctx, true); err == nil {
			meta = m
		}
		partitions := src.Partitions
		if len(partitions) == 0 {
			n := 1
			if meta != nil {
				for _, t := range meta.Topics {
					if t.Name == src.Topic && t.Partitions > 0 {
						n = int(t.Partitions)
					}
				}
			}
			partitions = make([]int32, 0, n)
			for i := 0; i < n; i++ {
				partitions = append(partitions, int32(i))
			}
		}
		sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
		for _, p := range partitions {
			off := int64(0)
			found := false
			err := callWithRouting(ctx, src.Topic, p, func(client firnpb.FirnServiceClient) error {
				r, ferr := client.FetchOffset(ctx, &firnpb.FetchOffsetRequest{GroupId: r.Group, Topic: src.Topic, Partition: p})
				if ferr != nil {
					return ferr
				}
				off, found = r.Offset, r.Found
				return nil
			})
			if err != nil {
				_, _ = fmt.Fprintf(tw, "%s\t%s\tp%d\terror: %v\n", r.Name, src.Topic, p, err)
				continue
			}
			if !found {
				off = 0
			}
			lag := int64(0)
			cur := off
			capped := false
			for lag < *maxScan {
				var recs []*firnpb.Record
				ferr := callWithRouting(ctx, src.Topic, p, func(client firnpb.FirnServiceClient) error {
					resp, ferr := client.Fetch(ctx, &firnpb.FetchRequest{Topic: src.Topic, Partition: p, Offset: cur, MaxRecords: 500})
					if ferr != nil {
						return ferr
					}
					recs = resp.Records
					return nil
				})
				if ferr != nil {
					_ = ferr // treat as EOF-ish; stop scanning
					break
				}
				if len(recs) == 0 {
					break
				}
				lag += int64(len(recs))
				cur = recs[len(recs)-1].Offset + 1
				if len(recs) < 500 {
					break
				}
			}
			if lag >= *maxScan {
				capped = true
			}
			suffix := ""
			if capped {
				suffix = "+"
			}
			_, _ = fmt.Fprintf(tw, "%s\t%s\tp%d\t%d%s\n", r.Name, src.Topic, p, lag, suffix)
		}
	}
	return tw.Flush()
}
