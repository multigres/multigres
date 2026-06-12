// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Command metricsgen regenerates the Multigres metric catalog
// (go/observability/metriccatalog/catalog_generated.go) by scanning the
// codebase for OpenTelemetry instrument definitions.
//
// Run from the repository root:
//
//	go run ./go/tools/metricsgen/main          # write the catalog
//	go run ./go/tools/metricsgen/main -verify  # fail if it is stale
package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/multigres/multigres/go/tools/metricsgen"
)

func main() {
	var (
		out     = flag.String("out", "go/observability/metriccatalog/catalog_generated.go", "Path of the generated catalog file")
		pattern = flag.String("pattern", "./go/...", "Package pattern to scan for metric definitions")
		dir     = flag.String("dir", ".", "Directory to load packages from (repository root)")
		verify  = flag.Bool("verify", false, "Verify the catalog on disk is up to date instead of writing it")
	)
	flag.Parse()

	metrics, err := metricsgen.Collect(*dir, *pattern)
	if err != nil {
		log.Fatalf("collecting metrics: %v", err)
	}
	if len(metrics) == 0 {
		log.Fatalf("no metrics found scanning %q; this is almost certainly a bug in the generator", *pattern)
	}

	rendered, err := metricsgen.Render(metrics)
	if err != nil {
		log.Fatalf("rendering catalog: %v", err)
	}

	if *verify {
		existing, err := os.ReadFile(*out)
		if err != nil {
			log.Fatalf("reading %s for verification: %v", *out, err)
		}
		if !bytes.Equal(existing, rendered) {
			log.Fatalf("%s is out of date; run 'make metrics' and commit the result", *out)
		}
		fmt.Printf("%s is up to date (%d metrics)\n", *out, len(metrics))
		return
	}

	if err := os.WriteFile(*out, rendered, 0o644); err != nil {
		log.Fatalf("writing %s: %v", *out, err)
	}
	fmt.Printf("wrote %s (%d metrics)\n", *out, len(metrics))
}
