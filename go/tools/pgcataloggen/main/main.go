// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Command main generates the Go tables in go/common/pgcatalog from the
// vendored PostgreSQL catalog .dat files. Run from the repository root:
//
//	go run ./go/tools/pgcataloggen/main
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/multigres/multigres/go/tools/pgcataloggen"
)

func main() {
	dataDir := flag.String("data", "go/common/pgcatalog/data", "directory containing the vendored .dat files")
	outDir := flag.String("out", "go/common/pgcatalog", "directory to write generated Go files to")
	flag.Parse()

	cat, err := pgcataloggen.Load(*dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pgcataloggen: %v\n", err)
		os.Exit(1) //nolint:forbidigo // main() may exit directly.
	}
	if err := pgcataloggen.Render(cat, *outDir); err != nil {
		fmt.Fprintf(os.Stderr, "pgcataloggen: %v\n", err)
		os.Exit(1) //nolint:forbidigo // main() may exit directly.
	}
	fmt.Printf("pgcataloggen: %d types, %d procs, %d operators, %d casts\n",
		len(cat.Types), len(cat.Procs), len(cat.Operators), len(cat.Casts))
}
