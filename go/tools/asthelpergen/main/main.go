// Copyright 2025 Supabase, Inc.
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

package main

import (
	"flag"
	"log"
	"strings"

	"github.com/multigres/multigres/go/tools/asthelpergen"
	"github.com/multigres/multigres/go/tools/asthelpergen/internal"
)

func main() {
	var options asthelpergen.Options
	var verify bool
	var packagesStr string
	var cloneExcludeStr string

	flag.StringVar(&packagesStr, "in", ".", "Go packages to load the generator (comma-separated)")
	flag.StringVar(&options.RootInterface, "iface", "", "Root interface to generate helpers for (fully qualified)")
	flag.StringVar(&cloneExcludeStr, "clone_exclude", "", "Don't deep clone these types (comma-separated)")
	flag.BoolVar(&verify, "verify", false, "Ensure that the generated files are correct")
	flag.Parse()

	// Parse comma-separated lists
	if packagesStr != "" {
		options.Packages = strings.Split(packagesStr, ",")
		for i := range options.Packages {
			options.Packages[i] = strings.TrimSpace(options.Packages[i])
		}
	}

	if cloneExcludeStr != "" {
		options.Clone.Exclude = strings.Split(cloneExcludeStr, ",")
		for i := range options.Clone.Exclude {
			options.Clone.Exclude[i] = strings.TrimSpace(options.Clone.Exclude[i])
		}
	}

	// Validate required flags
	if options.RootInterface == "" {
		log.Fatal("--iface flag is required")
	}

	// Generate AST helpers
	result, err := asthelpergen.GenerateASTHelpers(&options)
	if err != nil {
		log.Fatal(err)
	}

	// Either verify or save the generated files
	if verify {
		errors := asthelpergen.VerifyFilesOnDisk(result)
		if len(errors) > 0 {
			for _, err := range errors {
				log.Println(err)
			}
			log.Fatalf("%d verification error(s) found", len(errors))
		}
		log.Printf("%d files OK", len(result))
	} else {
		for fullPath, file := range result {
			if err := internal.SaveJenFile(fullPath, file); err != nil {
				log.Fatal(err)
			}
		}
	}
}
