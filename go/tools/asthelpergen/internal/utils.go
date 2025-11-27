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

package internal

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"strings"
	"unicode"

	"github.com/dave/jennifer/jen"
	"golang.org/x/tools/go/packages"
)

// Title returns a copy of the string s with all Unicode letters that begin words
// mapped to their Unicode title case.
func Title(s string) string {
	prev := ' '
	return strings.Map(
		func(r rune) rune {
			if unicode.IsSpace(prev) {
				prev = r
				return unicode.ToTitle(r)
			}
			prev = r
			return r
		},
		s)
}

// FormatJenFile formats the given *jen.File with goimports and return a slice
// of byte corresponding to the formatted file.
func FormatJenFile(file *jen.File) ([]byte, error) {
	tempFile, err := os.CreateTemp("/tmp", "*.go")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tempFile.Name())

	err = file.Save(tempFile.Name())
	if err != nil {
		return nil, err
	}

	err = GoImports(tempFile.Name())
	if err != nil {
		return nil, err
	}
	return os.ReadFile(tempFile.Name())
}

// GoImports runs gofmt and goimports on the given file
func GoImports(fullPath string) error {
	// Run gofmt with simplification flag
	cmd := exec.CommandContext(context.TODO(), "gofmt", "-s", "-w", fullPath)
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return err
	}

	// Run goimports
	cmd = exec.CommandContext(context.TODO(), "go", "tool", "goimports", "-local", "github.com/supabase/multigres", "-w", fullPath)
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return err
	}

	return nil
}

// SaveJenFile saves a jen.File to disk and formats it
func SaveJenFile(fullPath string, file *jen.File) error {
	if err := file.Save(fullPath); err != nil {
		return err
	}
	if err := GoImports(fullPath); err != nil {
		return err
	}
	log.Printf("saved '%s'", fullPath)
	return nil
}

// CheckErrors checks for package loading errors, skipping generated files
func CheckErrors(loaded []*packages.Package) error {
	var errors []string
	for _, l := range loaded {
		for _, e := range l.Errors {
			if idx := strings.Index(e.Pos, ":"); idx >= 0 {
				filePath := e.Pos[:idx]
				_, fileName := path.Split(filePath)
				// Skip generated files
				if !isGeneratedFile(fileName) {
					errors = append(errors, e.Error())
				}
			} else {
				errors = append(errors, e.Error())
			}
		}
	}
	if len(errors) > 0 {
		return fmt.Errorf("found %d error(s) when loading Go packages:\n\t%s", len(errors), strings.Join(errors, "\n\t"))
	}
	return nil
}

// isGeneratedFile returns true if the filename is a generated file
func isGeneratedFile(filename string) bool {
	switch filename {
	case "ast_rewrite.go", "ast_clone.go", "rewriter_api.go":
		return true
	default:
		return false
	}
}
