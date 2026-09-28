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

// Command lint validates an OpenAPI document against the OpenAPI meta-schema.
package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/pb33f/libopenapi"
	validator "github.com/pb33f/libopenapi-validator"
)

func run() error {
	if len(os.Args) != 2 {
		return errors.New("usage: lint SPEC.yaml")
	}
	data, err := os.ReadFile(os.Args[1]) //nolint:gosec // CLI argument specifies the input file.
	if err != nil {
		return err
	}
	doc, err := libopenapi.NewDocument(data)
	if err != nil {
		return err
	}
	v, errs := validator.NewValidator(doc)
	if len(errs) != 0 {
		return fmt.Errorf("build OpenAPI model: %v", errs)
	}
	valid, failures := v.ValidateDocument()
	if !valid {
		return fmt.Errorf("invalid OpenAPI: %+v", failures)
	}
	fmt.Printf("%s: valid OpenAPI %s\n", os.Args[1], doc.GetVersion())
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1) //nolint:forbidigo // Command entry point.
	}
}
