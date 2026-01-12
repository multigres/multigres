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

package telemetry

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"

	consistent "go.opentelemetry.io/contrib/samplers/probability/consistent"
	"go.opentelemetry.io/otel/attribute"
	"gopkg.in/yaml.v3"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// SamplingConfig represents the YAML configuration for operation-aware sampling
type SamplingConfig struct {
	Categories map[string]CategoryConfig `yaml:"categories"`
	GRPC       GRPCSpanConfig            `yaml:"grpc"`
	HTTP       HTTPSpanConfig            `yaml:"http"`
	Spans      SpanConfig                `yaml:"spans"`
}

// CategoryConfig defines the sampling probability for a category
type CategoryConfig struct {
	Probability float64 `yaml:"probability"`
}

// GRPCSpanConfig configures sampling for gRPC spans
// Services provides service-level defaults (e.g., "/package.Service")
// Methods provides method-level overrides (e.g., "/package.Service/Method")
type GRPCSpanConfig struct {
	Services map[string]string `yaml:"services"`
	Methods  map[string]string `yaml:"methods"`
}

// HTTPSpanConfig configures sampling for HTTP spans
// Exact provides exact matches (e.g., "GET /live")
// Patterns provides glob patterns (e.g., "GET /api/*")
type HTTPSpanConfig struct {
	Exact    map[string]string `yaml:"exact"`
	Patterns map[string]string `yaml:"patterns"`
}

// SpanConfig configures sampling for manually created spans
// Exact provides exact matches
// Patterns provides glob patterns
type SpanConfig struct {
	Exact    map[string]string `yaml:"exact"`
	Patterns map[string]string `yaml:"patterns"`
}

// loadSamplingConfig loads and validates sampling configuration from a YAML file
func loadSamplingConfig(path string) (*SamplingConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read sampling config file: %w", err)
	}

	var config SamplingConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse sampling config YAML: %w", err)
	}

	// Validate categories
	if len(config.Categories) == 0 {
		return nil, errors.New("sampling config must define at least one category")
	}

	// Ensure "default" category exists
	if _, ok := config.Categories["default"]; !ok {
		return nil, errors.New("sampling config must define a 'default' category")
	}

	// Validate probabilities are in [0, 1]
	for name, cat := range config.Categories {
		if cat.Probability < 0 || cat.Probability > 1 {
			return nil, fmt.Errorf("category %q has invalid probability %f (must be 0.0-1.0)", name, cat.Probability)
		}
	}

	// Validate all span mappings reference defined categories
	allMappings := make(map[string]string)
	maps.Copy(allMappings, config.GRPC.Services)
	maps.Copy(allMappings, config.GRPC.Methods)
	maps.Copy(allMappings, config.HTTP.Exact)
	maps.Copy(allMappings, config.HTTP.Patterns)
	maps.Copy(allMappings, config.Spans.Exact)
	maps.Copy(allMappings, config.Spans.Patterns)

	for span, cat := range allMappings {
		if _, ok := config.Categories[cat]; !ok {
			return nil, fmt.Errorf("span %q references undefined category %q", span, cat)
		}
	}

	return &config, nil
}

// ConfigurableSampler wraps base samplers and routes sampling decisions based on
// a file-based configuration that maps span names to categories with different probabilities.
// This enables fine-grained control over sampling rates for different operation types.
type ConfigurableSampler struct {
	config     *SamplingConfig
	samplers   map[string]sdktrace.Sampler // category name -> sampler
	defaultCat string                      // default category name
}

// getAttributeValue finds an attribute value by key from the span attributes
func getAttributeValue(attrs []attribute.KeyValue, key string) (string, bool) {
	for _, attr := range attrs {
		if string(attr.Key) == key {
			return attr.Value.AsString(), true
		}
	}
	return "", false
}

// getCategoryForSpan determines the sampling category using span metadata.
// It uses SpanKind and semantic convention attributes to robustly identify span types:
//   - gRPC spans: identified by rpc.system="grpc" attribute
//   - HTTP spans: identified by http.method attribute and SpanKind.Server
//   - Other spans: use span name for matching
//
// Priority order:
//  1. gRPC method (from rpc.service + rpc.method attributes)
//  2. gRPC service (from rpc.service attribute)
//  3. HTTP exact match (from http.method + http.target)
//  4. HTTP pattern match
//  5. Manual span exact match (from span name)
//  6. Manual span pattern match
//  7. Default category
func (s *ConfigurableSampler) getCategoryForSpan(params sdktrace.SamplingParameters) string {
	// Check for gRPC span using semantic conventions
	// otelgrpc sets: rpc.system="grpc", rpc.service, rpc.method
	if rpcSystem, ok := getAttributeValue(params.Attributes, "rpc.system"); ok && rpcSystem == "grpc" {
		rpcService, hasService := getAttributeValue(params.Attributes, "rpc.service")
		rpcMethod, hasMethod := getAttributeValue(params.Attributes, "rpc.method")

		if hasService && hasMethod {
			// Check method-level configuration (e.g., "/package.Service/Method")
			fullMethod := "/" + rpcService + "/" + rpcMethod
			if cat, ok := s.config.GRPC.Methods[fullMethod]; ok {
				return cat
			}

			// Check service-level configuration (e.g., "/package.Service")
			serviceName := "/" + rpcService
			if cat, ok := s.config.GRPC.Services[serviceName]; ok {
				return cat
			}
		}

		// gRPC span but no config match - use default
		return s.defaultCat
	}

	// Check for HTTP span using semantic conventions
	// otelhttp sets: http.method, http.target (or http.route)
	if httpMethod, ok := getAttributeValue(params.Attributes, "http.method"); ok {
		// Prefer http.route (pattern) over http.target (actual path) for better matching
		httpPath, _ := getAttributeValue(params.Attributes, "http.route")
		if httpPath == "" {
			httpPath, _ = getAttributeValue(params.Attributes, "http.target")
		}

		if httpPath != "" {
			// Check exact match (e.g., "GET /live")
			fullPath := httpMethod + " " + httpPath
			if cat, ok := s.config.HTTP.Exact[fullPath]; ok {
				return cat
			}

			// Check pattern match using filepath.Match
			for pattern, cat := range s.config.HTTP.Patterns {
				if matched, _ := filepath.Match(pattern, fullPath); matched {
					return cat
				}
			}
		}

		// HTTP span but no config match - use default
		return s.defaultCat
	}

	// Not gRPC or HTTP - treat as manual span, use span name for matching
	spanName := params.Name

	// TODO: Consider adding custom semantic conventions for multigres-specific operations
	// to make matching more robust and explicit. For example:
	//   - multigres.operation.type: "maintenance", "recovery", "background"
	//   - multigres.operation.priority: "critical", "normal", "low"
	// This would allow matching by attributes instead of span names, making configuration
	// safer and less fragile than pattern matching. Example:
	//   if opType, ok := getAttributeValue(params.Attributes, "multigres.operation.type"); ok {
	//       // Use opType for category lookup
	//   }

	// Check exact match
	if cat, ok := s.config.Spans.Exact[spanName]; ok {
		return cat
	}

	// Check pattern match using filepath.Match
	for pattern, cat := range s.config.Spans.Patterns {
		if matched, _ := filepath.Match(pattern, spanName); matched {
			return cat
		}
	}

	// Fall back to default category
	return s.defaultCat
}

func (s *ConfigurableSampler) ShouldSample(params sdktrace.SamplingParameters) sdktrace.SamplingResult {
	// Determine category for this span using full span metadata
	category := s.getCategoryForSpan(params)

	// Get sampler for this category
	sampler, ok := s.samplers[category]
	if !ok {
		// Should not happen due to validation, but fallback to default
		sampler = s.samplers[s.defaultCat]
	}

	// Delegate to category sampler
	return sampler.ShouldSample(params)
}

func (s *ConfigurableSampler) Description() string {
	return fmt.Sprintf("ConfigurableSampler{categories=%d, default=%s}",
		len(s.config.Categories), s.defaultCat)
}

// maybeCreateCustomSampler creates a custom file-based sampler if configured, otherwise returns nil
// to defer to standard OpenTelemetry environment variable handling.
//
// Configuration:
//
//   - OTEL_TRACES_SAMPLER: sampler type
//
//   - "multigres_custom": use file-based configuration (reads OTEL_TRACES_SAMPLER_CONFIG)
//
//   - other values: defer to standard OTEL handling (always_on, always_off, parentbased_traceidratio, etc.)
//
//   - OTEL_TRACES_SAMPLER_CONFIG: path to YAML config file (only used when OTEL_TRACES_SAMPLER="multigres_custom")
//
// Returns (nil, nil) if standard OTEL sampling should be used, allowing OTEL SDK to handle
// all standard sampler types automatically via environment variables.
// Returns (nil, error) if custom sampling is requested but configuration fails.
func maybeCreateCustomSampler() (sdktrace.Sampler, error) {
	samplerType := os.Getenv("OTEL_TRACES_SAMPLER")

	// Only handle "multigres_custom" sampler type - defer everything else to OTEL
	if samplerType != "multigres_custom" {
		return nil, nil
	}

	// Load file-based configuration
	configPath := os.Getenv("OTEL_TRACES_SAMPLER_CONFIG")
	if configPath == "" {
		return nil, errors.New("OTEL_TRACES_SAMPLER=multigres_custom but OTEL_TRACES_SAMPLER_CONFIG not set")
	}

	config, err := loadSamplingConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load sampling config from %s: %w", configPath, err)
	}

	// Create samplers for each category
	samplers := make(map[string]sdktrace.Sampler)
	for name, cat := range config.Categories {
		samplers[name] = consistent.ProbabilityBased(cat.Probability)
	}

	rootSampler := &ConfigurableSampler{
		config:     config,
		samplers:   samplers,
		defaultCat: "default",
	}

	// Wrap with ParentBased to ensure complete distributed traces
	return sdktrace.ParentBased(rootSampler), nil
}
