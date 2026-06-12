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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Test constants for category names
const (
	testCategoryDefault    = "default"
	testCategoryMonitoring = "monitoring"
	testCategoryOperations = "operations"
	testCategoryQueries    = "queries"
)

// Test constant for custom sampler type
const testSamplerTypeCustom = "multigres_custom"

// createTestConfig creates a temporary YAML config file for testing.
// Returns the absolute path to the created file.
func createTestConfig(t *testing.T, yamlContent string) string {
	t.Helper()
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "sampling.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte(yamlContent), 0o644))
	return configPath
}

func TestLoadSamplingConfig_Valid(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "sampling.yaml")

	configYAML := `
categories:
  default:
    probability: 1.0
  operations:
    probability: 1.0
  queries:
    probability: 0.001
  monitoring:
    probability: 0.1

grpc:
  services:
    /multigres.Manager: operations
  methods:
    /multigres.Gateway/Query: queries

http:
  exact:
    GET /live: monitoring
    GET /ready: monitoring
  patterns:
    GET /api/*: queries

spans:
  exact:
    bootstrap: operations
  patterns:
    health*: monitoring
`

	require.NoError(t, os.WriteFile(configPath, []byte(configYAML), 0o644))

	config, err := loadSamplingConfig(configPath)
	require.NoError(t, err)
	require.NotNil(t, config)

	require.Equal(t, 4, len(config.Categories))
	require.Equal(t, 1.0, config.Categories["default"].Probability)
	require.Equal(t, 0.001, config.Categories["queries"].Probability)
	require.Equal(t, 0.1, config.Categories["monitoring"].Probability)

	require.Equal(t, "operations", config.GRPC.Services["/multigres.Manager"])
	require.Equal(t, "queries", config.GRPC.Methods["/multigres.Gateway/Query"])
	require.Equal(t, "monitoring", config.HTTP.Exact["GET /live"])
	require.Equal(t, "queries", config.HTTP.Patterns["GET /api/*"])
	require.Equal(t, "operations", config.Spans.Exact["bootstrap"])
	require.Equal(t, "monitoring", config.Spans.Patterns["health*"])
}

func TestLoadSamplingConfig_MissingDefault(t *testing.T) {
	configPath := createTestConfig(t, `
categories:
  queries:
    probability: 0.1
`)

	_, err := loadSamplingConfig(configPath)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must define a 'default' category")
}

func TestLoadSamplingConfig_EmptyCategories(t *testing.T) {
	configPath := createTestConfig(t, `
categories: {}
`)

	_, err := loadSamplingConfig(configPath)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must define at least one category")
}

func TestLoadSamplingConfig_InvalidProbability(t *testing.T) {
	tests := []struct {
		name        string
		probability string
	}{
		{"negative", "-0.1"},
		{"greater than one", "1.5"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			configPath := filepath.Join(tmpDir, "sampling.yaml")

			configYAML := `
categories:
  default:
    probability: ` + tt.probability + `
`

			require.NoError(t, os.WriteFile(configPath, []byte(configYAML), 0o644))

			_, err := loadSamplingConfig(configPath)
			require.Error(t, err)
			require.Contains(t, err.Error(), "invalid probability")
		})
	}
}

func TestLoadSamplingConfig_UndefinedCategory(t *testing.T) {
	configPath := createTestConfig(t, `
categories:
  default:
    probability: 1.0

grpc:
  services:
    /multigres.Gateway: undefined_category
`)

	_, err := loadSamplingConfig(configPath)
	require.Error(t, err)
	require.Contains(t, err.Error(), "references undefined category")
}

func TestLoadSamplingConfig_FileNotFound(t *testing.T) {
	_, err := loadSamplingConfig("/nonexistent/path/config.yaml")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to read sampling config file")
}

func TestLoadSamplingConfig_InvalidYAML(t *testing.T) {
	configPath := createTestConfig(t, `
categories:
  default: invalid yaml structure without probability field
`)

	_, err := loadSamplingConfig(configPath)
	require.Error(t, err)
	// YAML parsing fails before our validation runs
	require.Contains(t, err.Error(), "failed to parse sampling config YAML")
}

func TestGetCategoryForSpan_GRPC(t *testing.T) {
	config := &SamplingConfig{
		Categories: map[string]CategoryConfig{
			"default":    {Probability: 1.0},
			"operations": {Probability: 1.0},
			"queries":    {Probability: 0.1},
		},
		GRPC: GRPCSpanConfig{
			Services: map[string]string{
				"/multigres.Manager": "operations",
			},
			Methods: map[string]string{
				"/multigres.Gateway/Query": "queries",
			},
		},
	}

	sampler := &ConfigurableSampler{
		config:     config,
		defaultCat: testCategoryDefault,
	}

	tests := []struct {
		name     string
		attrs    []attribute.KeyValue
		expected string
	}{
		{
			name: "gRPC method match",
			attrs: []attribute.KeyValue{
				attribute.String("rpc.system", "grpc"),
				attribute.String("rpc.service", "multigres.Gateway"),
				attribute.String("rpc.method", "Query"),
			},
			expected: testCategoryQueries,
		},
		{
			name: "gRPC service match",
			attrs: []attribute.KeyValue{
				attribute.String("rpc.system", "grpc"),
				attribute.String("rpc.service", "multigres.Manager"),
				attribute.String("rpc.method", "SomeMethod"),
			},
			expected: testCategoryOperations,
		},
		{
			name: "gRPC no match",
			attrs: []attribute.KeyValue{
				attribute.String("rpc.system", "grpc"),
				attribute.String("rpc.service", "unknown.Service"),
				attribute.String("rpc.method", "SomeMethod"),
			},
			expected: testCategoryDefault,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := sdktrace.SamplingParameters{
				Attributes: tt.attrs,
			}

			category := sampler.getCategoryForSpan(params)
			require.Equal(t, tt.expected, category)
		})
	}
}

func TestGetCategoryForSpan_HTTP(t *testing.T) {
	config := &SamplingConfig{
		Categories: map[string]CategoryConfig{
			"default":    {Probability: 1.0},
			"monitoring": {Probability: 0.1},
			"queries":    {Probability: 0.01},
		},
		HTTP: HTTPSpanConfig{
			Exact: map[string]string{
				"GET /live":  "monitoring",
				"GET /ready": "monitoring",
			},
			Patterns: map[string]string{
				"GET /api/*": "queries",
			},
		},
	}

	sampler := &ConfigurableSampler{
		config:     config,
		defaultCat: testCategoryDefault,
	}

	tests := []struct {
		name     string
		attrs    []attribute.KeyValue
		expected string
	}{
		{
			name: "HTTP exact match",
			attrs: []attribute.KeyValue{
				attribute.String("http.method", "GET"),
				attribute.String("http.target", "/live"),
			},
			expected: testCategoryMonitoring,
		},
		{
			name: "HTTP pattern match",
			attrs: []attribute.KeyValue{
				attribute.String("http.method", "GET"),
				attribute.String("http.target", "/api/users"),
			},
			expected: testCategoryQueries,
		},
		{
			name: "HTTP route preference",
			attrs: []attribute.KeyValue{
				attribute.String("http.method", "GET"),
				attribute.String("http.route", "/ready"),
				attribute.String("http.target", "/ready?foo=bar"),
			},
			expected: testCategoryMonitoring,
		},
		{
			name: "HTTP no match",
			attrs: []attribute.KeyValue{
				attribute.String("http.method", "POST"),
				attribute.String("http.target", "/unknown"),
			},
			expected: testCategoryDefault,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := sdktrace.SamplingParameters{
				Attributes: tt.attrs,
			}

			category := sampler.getCategoryForSpan(params)
			require.Equal(t, tt.expected, category)
		})
	}
}

func TestGetCategoryForSpan_ManualSpans(t *testing.T) {
	config := &SamplingConfig{
		Categories: map[string]CategoryConfig{
			"default":    {Probability: 1.0},
			"operations": {Probability: 1.0},
			"monitoring": {Probability: 0.1},
		},
		Spans: SpanConfig{
			Exact: map[string]string{
				"bootstrap": "operations",
				"failover":  "operations",
			},
			Patterns: map[string]string{
				"health*": "monitoring",
			},
		},
	}

	sampler := &ConfigurableSampler{
		config:     config,
		defaultCat: testCategoryDefault,
	}

	tests := []struct {
		name     string
		spanName string
		expected string
	}{
		{
			name:     "exact match",
			spanName: "bootstrap",
			expected: testCategoryOperations,
		},
		{
			name:     "pattern match",
			spanName: "healthcheck",
			expected: testCategoryMonitoring,
		},
		{
			name:     "no match",
			spanName: "random_operation",
			expected: testCategoryDefault,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := sdktrace.SamplingParameters{
				Name: tt.spanName,
			}

			category := sampler.getCategoryForSpan(params)
			require.Equal(t, tt.expected, category)
		})
	}
}

func TestConfigurableSampler_ShouldSample(t *testing.T) {
	config := &SamplingConfig{
		Categories: map[string]CategoryConfig{
			"default": {Probability: 1.0},
			"never":   {Probability: 0.0},
		},
		Spans: SpanConfig{
			Exact: map[string]string{
				"never_sample": "never",
			},
		},
	}

	samplers := make(map[string]sdktrace.Sampler)
	samplers[testCategoryDefault] = sdktrace.AlwaysSample()
	samplers["never"] = sdktrace.NeverSample()

	sampler := &ConfigurableSampler{
		config:     config,
		samplers:   samplers,
		defaultCat: testCategoryDefault,
	}

	t.Run("always sample", func(t *testing.T) {
		params := sdktrace.SamplingParameters{
			Name: "some_span",
		}

		result := sampler.ShouldSample(params)
		require.Equal(t, sdktrace.RecordAndSample, result.Decision)
	})

	t.Run("never sample", func(t *testing.T) {
		params := sdktrace.SamplingParameters{
			Name: "never_sample",
		}

		result := sampler.ShouldSample(params)
		require.Equal(t, sdktrace.Drop, result.Decision)
	})
}

func TestConfigurableSampler_ShouldSample_MissingCategory(t *testing.T) {
	config := &SamplingConfig{
		Categories: map[string]CategoryConfig{
			"default": {Probability: 1.0},
		},
		Spans: SpanConfig{
			Exact: map[string]string{
				"test_span": "missing_category",
			},
		},
	}

	samplers := make(map[string]sdktrace.Sampler)
	samplers[testCategoryDefault] = sdktrace.AlwaysSample()

	sampler := &ConfigurableSampler{
		config:     config,
		samplers:   samplers,
		defaultCat: testCategoryDefault,
	}

	params := sdktrace.SamplingParameters{
		Name: "test_span",
	}

	// Should fallback to default sampler
	result := sampler.ShouldSample(params)
	require.Equal(t, sdktrace.RecordAndSample, result.Decision)
}

func TestConfigurableSampler_Description(t *testing.T) {
	config := &SamplingConfig{
		Categories: map[string]CategoryConfig{
			"default": {Probability: 1.0},
			"queries": {Probability: 0.1},
		},
	}

	sampler := &ConfigurableSampler{
		config:     config,
		defaultCat: testCategoryDefault,
	}

	desc := sampler.Description()
	require.Contains(t, desc, "ConfigurableSampler")
	require.Contains(t, desc, "categories=2")
	require.Contains(t, desc, "default=default")
}

func TestMaybeCreateCustomSampler_NotCustom(t *testing.T) {
	// Should return nil when sampler type is not "multigres_custom"
	t.Setenv("OTEL_TRACES_SAMPLER", "always_on")

	sampler, err := maybeCreateCustomSampler()
	require.NoError(t, err)
	require.Nil(t, sampler)
}

func TestMaybeCreateCustomSampler_MissingConfig(t *testing.T) {
	t.Setenv("OTEL_TRACES_SAMPLER", testSamplerTypeCustom)
	t.Setenv("OTEL_TRACES_SAMPLER_CONFIG", "")

	sampler, err := maybeCreateCustomSampler()
	require.Error(t, err)
	require.Nil(t, sampler)
	require.Contains(t, err.Error(), "OTEL_TRACES_SAMPLER_CONFIG not set")
}

func TestMaybeCreateCustomSampler_InvalidConfigFile(t *testing.T) {
	t.Setenv("OTEL_TRACES_SAMPLER", testSamplerTypeCustom)
	t.Setenv("OTEL_TRACES_SAMPLER_CONFIG", "/nonexistent/config.yaml")

	sampler, err := maybeCreateCustomSampler()
	require.Error(t, err)
	require.Nil(t, sampler)
	require.Contains(t, err.Error(), "failed to load sampling config")
}

func TestMaybeCreateCustomSampler_Success(t *testing.T) {
	configPath := createTestConfig(t, `
categories:
  default:
    probability: 1.0
  queries:
    probability: 0.1
`)

	t.Setenv("OTEL_TRACES_SAMPLER", testSamplerTypeCustom)
	t.Setenv("OTEL_TRACES_SAMPLER_CONFIG", configPath)

	sampler, err := maybeCreateCustomSampler()
	require.NoError(t, err)
	require.NotNil(t, sampler)

	// Verify it's wrapped with ParentBased
	desc := sampler.Description()
	require.Contains(t, desc, "ParentBased")
}

func TestGetAttributeValue(t *testing.T) {
	attrs := []attribute.KeyValue{
		attribute.String("key1", "value1"),
		attribute.Int("key2", 42),
		attribute.Bool("key3", true),
	}

	t.Run("found string", func(t *testing.T) {
		value, ok := getAttributeValue(attrs, "key1")
		require.True(t, ok)
		require.Equal(t, "value1", value)
	})

	t.Run("not found", func(t *testing.T) {
		value, ok := getAttributeValue(attrs, "nonexistent")
		require.False(t, ok)
		require.Equal(t, "", value)
	})
}

func TestConfigurableSampler_ParentBasedBehavior(t *testing.T) {
	configPath := createTestConfig(t, `
categories:
  default:
    probability: 0.0
`)

	t.Setenv("OTEL_TRACES_SAMPLER", testSamplerTypeCustom)
	t.Setenv("OTEL_TRACES_SAMPLER_CONFIG", configPath)

	sampler, err := maybeCreateCustomSampler()
	require.NoError(t, err)
	require.NotNil(t, sampler)

	// Create a sampled parent context
	ctx := t.Context()
	traceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	spanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	traceFlags := trace.FlagsSampled
	traceState, _ := trace.ParseTraceState("")

	spanContext := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: traceFlags,
		TraceState: traceState,
	})

	ctx = trace.ContextWithSpanContext(ctx, spanContext)

	// Even though root sampler has 0% probability, child should be sampled because parent is sampled
	params := sdktrace.SamplingParameters{
		ParentContext: ctx,
		Name:          "child_span",
	}

	result := sampler.ShouldSample(params)

	// ParentBased should respect parent's sampling decision
	require.Equal(t, sdktrace.RecordAndSample, result.Decision)
}

func TestGetCategoryForSpan_HTTPExactMatchPrecedence(t *testing.T) {
	// This test verifies that exact matches take precedence over pattern matches
	// Mirrors production config where "GET /ready" should match exactly before "GET *" pattern
	config := &SamplingConfig{
		Categories: map[string]CategoryConfig{
			"default":    {Probability: 1.0},
			"monitoring": {Probability: 0.1},
			"operations": {Probability: 1.0},
		},
		HTTP: HTTPSpanConfig{
			Exact: map[string]string{
				"GET /ready": "monitoring",
				"GET /live":  "monitoring",
			},
			Patterns: map[string]string{
				"GET /*": "operations", // Wildcard that would match /ready if checked first
			},
		},
	}

	sampler := &ConfigurableSampler{
		config:     config,
		defaultCat: testCategoryDefault,
	}

	tests := []struct {
		name     string
		method   string
		target   string
		expected string
	}{
		{
			name:     "/ready should match exact rule, not wildcard",
			method:   "GET",
			target:   "/ready",
			expected: testCategoryMonitoring,
		},
		{
			name:     "/live should match exact rule, not wildcard",
			method:   "GET",
			target:   "/live",
			expected: testCategoryMonitoring,
		},
		{
			name:     "/other should match wildcard pattern",
			method:   "GET",
			target:   "/other",
			expected: testCategoryOperations,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := sdktrace.SamplingParameters{
				Attributes: []attribute.KeyValue{
					attribute.String("http.method", tt.method),
					attribute.String("http.target", tt.target),
				},
			}

			category := sampler.getCategoryForSpan(params)
			require.Equal(t, tt.expected, category,
				"Expected %s for %s %s, got %s", tt.expected, tt.method, tt.target, category)
		})
	}
}

func TestGetCategoryForSpan_GRPCMethodOverride(t *testing.T) {
	// This test verifies that gRPC method overrides take precedence over service defaults
	// Mirrors production config where Status methods should be "monitoring" (10%)
	// while other methods in the same service should be "operations" (100%)
	config := &SamplingConfig{
		Categories: map[string]CategoryConfig{
			"default":    {Probability: 1.0},
			"monitoring": {Probability: 0.1},
			"operations": {Probability: 1.0},
		},
		GRPC: GRPCSpanConfig{
			Services: map[string]string{
				"/consensus.MultiPoolerConsensus":        "operations",
				"/pgctldservice.PgCtld":                  "operations",
				"/multipoolermanager.MultiPoolerManager": "operations",
			},
			Methods: map[string]string{
				"/consensus.MultiPoolerConsensus/Status":        "monitoring",
				"/pgctldservice.PgCtld/Status":                  "monitoring",
				"/multipoolermanager.MultiPoolerManager/Status": "monitoring",
			},
		},
	}

	sampler := &ConfigurableSampler{
		config:     config,
		defaultCat: testCategoryDefault,
	}

	tests := []struct {
		name     string
		service  string
		method   string
		expected string
	}{
		{
			name:     "MultiPoolerConsensus Status should use method override (monitoring)",
			service:  "consensus.MultiPoolerConsensus",
			method:   "Status",
			expected: testCategoryMonitoring,
		},
		{
			name:     "MultiPoolerConsensus other method should use service default (operations)",
			service:  "consensus.MultiPoolerConsensus",
			method:   "Promote",
			expected: testCategoryOperations,
		},
		{
			name:     "PgCtld Status should use method override (monitoring)",
			service:  "pgctldservice.PgCtld",
			method:   "Status",
			expected: testCategoryMonitoring,
		},
		{
			name:     "PgCtld other method should use service default (operations)",
			service:  "pgctldservice.PgCtld",
			method:   "Start",
			expected: testCategoryOperations,
		},
		{
			name:     "MultiPoolerManager Status should use method override (monitoring)",
			service:  "multipoolermanager.MultiPoolerManager",
			method:   "Status",
			expected: testCategoryMonitoring,
		},
		{
			name:     "MultiPoolerManager other method should use service default (operations)",
			service:  "multipoolermanager.MultiPoolerManager",
			method:   "CreatePool",
			expected: testCategoryOperations,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := sdktrace.SamplingParameters{
				Attributes: []attribute.KeyValue{
					attribute.String("rpc.system", "grpc"),
					attribute.String("rpc.service", tt.service),
					attribute.String("rpc.method", tt.method),
				},
			}

			category := sampler.getCategoryForSpan(params)
			require.Equal(t, tt.expected, category,
				"Expected %s for %s/%s, got %s", tt.expected, tt.service, tt.method, category)
		})
	}
}
