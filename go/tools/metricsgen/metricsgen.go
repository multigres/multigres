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

// Package metricsgen statically discovers every OpenTelemetry metric defined in
// the Multigres codebase and computes the Prometheus time-series names they are
// exported as.
//
// It works by loading the requested Go packages with full type information,
// finding every call to an OpenTelemetry metric.Meter instrument constructor
// (Int64Counter, Float64Histogram, Int64ObservableGauge, ...), and extracting
// the instrument's name and unit. The Prometheus name is then computed using
// github.com/prometheus/otlptranslator, which is the exact library the
// OpenTelemetry Prometheus exporter uses under the hood. This keeps the
// generated catalog in lock-step with what is actually scraped at runtime,
// rather than relying on a hand-maintained naming transform that can drift.
package metricsgen

import (
	"fmt"
	"go/ast"
	"go/constant"
	"go/types"
	"path"
	"sort"
	"strings"

	"github.com/prometheus/otlptranslator"
	"golang.org/x/tools/go/packages"
)

// otelMetricPkg is the import path of the OpenTelemetry metric API. All
// instrument constructors and option helpers (WithUnit) are declared here.
const otelMetricPkg = "go.opentelemetry.io/otel/metric"

// translationStrategy mirrors the default used by the OpenTelemetry Prometheus
// exporter (see go.opentelemetry.io/otel/exporters/prometheus newConfig). The
// Multigres telemetry setup does not override it, nor does it pass WithoutUnits
// or WithoutCounterSuffixes, so suffixes (unit, _total) are applied.
var translationStrategy = otlptranslator.UnderscoreEscapingWithSuffixes

// constructor describes one metric.Meter instrument constructor method.
type constructor struct {
	// metricType is how otlptranslator should treat the instrument when
	// deciding which suffixes to append.
	metricType otlptranslator.MetricType
	// histogram is true for histogram instruments, which Prometheus expands
	// into _bucket/_count/_sum series rather than a single series.
	histogram bool
}

// constructors enumerates every instrument constructor on the metric.Meter
// interface and maps it to the Prometheus metric type used for naming. The
// mapping matches collector.namingMetricType in the OpenTelemetry Prometheus
// exporter: monotonic counters get _total, up/down counters are treated as
// non-monotonic, gauges and histograms are named accordingly.
var constructors = map[string]constructor{
	"Int64Counter":                   {metricType: otlptranslator.MetricTypeMonotonicCounter},
	"Float64Counter":                 {metricType: otlptranslator.MetricTypeMonotonicCounter},
	"Int64ObservableCounter":         {metricType: otlptranslator.MetricTypeMonotonicCounter},
	"Float64ObservableCounter":       {metricType: otlptranslator.MetricTypeMonotonicCounter},
	"Int64UpDownCounter":             {metricType: otlptranslator.MetricTypeNonMonotonicCounter},
	"Float64UpDownCounter":           {metricType: otlptranslator.MetricTypeNonMonotonicCounter},
	"Int64ObservableUpDownCounter":   {metricType: otlptranslator.MetricTypeNonMonotonicCounter},
	"Float64ObservableUpDownCounter": {metricType: otlptranslator.MetricTypeNonMonotonicCounter},
	"Int64Gauge":                     {metricType: otlptranslator.MetricTypeGauge},
	"Float64Gauge":                   {metricType: otlptranslator.MetricTypeGauge},
	"Int64ObservableGauge":           {metricType: otlptranslator.MetricTypeGauge},
	"Float64ObservableGauge":         {metricType: otlptranslator.MetricTypeGauge},
	"Int64Histogram":                 {metricType: otlptranslator.MetricTypeHistogram, histogram: true},
	"Float64Histogram":               {metricType: otlptranslator.MetricTypeHistogram, histogram: true},
}

// Metric is a single discovered metric and everything needed to describe how it
// is exported to Prometheus.
type Metric struct {
	// OTelName is the dotted instrument name as written in the source, e.g.
	// "mg.gateway.query.duration".
	OTelName string
	// Constructor is the metric.Meter method used to create the instrument,
	// e.g. "Float64Histogram".
	Constructor string
	// Unit is the OpenTelemetry (UCUM) unit, or "" when WithUnit is not used.
	Unit string
	// Package is the Go import path where the instrument is defined.
	Package string
	// Binaries are the cmd/* binaries whose import graph reaches Package, i.e.
	// the binaries that can export this metric. Sorted. A metric defined in a
	// shared package (go/common/...) lists every binary that links it.
	//
	// This is static import reachability: a binary that links the package but
	// never constructs the instrument at runtime is still listed, so treat this
	// as "may expose" rather than a runtime guarantee.
	Binaries []string
	// Pos is "file:line" of the constructor call, for diagnostics.
	Pos string
	// PrometheusName is the base Prometheus metric/family name, including any
	// unit and _total suffixes applied by the exporter.
	PrometheusName string
	// Series are the concrete time-series names scraped from /metrics.
	// Histograms expand to _bucket/_count/_sum; everything else is a single
	// series equal to PrometheusName.
	Series []string
	// histogram records whether this metric is a histogram, used when building
	// the keep-list regex.
	histogram bool
}

// Collect loads the given package patterns (e.g. "./go/...") and returns every
// metric discovered, sorted by OTel name then package. It returns an error if
// the packages fail to load/type-check, or if a metric name or unit is not a
// compile-time constant string (the catalog can only be built statically).
func Collect(dir string, patterns ...string) ([]Metric, error) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax |
			packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports |
			packages.NeedDeps,
		Dir:   dir,
		Tests: false,
	}
	pkgs, err := packages.Load(cfg, patterns...)
	if err != nil {
		return nil, fmt.Errorf("loading packages: %w", err)
	}

	var loadErrs []string
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		for _, e := range p.Errors {
			loadErrs = append(loadErrs, e.Error())
		}
	})
	if len(loadErrs) > 0 {
		return nil, fmt.Errorf("packages failed to load:\n%s", strings.Join(loadErrs, "\n"))
	}

	namer := otlptranslator.NewMetricNamer("", translationStrategy)

	var (
		metrics  []Metric
		extracts []string // accumulated extraction errors
	)
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			ast.Inspect(file, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				m, err := extractMetric(pkg, namer, call)
				if err != nil {
					extracts = append(extracts, err.Error())
					return true
				}
				if m != nil {
					metrics = append(metrics, *m)
				}
				return true
			})
		}
	}
	if len(extracts) > 0 {
		return nil, fmt.Errorf("could not extract some metrics:\n%s", strings.Join(extracts, "\n"))
	}

	metrics = dedupe(metrics)
	assignBinaries(metrics, binaryImportSets(pkgs))
	sort.Slice(metrics, func(i, j int) bool {
		if metrics[i].OTelName != metrics[j].OTelName {
			return metrics[i].OTelName < metrics[j].OTelName
		}
		return metrics[i].Package < metrics[j].Package
	})
	return metrics, nil
}

// extractMetric returns a Metric if call is an OpenTelemetry instrument
// constructor, nil if it is an unrelated call, or an error if it is a
// constructor whose name/unit cannot be resolved to a constant string.
func extractMetric(pkg *packages.Package, namer otlptranslator.MetricNamer, call *ast.CallExpr) (*Metric, error) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return nil, nil
	}
	fn := resolveFunc(pkg.TypesInfo, sel)
	if fn == nil || fn.Pkg() == nil || fn.Pkg().Path() != otelMetricPkg {
		return nil, nil
	}
	ctor, ok := constructors[fn.Name()]
	if !ok {
		return nil, nil
	}
	if len(call.Args) == 0 {
		return nil, nil
	}

	p := pkg.Fset.Position(call.Pos())
	pos := fmt.Sprintf("%s:%d", p.Filename, p.Line)
	name, ok := constString(pkg.TypesInfo, call.Args[0])
	if !ok {
		return nil, fmt.Errorf("%s: %s metric name is not a constant string", pos, fn.Name())
	}

	unit, err := extractUnit(pkg, call.Args[1:], pos)
	if err != nil {
		return nil, err
	}

	base, err := namer.Build(otlptranslator.Metric{Name: name, Unit: unit, Type: ctor.metricType})
	if err != nil {
		return nil, fmt.Errorf("%s: building prometheus name for %q: %w", pos, name, err)
	}

	return &Metric{
		OTelName:       name,
		Constructor:    fn.Name(),
		Unit:           unit,
		Package:        pkg.PkgPath,
		Pos:            relPos(pos),
		PrometheusName: base,
		Series:         seriesFor(base, ctor.histogram),
		histogram:      ctor.histogram,
	}, nil
}

// extractUnit scans constructor option arguments for a metric.WithUnit(...) call
// and returns its constant string value, or "" if none is present.
func extractUnit(pkg *packages.Package, opts []ast.Expr, pos string) (string, error) {
	for _, opt := range opts {
		optCall, ok := opt.(*ast.CallExpr)
		if !ok {
			continue
		}
		optSel, ok := optCall.Fun.(*ast.SelectorExpr)
		if !ok {
			continue
		}
		fn := resolveFunc(pkg.TypesInfo, optSel)
		if fn == nil || fn.Pkg() == nil || fn.Pkg().Path() != otelMetricPkg || fn.Name() != "WithUnit" {
			continue
		}
		if len(optCall.Args) != 1 {
			continue
		}
		unit, ok := constString(pkg.TypesInfo, optCall.Args[0])
		if !ok {
			return "", fmt.Errorf("%s: WithUnit argument is not a constant string", pos)
		}
		return unit, nil
	}
	return "", nil
}

// resolveFunc returns the function/method object a selector refers to, handling
// both interface method calls (via Selections) and package-level functions or
// declared methods (via Uses).
func resolveFunc(info *types.Info, sel *ast.SelectorExpr) *types.Func {
	if seler, ok := info.Selections[sel]; ok {
		if fn, ok := seler.Obj().(*types.Func); ok {
			return fn
		}
		return nil
	}
	if obj, ok := info.Uses[sel.Sel]; ok {
		if fn, ok := obj.(*types.Func); ok {
			return fn
		}
	}
	return nil
}

// constString evaluates expr to a compile-time constant string. It returns the
// value and true for string literals as well as named string constants.
func constString(info *types.Info, expr ast.Expr) (string, bool) {
	tv, ok := info.Types[expr]
	if !ok || tv.Value == nil || tv.Value.Kind() != constant.String {
		return "", false
	}
	return constant.StringVal(tv.Value), true
}

// seriesFor expands a base Prometheus name into the concrete time-series names
// Prometheus exposes for it.
func seriesFor(base string, histogram bool) []string {
	if histogram {
		return []string{base + "_bucket", base + "_count", base + "_sum"}
	}
	return []string{base}
}

// dedupe removes duplicate metrics keyed by OTel name + package + constructor +
// unit. The same instrument can legitimately appear behind multiple build tags
// or be parsed twice; a metric defined identically in two packages is kept as
// two entries so the source of each is visible.
func dedupe(metrics []Metric) []Metric {
	seen := make(map[string]bool, len(metrics))
	out := metrics[:0]
	for _, m := range metrics {
		key := m.OTelName + "\x00" + m.Package + "\x00" + m.Constructor + "\x00" + m.Unit
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, m)
	}
	return out
}

// relPos trims everything before "/go/" so positions are repo-relative and
// stable across checkouts.
func relPos(pos string) string {
	if i := strings.Index(pos, "/go/"); i >= 0 {
		return pos[i+1:]
	}
	return pos
}

// KeepListRegex builds a single anchored alternation matching every Prometheus
// series produced by the given metrics, suitable as a Prometheus relabel `keep`
// regex. Histograms collapse to a "<base>_(bucket|count|sum)" group so the
// regex stays compact. Fragments are sorted and de-duplicated for stable output.
func KeepListRegex(metrics []Metric) string {
	set := make(map[string]bool)
	for _, m := range metrics {
		if m.histogram {
			set[m.PrometheusName+"_(bucket|count|sum)"] = true
		} else {
			set[m.PrometheusName] = true
		}
	}
	fragments := make([]string, 0, len(set))
	for f := range set {
		fragments = append(fragments, f)
	}
	sort.Strings(fragments)
	return strings.Join(fragments, "|")
}

// KeepListByBinary returns one keep-list regex per binary, covering only the
// series that binary can export. The union of all of them equals KeepListRegex.
// A binary with no metrics is omitted.
func KeepListByBinary(metrics []Metric) map[string]string {
	byBin := make(map[string][]Metric)
	for _, m := range metrics {
		for _, b := range m.Binaries {
			byBin[b] = append(byBin[b], m)
		}
	}
	out := make(map[string]string, len(byBin))
	for b, ms := range byBin {
		out[b] = KeepListRegex(ms)
	}
	return out
}

// binaryImportSets returns, for each cmd/* main package among pkgs, the set of
// package import paths reachable from it (including the main package itself).
func binaryImportSets(pkgs []*packages.Package) map[string]map[string]bool {
	sets := make(map[string]map[string]bool)
	for _, pkg := range pkgs {
		// A binary is a `package main` directly under a "cmd" directory, e.g.
		// .../go/cmd/multigateway. This excludes helper subpackages like
		// .../go/cmd/pgctld/command, whose parent dir is not "cmd".
		if pkg.Name != "main" || path.Base(path.Dir(pkg.PkgPath)) != "cmd" {
			continue
		}
		seen := map[string]bool{pkg.PkgPath: true}
		var visit func(p *packages.Package)
		visit = func(p *packages.Package) {
			for importPath, imp := range p.Imports {
				if seen[importPath] {
					continue
				}
				seen[importPath] = true
				visit(imp)
			}
		}
		visit(pkg)
		sets[path.Base(pkg.PkgPath)] = seen
	}
	return sets
}

// assignBinaries fills each metric's Binaries from the per-binary import sets:
// a binary exposes a metric when its import graph reaches the metric's package.
func assignBinaries(metrics []Metric, binSets map[string]map[string]bool) {
	for i := range metrics {
		var bins []string
		for name, set := range binSets {
			if set[metrics[i].Package] {
				bins = append(bins, name)
			}
		}
		sort.Strings(bins)
		metrics[i].Binaries = bins
	}
}
