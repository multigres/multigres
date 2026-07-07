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

package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/process"
	runtimemetrics "go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// processMeterName is the instrumentation scope for the process- and
// runtime-level resource metrics emitted by every Multigres component.
const processMeterName = "github.com/multigres/multigres/go/tools/telemetry"

// cpuModeKey labels process.cpu.time by the CPU mode (user vs. system), matching
// the OpenTelemetry process metric semantic conventions. Prometheus scrapers
// derive CPU utilization via rate() over the resulting counter.
const cpuModeKey = attribute.Key("cpu.mode")

// processStatReader reads CPU and memory statistics for a single process. It is
// satisfied by *gopsutil/process.Process in production and faked in tests.
type processStatReader interface {
	TimesWithContext(context.Context) (*cpu.TimesStat, error)
	MemoryInfoWithContext(context.Context) (*process.MemoryInfoStat, error)
}

// startRuntimeMetrics and newProcessReader are indirection seams: production
// wraps the real dependencies, while tests override them to exercise the failure
// paths (which are otherwise unreachable with healthy dependencies).
var (
	startRuntimeMetrics = func(mp metric.MeterProvider) error {
		return runtimemetrics.Start(runtimemetrics.WithMeterProvider(mp))
	}
	newProcessReader = func() (processStatReader, error) {
		return process.NewProcess(int32(os.Getpid()))
	}
)

// initProcessMetrics registers process- and runtime-level resource metrics
// (CPU time, resident/virtual memory, and Go runtime internals) on the shared
// MeterProvider. Because every Multigres component initializes telemetry through
// InitTelemetry, wiring these here means all of them export the metrics with no
// per-component code.
//
// The CPU/memory gauges are what replace `kubectl top`: process.cpu.time and
// process.memory.usage (RSS) match what the kubelet reports. The Go runtime
// collector (heap, GC, goroutines) is a debugging complement, not a substitute.
func (t *Telemetry) initProcessMetrics() error {
	// Go runtime metrics (heap alloc, GC, goroutine count, ...).
	if err := startRuntimeMetrics(t.meterProvider); err != nil {
		return fmt.Errorf("failed to start Go runtime metrics: %w", err)
	}

	// Process-level CPU/memory, read from this process via gopsutil.
	proc, err := newProcessReader()
	if err != nil {
		return fmt.Errorf("failed to open current process for metrics: %w", err)
	}

	meter := t.meterProvider.Meter(processMeterName)

	var errs []error

	cpuTime, err := meter.Float64ObservableCounter(
		"process.cpu.time",
		metric.WithDescription("Total CPU seconds consumed by the process, broken down by mode."),
		metric.WithUnit("s"),
	)
	errs = append(errs, err)

	memUsage, err := meter.Int64ObservableGauge(
		"process.memory.usage",
		metric.WithDescription("Resident set size (RSS) of the process."),
		metric.WithUnit("By"),
	)
	errs = append(errs, err)

	memVirtual, err := meter.Int64ObservableGauge(
		"process.memory.virtual",
		metric.WithDescription("Virtual memory size (VMS) of the process."),
		metric.WithUnit("By"),
	)
	errs = append(errs, err)

	if joined := errors.Join(errs...); joined != nil {
		return fmt.Errorf("failed to create process metrics: %w", joined)
	}

	_, err = meter.RegisterCallback(
		func(ctx context.Context, o metric.Observer) error {
			observeProcessStats(ctx, o, proc, cpuTime, memUsage, memVirtual)
			return nil
		},
		cpuTime, memUsage, memVirtual,
	)
	if err != nil {
		return fmt.Errorf("failed to register process metrics callback: %w", err)
	}

	return nil
}

// observeProcessStats reads the process CPU/memory stats and records them on the
// observer. It observes whatever succeeds: a transient read failure on one
// metric is logged and skipped so the others still report, and never fails the
// collection (errors are logged, not returned, to avoid the OTel SDK re-logging
// on every scrape).
func observeProcessStats(
	ctx context.Context,
	o metric.Observer,
	proc processStatReader,
	cpuTime metric.Float64ObservableCounter,
	memUsage, memVirtual metric.Int64ObservableGauge,
) {
	if times, err := proc.TimesWithContext(ctx); err != nil {
		slog.DebugContext(ctx, "process cpu metric read failed", "error", err)
	} else {
		o.ObserveFloat64(cpuTime, times.User, metric.WithAttributes(cpuModeKey.String("user")))
		o.ObserveFloat64(cpuTime, times.System, metric.WithAttributes(cpuModeKey.String("system")))
	}

	if mem, err := proc.MemoryInfoWithContext(ctx); err != nil {
		slog.DebugContext(ctx, "process memory metric read failed", "error", err)
	} else {
		o.ObserveInt64(memUsage, int64(mem.RSS))
		o.ObserveInt64(memVirtual, int64(mem.VMS))
	}
}
