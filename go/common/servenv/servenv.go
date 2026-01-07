// Copyright 2023 The Vitess Authors.
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
//
// Modifications Copyright 2025 Supabase, Inc.

package servenv

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	viperdebug "github.com/multigres/multigres/go/common/servenv/viperdebug"
	"github.com/multigres/multigres/go/tools/event"
	"github.com/multigres/multigres/go/tools/netutil"
	"github.com/multigres/multigres/go/tools/stringutil"
	"github.com/multigres/multigres/go/tools/telemetry"
	"github.com/multigres/multigres/go/tools/viperutil"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// ServiceIdentity holds service identification for telemetry and topology registration.
type ServiceIdentity struct {
	// ServiceName is the logical name of the service (e.g., "multipooler", "multigateway").
	ServiceName string

	// ServiceInstanceID is the unique identifier for this service instance.
	ServiceInstanceID string

	// Cell is the availability zone/cell this service runs in.
	Cell string
}

// GenerateRandomServiceID generates a random 8-character service instance ID.
func GenerateRandomServiceID() string {
	return stringutil.RandomString(8)
}

// ServEnv holds the service environment configuration and state
type ServEnv struct {
	// Configuration registry
	reg *viperutil.Registry

	// Configuration
	httpPort       viperutil.Value[int]
	bindAddress    viperutil.Value[string]
	hostname       viperutil.Value[string]
	lameduckPeriod viperutil.Value[time.Duration]
	onTermTimeout  viperutil.Value[time.Duration]
	onCloseTimeout viperutil.Value[time.Duration]
	pidFile        viperutil.Value[string]
	httpPprof      viperutil.Value[bool]
	pprofFlag      viperutil.Value[[]string]
	serviceMapFlag viperutil.Value[[]string]
	catchSigpipe   bool
	maxStackSize   int
	initStartTime  time.Time
	vc             *viperutil.ViperConfig

	// Hooks
	onInitHooks     event.Hooks
	onTermHooks     event.Hooks
	onTermSyncHooks event.Hooks
	onRunHooks      event.Hooks
	onRunEHooks     event.ErrorHooks

	// State
	mu           sync.Mutex
	inited       bool
	listeningURL url.URL

	mux          *http.ServeMux
	onCloseHooks event.Hooks
	// exitChan waits for a signal that tells the process to terminate
	exitChan  chan os.Signal
	lg        *Logger
	telemetry *telemetry.Telemetry

	// serviceMap is the used version of the service map.
	// init() functions can add default values to it (using InitServiceMap).
	// service_map command line parameter will alter the map.
	// Can only be used after servenv.Init has been called.
	serviceMap map[string]bool
}

// NewServEnv creates a new ServEnv instance with the given registry
func NewServEnv(reg *viperutil.Registry) *ServEnv {
	telemetry := telemetry.NewTelemetry()
	return NewServEnvWithConfig(reg, NewLogger(reg, telemetry), viperutil.NewViperConfig(reg), telemetry)
}

// NewServEnvWithConfig creates a new ServEnv instance with external registry, logger and viper config.
// This allows sharing registry, logger and viper config instances across multiple components
// to avoid duplicate flag registrations and binding conflicts.
func NewServEnvWithConfig(reg *viperutil.Registry, lg *Logger, vc *viperutil.ViperConfig, telemetry *telemetry.Telemetry) *ServEnv {
	return &ServEnv{
		reg: reg,
		httpPort: viperutil.Configure(reg, "http-port", viperutil.Options[int]{
			Default:  0,
			FlagName: "http-port",
			Dynamic:  false,
		}),
		hostname: viperutil.Configure(reg, "hostname", viperutil.Options[string]{
			Default:  "",
			FlagName: "hostname",
			Dynamic:  false,
		}),
		bindAddress: viperutil.Configure(reg, "bind-address", viperutil.Options[string]{
			Default:  "",
			FlagName: "bind-address",
			Dynamic:  false,
		}),
		lameduckPeriod: viperutil.Configure(reg, "lameduck-period", viperutil.Options[time.Duration]{
			Default:  50 * time.Millisecond,
			FlagName: "lameduck-period",
			Dynamic:  false,
		}),
		onTermTimeout: viperutil.Configure(reg, "onterm-timeout", viperutil.Options[time.Duration]{
			Default:  10 * time.Second,
			FlagName: "onterm-timeout",
			Dynamic:  false,
		}),
		onCloseTimeout: viperutil.Configure(reg, "onclose-timeout", viperutil.Options[time.Duration]{
			Default:  10 * time.Second,
			FlagName: "onclose-timeout",
			Dynamic:  false,
		}),
		pidFile: viperutil.Configure(reg, "pid-file", viperutil.Options[string]{
			Default:  "",
			FlagName: "pid-file",
			Dynamic:  false,
		}),
		httpPprof: viperutil.Configure(reg, "pprof-http", viperutil.Options[bool]{
			Default:  false,
			FlagName: "pprof-http",
			Dynamic:  false,
		}),
		pprofFlag: viperutil.Configure(reg, "pprof", viperutil.Options[[]string]{
			Default:  []string{},
			FlagName: "pprof",
			Dynamic:  false,
		}),
		serviceMapFlag: viperutil.Configure(reg, "service-map", viperutil.Options[[]string]{
			Default:  []string{},
			FlagName: "service-map",
			Dynamic:  false,
		}),
		vc:           vc,
		maxStackSize: 64 * 1024 * 1024,
		mux:          http.NewServeMux(),
		lg:           lg,
		telemetry:    telemetry,
		serviceMap:   make(map[string]bool),
		exitChan:     make(chan os.Signal, 1),
	}
}

// GetInitStartTime returns the initialization start time
func (se *ServEnv) GetInitStartTime() time.Time {
	se.mu.Lock()
	defer se.mu.Unlock()
	return se.initStartTime
}

// SetListeningURL sets the listening URL
func (se *ServEnv) SetListeningURL(u url.URL) {
	se.listeningURL = u
}

// PopulateListeningURL sets the listening URL based on the configured hostname and port.
// The hostname should already be set by Init() before this is called.
func (se *ServEnv) PopulateListeningURL(port int32) {
	hostname := se.hostname.Get()
	slog.Info("Setting listening URL", "hostname", hostname, "port", port)
	se.SetListeningURL(url.URL{
		Scheme: "http",
		Host:   netutil.JoinHostPort(hostname, port),
		Path:   "/",
	})
}

// GetHTTPPort returns the HTTP port value
func (se *ServEnv) GetHTTPPort() int {
	return se.httpPort.Get()
}

// GetBindAddress returns the bind address value
func (se *ServEnv) GetBindAddress() string {
	return se.bindAddress.Get()
}

// GetHostname returns the hostname value
func (se *ServEnv) GetHostname() string {
	return se.hostname.Get()
}

// Hostname returns the hostname viperutil.Value for advanced usage
func (se *ServEnv) Hostname() viperutil.Value[string] {
	return se.hostname
}

// OnInit registers f to be run at the beginning of the app lifecycle
func (se *ServEnv) OnInit(f func()) {
	se.onInitHooks.Add(f)
}

// OnTerm registers a function to be run when the process receives a SIGTERM
func (se *ServEnv) OnTerm(f func()) {
	se.onTermHooks.Add(f)
}

// OnTermSync registers a function to be run when the process receives SIGTERM
func (se *ServEnv) OnTermSync(f func()) {
	se.onTermSyncHooks.Add(f)
}

// OnRun registers f to be run right at the beginning of Run
func (se *ServEnv) OnRun(f func()) {
	se.onRunHooks.Add(f)
}

// OnRunE registers an error-returning function to be run right at the beginning of Run.
// If the function returns an error, it will be collected and returned by FireRunHooks.
func (se *ServEnv) OnRunE(f func() error) {
	se.onRunEHooks.Add(f)
}

// OnClose registers f to be run at the end of the app lifecycle.
// This happens after the lameduck period just before the program exits.
// All hooks are run in parallel.
func (sv *ServEnv) OnClose(f func()) {
	sv.onCloseHooks.Add(f)
}

// FireRunHooks fires the hooks registered by OnRun and OnRunE.
// Returns an error if any OnRunE hooks fail (combined with errors.Join).
func (se *ServEnv) FireRunHooks() error {
	se.onRunHooks.Fire()
	return se.onRunEHooks.Fire()
}

// fireOnTermSyncHooks returns true iff all the hooks finish before the timeout
func (se *ServEnv) fireOnTermSyncHooks(timeout time.Duration) bool {
	return se.fireHooksWithTimeout(timeout, "OnTermSync", se.onTermSyncHooks.Fire)
}

// fireOnCloseHooks returns true iff all the hooks finish before the timeout
func (se *ServEnv) fireOnCloseHooks(timeout time.Duration) bool {
	return se.fireHooksWithTimeout(timeout, "OnClose", func() {
		se.onCloseHooks.Fire()
		se.SetListeningURL(url.URL{})
	})
}

// fireHooksWithTimeout returns true iff all the hooks finish before the timeout
func (se *ServEnv) fireHooksWithTimeout(timeout time.Duration, name string, hookFn func()) bool {
	slog.Info("Firing hooks and waiting for them", "name", name, "timeout", timeout)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	done := make(chan struct{})
	go func() {
		hookFn()
		close(done)
	}()

	select {
	case <-done:
		slog.Info(fmt.Sprintf("%s hooks finished", name))
		return true
	case <-timer.C:
		slog.Info(fmt.Sprintf("%s hooks timed out", name))
		return false
	}
}

// RunDefault calls Run() with the parameters from the flags
func (se *ServEnv) RunDefault(grpcServer *GrpcServer) error {
	return se.Run(se.bindAddress.Get(), se.httpPort.Get(), grpcServer)
}

var (
	flagHooksM      sync.Mutex
	globalFlagHooks = []func(*pflag.FlagSet){
		mterrors.RegisterFlags,
	}
)

// OnParse registers a callback function to register flags on the flagset that are
// used by any caller of servenv.Parse or servenv.ParseWithArgs.
func OnParse(f func(fs *pflag.FlagSet)) {
	flagHooksM.Lock()
	defer flagHooksM.Unlock()

	globalFlagHooks = append(globalFlagHooks, f)
}

func getGlobalFlagHooks() (hooks []func(fs *pflag.FlagSet)) {
	flagHooksM.Lock()
	defer flagHooksM.Unlock()
	hooks = append(hooks, globalFlagHooks...) // done deliberately to copy the slice
	return hooks
}

// CobraPreRunE returns the common function that commands will need to load
// viper infrastructure. It matches the signature of cobra's (Pre|Post)RunE-type
// functions.
func (sv *ServEnv) CobraPreRunE(cmd *cobra.Command) error {
	// Register logging on config file change.
	ch := make(chan struct{})
	viperutil.NotifyConfigReload(sv.reg, ch)
	go func() {
		for range ch {
			slog.Info("Change in configuration", "settings", viperdebug.AllSettings(sv.reg))
		}
	}()

	watchCancel, err := sv.vc.LoadConfig(sv.reg)
	if err != nil {
		return fmt.Errorf("%s: failed to read in config: %w", cmd.Name(), err)
	}

	sv.OnTerm(watchCancel)
	// Register a function to be called on termination that closes the channel.
	// This is done after the watchCancel has registered to ensure that we don't end up
	// sending on a closed channel.
	sv.OnTerm(func() { close(ch) })
	return nil
}

// TestingEndtoend is true when this Multigres binary is being run as part of an endtoend test suite
var TestingEndtoend = false

func init() {
	TestingEndtoend = os.Getenv("MTTEST") == "endtoend"
}

func (se *ServEnv) RegisterFlags(fs *pflag.FlagSet) {
	se.registerFlags(fs, true)
}

// RegisterFlagsWithoutLoggerAndConfig registers servenv flags but skips logger and viper config flags.
// Use this when the logger and viper config are managed externally (e.g., as persistent flags in a root command).
func (se *ServEnv) RegisterFlagsWithoutLoggerAndConfig(fs *pflag.FlagSet) {
	se.registerFlags(fs, false)
}

func (se *ServEnv) registerFlags(fs *pflag.FlagSet, includeLoggerAndConfig bool) {
	// Default flags
	fs.Int("http-port", se.httpPort.Default(), "HTTP port for the server")
	fs.String("bind-address", se.bindAddress.Default(), "Bind address for the server. If empty, the server will listen on all available unicast and anycast IP addresses of the local system.")
	fs.String("hostname", se.hostname.Default(), "Hostname to use for service registration. If not set, will auto-detect using FQDN or os.Hostname()")
	fs.Bool("pprof-http", se.httpPprof.Default(), "enable pprof http endpoints")
	fs.StringSlice("pprof", se.pprofFlag.Default(), "enable profiling")
	fs.StringSlice("service-map", se.serviceMapFlag.Default(), "comma separated list of services to enable (or disable if prefixed with '-') Example: grpc-queryservice")

	// Timeout flags
	fs.Duration("lameduck-period", se.lameduckPeriod.Default(), "keep running at least this long after SIGTERM before stopping")
	fs.Duration("onterm-timeout", se.onTermTimeout.Default(), "wait no more than this for OnTermSync handlers before stopping")
	fs.Duration("onclose-timeout", se.onCloseTimeout.Default(), "wait no more than this for OnClose handlers before stopping")
	fs.String("pid-file", se.pidFile.Default(), "If set, the process will write its pid to the named file, and delete it on graceful shutdown.")

	viperutil.BindFlags(fs, se.httpPort, se.bindAddress, se.hostname, se.lameduckPeriod, se.onTermTimeout, se.onCloseTimeout, se.pidFile, se.httpPprof, se.pprofFlag, se.serviceMapFlag)

	// Server auth flags
	for _, fn := range grpcAuthServerFlagHooks {
		fn(fs)
	}

	// Only register logger and viper config flags if requested
	// Skip if these are managed externally (e.g., as persistent flags in root command)
	if includeLoggerAndConfig {
		se.lg.RegisterFlags(fs)
		se.vc.RegisterFlags(fs)
	}

	// Global and command flag hooks
	for _, hook := range getGlobalFlagHooks() {
		hook(fs)
	}
}

// IsTestOrphanDetectionEnabled returns true if test orphan detection environment
// variables are set. This is used to determine if subprocesses should enable
// orphan detection monitoring.
func IsTestOrphanDetectionEnabled() bool {
	testDataDir := os.Getenv("MULTIGRES_TESTDATA_DIR")
	testParentPID := os.Getenv("MULTIGRES_TEST_PARENT_PID")
	return testDataDir != "" || testParentPID != ""
}
