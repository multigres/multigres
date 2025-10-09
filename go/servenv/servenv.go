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

	"github.com/multigres/multigres/go/event"
	"github.com/multigres/multigres/go/mterrors"
	"github.com/multigres/multigres/go/netutil"
	"github.com/multigres/multigres/go/viperutil"
	viperdebug "github.com/multigres/multigres/go/viperutil/debug"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// TimeoutFlags holds timeout configuration
type TimeoutFlags struct {
	LameduckPeriod time.Duration
	OnTermTimeout  time.Duration
	OnCloseTimeout time.Duration
}

// ServEnv holds the service environment configuration and state
type ServEnv struct {
	// Configuration
	HTTPPort        viperutil.Value[int]
	BindAddress     viperutil.Value[string]
	Hostname        viperutil.Value[string]
	Timeouts        *TimeoutFlags
	CatchSigpipe    bool
	MaxStackSize    int
	InitStartTime   time.Time
	TableRefreshInt int
	vc              *viperutil.ViperConfig

	// Hooks
	OnInitHooks     event.Hooks
	OnTermHooks     event.Hooks
	OnTermSyncHooks event.Hooks
	OnRunHooks      event.Hooks

	// State
	mu           sync.Mutex
	inited       bool
	ListeningURL url.URL

	mux          *http.ServeMux
	onCloseHooks event.Hooks
	// exitChan waits for a signal that tells the process to terminate
	exitChan chan os.Signal
	lg       *Logger
	pidFile  string // registered in RegisterFlags as --pid_file

	pprofFlag      []string
	httpPprof      bool
	serviceMapFlag []string

	// serviceMap is the used version of the service map.
	// init() functions can add default values to it (using InitServiceMap).
	// service_map command line parameter will alter the map.
	// Can only be used after servenv.Init has been called.
	serviceMap map[string]bool
}

// NewServEnv creates a new ServEnv instance with default configuration
func NewServEnv() *ServEnv {
	return NewServEnvWithConfig(NewLogger(), viperutil.NewViperConfig())
}

// NewServEnvWithConfig creates a new ServEnv instance with external logger and viper config.
// This allows sharing logger and viper config instances across multiple components
// to avoid duplicate flag registrations and binding conflicts.
func NewServEnvWithConfig(lg *Logger, vc *viperutil.ViperConfig) *ServEnv {
	return &ServEnv{
		HTTPPort: viperutil.Configure("http-port", viperutil.Options[int]{
			Default:  0,
			FlagName: "http-port",
			Dynamic:  false,
		}),
		Hostname: viperutil.Configure("hostname", viperutil.Options[string]{
			Default:  "",
			FlagName: "hostname",
			Dynamic:  false,
		}),
		BindAddress: viperutil.Configure("bind-address", viperutil.Options[string]{
			Default:  "",
			FlagName: "bind-address",
			Dynamic:  false,
		}),
		Timeouts: &TimeoutFlags{
			LameduckPeriod: 50 * time.Millisecond,
			OnTermTimeout:  10 * time.Second,
			OnCloseTimeout: 10 * time.Second,
		},
		vc:           vc,
		MaxStackSize: 64 * 1024 * 1024,
		mux:          http.NewServeMux(),
		lg:           lg,
		serviceMap:   make(map[string]bool),
	}
}

// GetInitStartTime returns the initialization start time
func (se *ServEnv) GetInitStartTime() time.Time {
	se.mu.Lock()
	defer se.mu.Unlock()
	return se.InitStartTime
}

// PopulateListeningURL sets the listening URL based on hostname and port
func (se *ServEnv) PopulateListeningURL(port int32) {
	host, err := netutil.FullyQualifiedHostname()
	if err != nil {
		slog.Warn("Failed to get fully qualified hostname, falling back to simple hostname",
			"error", err,
			"note", "This may indicate DNS configuration issues but service will continue normally")
		host, err = os.Hostname()
		if err != nil {
			slog.Error("os.Hostname() failed", "err", err)
			os.Exit(1)
		}
		slog.Info("Using simple hostname for service URL", "hostname", host)
	} else {
		slog.Info("Using fully qualified hostname for service URL", "hostname", host)
	}
	se.ListeningURL = url.URL{
		Scheme: "http",
		Host:   netutil.JoinHostPort(se.Hostname.Get(), port),
		Path:   "/",
	}
}

// OnInit registers f to be run at the beginning of the app lifecycle
func (se *ServEnv) OnInit(f func()) {
	se.OnInitHooks.Add(f)
}

// OnTerm registers a function to be run when the process receives a SIGTERM
func (se *ServEnv) OnTerm(f func()) {
	se.OnTermHooks.Add(f)
}

// OnTermSync registers a function to be run when the process receives SIGTERM
func (se *ServEnv) OnTermSync(f func()) {
	se.OnTermSyncHooks.Add(f)
}

// OnRun registers f to be run right at the beginning of Run
func (se *ServEnv) OnRun(f func()) {
	se.OnRunHooks.Add(f)
}

// OnClose registers f to be run at the end of the app lifecycle.
// This happens after the lameduck period just before the program exits.
// All hooks are run in parallel.
func (sv *ServEnv) OnClose(f func()) {
	sv.onCloseHooks.Add(f)
}

// FireRunHooks fires the hooks registered by OnRun
func (se *ServEnv) FireRunHooks() {
	se.OnRunHooks.Fire()
}

// fireOnTermSyncHooks returns true iff all the hooks finish before the timeout
func (se *ServEnv) fireOnTermSyncHooks(timeout time.Duration) bool {
	return se.fireHooksWithTimeout(timeout, "OnTermSync", se.OnTermSyncHooks.Fire)
}

// fireOnCloseHooks returns true iff all the hooks finish before the timeout
func (se *ServEnv) fireOnCloseHooks(timeout time.Duration) bool {
	return se.fireHooksWithTimeout(timeout, "OnClose", func() {
		se.onCloseHooks.Fire()
		se.ListeningURL = url.URL{}
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
func (se *ServEnv) RunDefault(grpcServer *GrpcServer) {
	se.Run(se.BindAddress.Get(), se.HTTPPort.Get(), grpcServer)
}

var (
	flagHooksM      sync.Mutex
	globalFlagHooks = []func(*pflag.FlagSet){
		mterrors.RegisterFlags,
	}
	commandFlagHooks = map[string][]func(*pflag.FlagSet){}
)

// OnParse registers a callback function to register flags on the flagset that are
// used by any caller of servenv.Parse or servenv.ParseWithArgs.
func OnParse(f func(fs *pflag.FlagSet)) {
	flagHooksM.Lock()
	defer flagHooksM.Unlock()

	globalFlagHooks = append(globalFlagHooks, f)
}

func getFlagHooksFor(cmd string) (hooks []func(fs *pflag.FlagSet)) {
	flagHooksM.Lock()
	defer flagHooksM.Unlock()

	hooks = append(hooks, globalFlagHooks...) // done deliberately to copy the slice

	if commandHooks, ok := commandFlagHooks[cmd]; ok {
		hooks = append(hooks, commandHooks...)
	}

	return hooks
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
	viperutil.NotifyConfigReload(ch)
	go func() {
		for range ch {
			slog.Info("Change in configuration", "settings", viperdebug.AllSettings())
		}
	}()

	watchCancel, err := sv.vc.LoadConfig()
	if err != nil {
		return fmt.Errorf("%s: failed to read in config: %s", cmd.Name(), err)
	}

	sv.OnTerm(watchCancel)
	// Register a function to be called on termination that closes the channel.
	// This is done after the watchCancel has registered to ensure that we don't end up
	// sending on a closed channel.
	sv.OnTerm(func() { close(ch) })
	return nil
}

// GetFlagSetFor returns the flag set for a given command.
// This has to exported for the Multigres-operator to use
func GetFlagSetFor(cmd string) *pflag.FlagSet {
	fs := pflag.NewFlagSet(cmd, pflag.ExitOnError)
	for _, hook := range getFlagHooksFor(cmd) {
		hook(fs)
	}

	return fs
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
	fs.Int("http-port", se.HTTPPort.Default(), "HTTP port for the server")
	fs.String("bind-address", se.BindAddress.Default(), "Bind address for the server. If empty, the server will listen on all available unicast and anycast IP addresses of the local system.")
	fs.String("hostname", se.Hostname.Default(), "Hostname to use for service registration. If not set, will auto-detect using FQDN or os.Hostname()")
	fs.BoolVar(&se.httpPprof, "pprof-http", se.httpPprof, "enable pprof http endpoints")
	fs.StringSliceVar(&se.pprofFlag, "pprof", se.pprofFlag, "enable profiling")
	fs.StringSliceVar(&se.serviceMapFlag, "service-map", se.serviceMapFlag, "comma separated list of services to enable (or disable if prefixed with '-') Example: grpc-queryservice")
	viperutil.BindFlags(fs, se.HTTPPort, se.BindAddress, se.Hostname)

	// Timeout flags
	fs.DurationVar(&se.Timeouts.LameduckPeriod, "lameduck-period", se.Timeouts.LameduckPeriod, "keep running at least this long after SIGTERM before stopping")
	fs.DurationVar(&se.Timeouts.OnTermTimeout, "onterm-timeout", se.Timeouts.OnTermTimeout, "wait no more than this for OnTermSync handlers before stopping")
	fs.DurationVar(&se.Timeouts.OnCloseTimeout, "onclose-timeout", se.Timeouts.OnCloseTimeout, "wait no more than this for OnClose handlers before stopping")
	fs.StringVar(&se.pidFile, "pid-file", "", "If set, the process will write its pid to the named file, and delete it on graceful shutdown.")

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
	sync.OnceFunc(func() {
		for _, hook := range getGlobalFlagHooks() {
			hook(fs)
		}
	})
}
