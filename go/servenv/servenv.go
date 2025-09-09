/*
Copyright 2023 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Modifications Copyright 2025 The Multigres Authors.
*/

package servenv

import (
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
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

var (
	// httpPort is part of the flags used when calling RegisterDefaultFlags.
	httpPort    int
	bindAddress string

	// mutex used to protect the Init function
	mu sync.Mutex

	onInitHooks     event.Hooks
	onTermHooks     event.Hooks
	onTermSyncHooks event.Hooks
	onRunHooks      event.Hooks
	inited          bool

	// ListeningURL is filled in when calling Run, contains the server URL.
	ListeningURL url.URL
)

// Flags specific to Init, Run, and RunDefault functions.
var (
	catchSigpipe         bool
	maxStackSize         = 64 * 1024 * 1024
	initStartTime        time.Time // time when tablet init started: for debug purposes to time how long a tablet init takes
	tableRefreshInterval int
)

type TimeoutFlags struct {
	LameduckPeriod time.Duration
	OnTermTimeout  time.Duration
	OnCloseTimeout time.Duration
}

var timeouts = &TimeoutFlags{
	LameduckPeriod: 50 * time.Millisecond,
	OnTermTimeout:  10 * time.Second,
	OnCloseTimeout: 10 * time.Second,
}

// RegisterFlags installs the flags used by Init, Run, and RunDefault.
//
// This must be called before servenv.ParseFlags if using any of those
// functions.
func RegisterFlags() {
	OnParse(func(fs *pflag.FlagSet) {
		fs.DurationVar(&timeouts.LameduckPeriod, "lameduck-period", timeouts.LameduckPeriod, "keep running at least this long after SIGTERM before stopping")
		fs.DurationVar(&timeouts.OnTermTimeout, "onterm-timeout", timeouts.OnTermTimeout, "wait no more than this for OnTermSync handlers before stopping")
		fs.DurationVar(&timeouts.OnCloseTimeout, "onclose-timeout", timeouts.OnCloseTimeout, "wait no more than this for OnClose handlers before stopping")
		fs.StringVar(&pidFile, "pid-file", pidFile, "If set, the process will write its pid to the named file, and delete it on graceful shutdown.")
	})
}

func RegisterFlagsWithTimeouts(tf *TimeoutFlags) {
	OnParse(func(fs *pflag.FlagSet) {
		fs.DurationVar(&tf.LameduckPeriod, "lameduck-period", tf.LameduckPeriod, "keep running at least this long after SIGTERM before stopping")
		fs.DurationVar(&tf.OnTermTimeout, "onterm-timeout", tf.OnTermTimeout, "wait no more than this for OnTermSync handlers before stopping")
		fs.DurationVar(&tf.OnCloseTimeout, "onclose-timeout", tf.OnCloseTimeout, "wait no more than this for OnClose handlers before stopping")
		// pid_file.go
		fs.StringVar(&pidFile, "pid-file", pidFile, "If set, the process will write its pid to the named file, and delete it on graceful shutdown.")
		timeouts = tf
	})
}

func GetInitStartTime() time.Time {
	mu.Lock()
	defer mu.Unlock()
	return initStartTime
}

func populateListeningURL(port int32) {
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
	ListeningURL = url.URL{
		Scheme: "http",
		Host:   netutil.JoinHostPort(host, port),
		Path:   "/",
	}
}

// OnInit registers f to be run at the beginning of the app
// lifecycle. It should be called in an init() function.
func OnInit(f func()) {
	onInitHooks.Add(f)
}

// OnTerm registers a function to be run when the process receives a SIGTERM.
// This allows the program to change its behavior during the lameduck period.
//
// All hooks are run in parallel, and there is no guarantee that the process
// will wait for them to finish before dying when the lameduck period expires.
//
// See also: OnTermSync
func OnTerm(f func()) {
	onTermHooks.Add(f)
}

// OnTermSync registers a function to be run when the process receives SIGTERM.
// This allows the program to change its behavior during the lameduck period.
//
// All hooks are run in parallel, and the process will do its best to wait
// (up to -onterm_timeout) for all of them to finish before dying.
//
// See also: OnTerm
func OnTermSync(f func()) {
	onTermSyncHooks.Add(f)
}

// fireOnTermSyncHooks returns true iff all the hooks finish before the timeout.
func fireOnTermSyncHooks(timeout time.Duration) bool {
	return fireHooksWithTimeout(timeout, "OnTermSync", onTermSyncHooks.Fire)
}

// fireOnCloseHooks returns true iff all the hooks finish before the timeout.
func fireOnCloseHooks(timeout time.Duration) bool {
	return fireHooksWithTimeout(timeout, "OnClose", func() {
		onCloseHooks.Fire()
		ListeningURL = url.URL{}
	})
}

// fireHooksWithTimeout returns true iff all the hooks finish before the timeout.
func fireHooksWithTimeout(timeout time.Duration, name string, hookFn func()) bool {
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

// OnRun registers f to be run right at the beginning of Run. All
// hooks are run in parallel.
func OnRun(f func()) {
	onRunHooks.Add(f)
}

// FireRunHooks fires the hooks registered by OnHook.
// Use this in a non-server to run the hooks registered
// by servenv.OnRun().
func FireRunHooks() {
	onRunHooks.Fire()
}

// RegisterDefaultFlags registers the default flags for
// listening to a given port for standard connections.
// If calling this, then call RunDefault()
func RegisterDefaultFlags() {
	OnParse(func(fs *pflag.FlagSet) {
		fs.IntVar(&httpPort, "http-port", httpPort, "HTTP port for the server")
		fs.StringVar(&bindAddress, "bind-address", bindAddress, "Bind address for the server. If empty, the server will listen on all available unicast and anycast IP addresses of the local system.")
	})
}

// HTTPPort returns the value of the `--http-port` flag.
func HTTPPort() int {
	return httpPort
}

// Port returns the value of the `--http-port` flag.
// Deprecated: Use HTTPPort() instead.
func Port() int {
	return httpPort
}

// RunDefault calls Run() with the parameters from the flags.
func RunDefault() {
	Run(bindAddress, httpPort)
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

// OnParseFor registers a callback function to register flags on the flagset
// used by servenv.Parse or servenv.ParseWithArgs. The provided callback will
// only be called if the `cmd` argument passed to either Parse or ParseWithArgs
// exactly matches the `cmd` argument passed to OnParseFor.
//
// To register for flags for multiple commands, for example if a package's flags
// should be used for only vtgate and vttablet but no other binaries, call this
// multiple times with the same callback function. To register flags for all
// commands globally, use OnParse instead.
func OnParseFor(cmd string, f func(fs *pflag.FlagSet)) {
	flagHooksM.Lock()
	defer flagHooksM.Unlock()

	commandFlagHooks[cmd] = append(commandFlagHooks[cmd], f)
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

// Needed because some tests require multiple parse passes, so we guard against
// that here.
var debugConfigRegisterOnce sync.Once

// ParseFlags initializes flags and handles the common case when no positional
// arguments are expected.
func ParseFlags(cmd string) {
	fs := GetFlagSetFor(cmd)

	viperutil.BindFlags(fs)
	args := fs.Args()
	if len(args) > 0 {
		slog.Info(fmt.Sprintf("%s doesn't take any positional arguments, got '%s'", cmd, strings.Join(args, " ")))
		os.Exit(1)
	}

	loadViper(cmd)

}

// ParseFlagsForTests initializes flags but skips the version, filesystem
// args and go flag related work.
// Note: this should not be used outside of unit tests.
func ParseFlagsForTests(cmd string) {
	fs := GetFlagSetFor(cmd)
	pflag.CommandLine = fs
	pflag.Parse()
	viperutil.BindFlags(fs)
	loadViper(cmd)
}

// CobraPreRunE returns the common function that commands will need to load
// viper infrastructure. It matches the signature of cobra's (Pre|Post)RunE-type
// functions.
func CobraPreRunE(cmd *cobra.Command, args []string) error {
	// Register logging on config file change.
	ch := make(chan struct{})
	viperutil.NotifyConfigReload(ch)
	go func() {
		for range ch {
			slog.Info("Change in configuration", "settings", viperdebug.AllSettings())
		}
	}()

	watchCancel, err := viperutil.LoadConfig()
	if err != nil {
		return fmt.Errorf("%s: failed to read in config: %s", cmd.Name(), err)
	}

	OnTerm(watchCancel)
	// Register a function to be called on termination that closes the channel.
	// This is done after the watchCancel has registered to ensure that we don't end up
	// sending on a closed channel.
	OnTerm(func() { close(ch) })

	// Setup logging after config is loaded and flags are parsed
	SetupLogging()

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

// ParseFlagsWithArgs initializes flags and returns the positional arguments
func ParseFlagsWithArgs(cmd string) []string {
	fs := GetFlagSetFor(cmd)

	viperutil.BindFlags(fs)

	args := fs.Args()
	if len(args) == 0 {
		slog.Info(fmt.Sprintf("%s expected at least one positional argument", cmd))
		os.Exit(1)
	}

	loadViper(cmd)

	return args
}

func loadViper(cmd string) {
	watchCancel, err := viperutil.LoadConfig()
	if err != nil {
		slog.Error("failed to read in config", "cmd", cmd, "err", err)
		os.Exit(1)
	}
	OnTerm(watchCancel)
}

// Flag installations for packages that servenv imports. We need to register
// here rather than in those packages (which is what we would normally do)
// because that would create a dependency cycle.
func init() {
	// TODO: @rafael
	// Leaving this here as a pointer as we continue the port.
	// In this block we will call OnParseFor to register
	// common flags for things like tracing / stats / grpc common flags
	// Register logging flags by default
	RegisterLoggingFlags()

	OnParse(viperutil.RegisterFlags)
}

// TestingEndtoend is true when this Multigres binary is being run as part of an endtoend test suite
var TestingEndtoend = false

func init() {
	TestingEndtoend = os.Getenv("MTTEST") == "endtoend"
}

// AddFlagSetToCobraCommand moves the servenv-registered flags to the flagset of
// the given cobra command.
func AddFlagSetToCobraCommand(cmd *cobra.Command) {
	fs := cmd.PersistentFlags()
	fs.AddFlagSet(GetFlagSetFor(cmd.Name()))
	pflag.CommandLine = fs
}

// RegisterCommonServiceFlags registers the common flags that most services need.
// This includes default flags, timeout flags, gRPC server flags, gRPC auth flags,
// and service map flags. This method is mainly used internally by RegisterServiceCmd.
// For most use cases, prefer using RegisterServiceCmd instead.
func RegisterCommonServiceFlags() {
	RegisterDefaultFlags()
	RegisterFlags()
	RegisterGRPCServerFlags()
	RegisterGRPCServerAuthFlags()
	RegisterServiceMapFlag()
}

// RegisterServiceCmd registers the common flags that most services need and
// sets up the command with flags parsed and hooked. This is a convenience
// method that combines flag registration and command setup.
func RegisterServiceCmd(cmd *cobra.Command) {
	RegisterCommonServiceFlags()
	// Get the flag set from servenv and add it to the cobra command
	AddFlagSetToCobraCommand(cmd)
}
