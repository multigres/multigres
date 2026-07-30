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

package servenv

import (
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"

	"github.com/multigres/multigres/go/tools/telemetry"
	"github.com/multigres/multigres/go/tools/viperutil"

	"github.com/spf13/pflag"
)

// Log output target constants
const (
	logOutputStdout = "stdout"
	logOutputStderr = "stderr"
)

// parseLevel maps a configured level string to a slog.Level, defaulting to
// info for empty or unrecognized values.
func parseLevel(levelStr string) slog.Level {
	switch strings.ToLower(levelStr) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// resolveOutput maps a configured output string to an io.Writer. "stdout" and
// "stderr" are special-cased; anything else is treated as a file path, falling
// back to stdout if the file cannot be opened.
func resolveOutput(outputStr string) io.Writer {
	switch strings.ToLower(outputStr) {
	case logOutputStdout:
		return os.Stdout
	case logOutputStderr:
		return os.Stderr
	default:
		file, err := os.OpenFile(outputStr, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return os.Stdout
		}
		return file
	}
}

// buildHandler constructs the base slog handler for the given format, output,
// and level. All logging paths funnel through here so JSON and text output share
// identical options. Unknown formats fall back to JSON.
//
// Attribute-key consistency (e.g. "error" over "err") is enforced statically by
// sloglint in CI rather than rewritten at runtime, so the handler needs no
// ReplaceAttr.
func buildHandler(output io.Writer, format string, level slog.Level) slog.Handler {
	opts := &slog.HandlerOptions{Level: level}
	if strings.EqualFold(format, "text") {
		return slog.NewTextHandler(output, opts)
	}
	return slog.NewJSONHandler(output, opts)
}

type Logger struct {
	// Logging configuration flags
	logLevel  viperutil.Value[string]
	logFormat viperutil.Value[string]
	logOutput viperutil.Value[string]

	// Internal state
	loggerOnce  sync.Once
	logger      *slog.Logger
	loggerMu    sync.Mutex
	telemetry   *telemetry.Telemetry
	baseHandler slog.Handler // Handler before telemetry wrapping

	// Hooks for customizing logging behavior
	loggingSetupHooks  []func(*slog.Logger)
	loggingChangeHooks []func(*slog.Logger)
	loggingHooksMu     sync.Mutex
}

func NewLogger(reg *viperutil.Registry, telemetry *telemetry.Telemetry) *Logger {
	return &Logger{
		telemetry: telemetry,
		logLevel: viperutil.Configure(reg, "log-level", viperutil.Options[string]{
			Default:  "info",
			FlagName: "log-level",
			Dynamic:  false,
		}),
		logFormat: viperutil.Configure(reg, "log-format", viperutil.Options[string]{
			Default:  "json",
			FlagName: "log-format",
			Dynamic:  false,
		}),
		logOutput: viperutil.Configure(reg, "log-output", viperutil.Options[string]{
			Default:  "stdout",
			FlagName: "log-output",
			Dynamic:  false,
		}),
	}
}

// RegisterFlags registers logging-related command line flags.
// This must be called before ParseFlags if using the logging system.
func (lg *Logger) RegisterFlags(fs *pflag.FlagSet) {
	fs.String("log-level", lg.logLevel.Default(), "Log level (debug, info, warn, error)")
	fs.String("log-format", lg.logFormat.Default(), "Log format (json, text)")
	fs.String("log-output", lg.logOutput.Default(), "Log output (stdout, stderr, or file path)")
	viperutil.BindFlags(fs, lg.logLevel, lg.logFormat, lg.logOutput)
}

// GetLogger returns the process-wide default logger.
//
// The struct-based (*Logger).SetupLogging installs the configured logger as the
// slog default, so callers that don't hold a *Logger (or run before setup) can
// still obtain the right logger here. Before setup it returns slog's built-in
// default.
func GetLogger() *slog.Logger {
	return slog.Default()
}

// OnLoggingSetup registers a callback function to be called after the logger is created.
// This allows applications to customize the logger behavior.
func (lg *Logger) OnLoggingSetup(f func(*slog.Logger)) {
	lg.loggingHooksMu.Lock()
	defer lg.loggingHooksMu.Unlock()
	lg.loggingSetupHooks = append(lg.loggingSetupHooks, f)
}

// OnLoggingChange registers a callback function to be called when logging configuration changes.
func (lg *Logger) OnLoggingChange(f func(*slog.Logger)) {
	lg.loggingHooksMu.Lock()
	defer lg.loggingHooksMu.Unlock()
	lg.loggingChangeHooks = append(lg.loggingChangeHooks, f)
}

// SetupLogging initializes the logger based on the configured flags.
// This should be called after flags are parsed but before any logging occurs.
func (lg *Logger) SetupLogging() {
	lg.loggerOnce.Do(func() {
		levelStr := lg.logLevel.Get()
		if levelStr == "" {
			levelStr = "info" // Default fallback
		}
		level := parseLevel(levelStr)

		outputStr := lg.logOutput.Get()
		if outputStr == "" {
			outputStr = logOutputStdout // Default fallback
		}
		output := resolveOutput(outputStr)

		formatStr := lg.logFormat.Get()
		if formatStr == "" {
			formatStr = "json" // Default fallback
		}
		handler := buildHandler(output, formatStr, level)

		// Store base handler before wrapping (for later re-wrapping after telemetry init)
		lg.loggerMu.Lock()
		lg.baseHandler = handler
		lg.loggerMu.Unlock()

		// Wrap handler with OpenTelemetry bridge to inject trace context
		if lg.telemetry != nil {
			handler = lg.telemetry.WrapSlogHandler(handler)
		}

		// Create logger
		newLogger := slog.New(handler)

		// Set as default slog logger
		slog.SetDefault(newLogger)

		// Store logger
		lg.loggerMu.Lock()
		lg.logger = newLogger
		lg.loggerMu.Unlock()

		// Fire setup hooks
		lg.fireLoggingSetupHooks(newLogger)

		// Log initial configuration
		newLogger.Info("logging initialized",
			"level", levelStr,
			"format", formatStr,
			"output", outputStr,
		)
	})
}

// UpdateTelemetryWrapper re-wraps the logger with telemetry after telemetry initialization.
// Call this after InitTelemetry() to enable OTLP logs export.
func (lg *Logger) UpdateTelemetryWrapper() {
	lg.loggerMu.Lock()
	defer lg.loggerMu.Unlock()

	if lg.baseHandler == nil || lg.telemetry == nil {
		return
	}

	handler := lg.telemetry.WrapSlogHandler(lg.baseHandler)
	lg.logger = slog.New(handler)
	slog.SetDefault(lg.logger)
}

// GetLogger returns the configured logger instance.
// SetupLogging must be called before this function.
func (lg *Logger) GetLogger() *slog.Logger {
	lg.loggerMu.Lock()
	defer lg.loggerMu.Unlock()
	if lg.logger == nil {
		// Return default slog logger if our logger hasn't been set up yet
		return slog.Default()
	}
	return lg.logger
}

// GetLogger returns the configured logger instance.
func (sv *ServEnv) GetLogger() *slog.Logger {
	return sv.lg.GetLogger()
}

// fireLoggingSetupHooks calls all registered logging setup hooks.
func (lg *Logger) fireLoggingSetupHooks(l *slog.Logger) {
	lg.loggingHooksMu.Lock()
	hooks := make([]func(*slog.Logger), len(lg.loggingSetupHooks))
	copy(hooks, lg.loggingSetupHooks)
	lg.loggingHooksMu.Unlock()

	for _, hook := range hooks {
		hook(l)
	}
}

// GetLogLevel returns the current log level setting.
func (lg *Logger) GetLogLevel() string {
	return lg.logLevel.Get()
}

// GetLogFormat returns the current log format setting.
func (lg *Logger) GetLogFormat() string {
	return lg.logFormat.Get()
}

// GetLogOutput returns the current log output setting.
func (lg *Logger) GetLogOutput() string {
	return lg.logOutput.Get()
}
