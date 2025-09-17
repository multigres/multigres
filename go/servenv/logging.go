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

	"github.com/spf13/pflag"
)

var (
	// Logging configuration flags
	logLevel  string
	logFormat string
	logOutput string

	// Internal state
	loggerOnce sync.Once
	logger     *slog.Logger
	loggerMu   sync.RWMutex

	// Hooks for customizing logging behavior
	loggingSetupHooks  []func(*slog.Logger)
	loggingChangeHooks []func(*slog.Logger)
	loggingHooksMu     sync.RWMutex
)

// RegisterLoggingFlags registers logging-related command line flags.
// This must be called before ParseFlags if using the logging system.
func RegisterLoggingFlags() {
	OnParse(func(fs *pflag.FlagSet) {
		fs.StringVar(&logLevel, "log-level", "info", "Log level (debug, info, warn, error)")
		fs.StringVar(&logFormat, "log-format", "json", "Log format (json, text)")
		fs.StringVar(&logOutput, "log-output", "stdout", "Log output (stdout, stderr, or file path)")
	})
}

// OnLoggingSetup registers a callback function to be called after the logger is created.
// This allows applications to customize the logger behavior.
func OnLoggingSetup(f func(*slog.Logger)) {
	loggingHooksMu.Lock()
	defer loggingHooksMu.Unlock()
	loggingSetupHooks = append(loggingSetupHooks, f)
}

// OnLoggingChange registers a callback function to be called when logging configuration changes.
func OnLoggingChange(f func(*slog.Logger)) {
	loggingHooksMu.Lock()
	defer loggingHooksMu.Unlock()
	loggingChangeHooks = append(loggingChangeHooks, f)
}

// SetupLogging initializes the logger based on the configured flags.
// This should be called after flags are parsed but before any logging occurs.
func SetupLogging() {
	loggerOnce.Do(func() {
		// Parse log level with fallback to default
		var level slog.Level
		levelStr := logLevel
		if levelStr == "" {
			levelStr = "info" // Default fallback
		}
		switch strings.ToLower(levelStr) {
		case "debug":
			level = slog.LevelDebug
		case "info":
			level = slog.LevelInfo
		case "warn":
			level = slog.LevelWarn
		case "error":
			level = slog.LevelError
		default:
			level = slog.LevelInfo
		}

		// Determine output writer with fallback to stdout
		var output io.Writer
		outputStr := logOutput
		if outputStr == "" {
			outputStr = "stdout" // Default fallback
		}
		switch strings.ToLower(outputStr) {
		case "stdout":
			output = os.Stdout
		case "stderr":
			output = os.Stderr
		default:
			// Treat as file path
			file, err := os.OpenFile(outputStr, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
			if err != nil {
				// Fallback to stdout if file creation fails
				output = os.Stdout
			} else {
				output = file
			}
		}

		// Create handler based on format with fallback to json
		var handler slog.Handler
		formatStr := logFormat
		if formatStr == "" {
			formatStr = "json" // Default fallback
		}
		switch strings.ToLower(formatStr) {
		case "text":
			handler = slog.NewTextHandler(output, &slog.HandlerOptions{
				Level: level,
			})
		case "json":
			handler = slog.NewJSONHandler(output, &slog.HandlerOptions{
				Level: level,
			})
		default:
			handler = slog.NewJSONHandler(output, &slog.HandlerOptions{
				Level: level,
			})
		}

		// Ensure we have a valid handler
		if handler == nil {
			// Ultimate fallback: create a basic JSON handler
			handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
				Level: slog.LevelInfo,
			})
		}

		// Create logger
		newLogger := slog.New(handler)

		// Set as default slog logger
		slog.SetDefault(newLogger)

		// Store logger
		loggerMu.Lock()
		logger = newLogger
		loggerMu.Unlock()

		// Fire setup hooks
		fireLoggingSetupHooks(newLogger)

		// Log initial configuration
		newLogger.Info("logging initialized",
			"level", levelStr,
			"format", formatStr,
			"output", outputStr,
		)
	})
}

// GetLogger returns the configured logger instance.
// SetupLogging must be called before this function.
func GetLogger() *slog.Logger {
	loggerMu.RLock()
	defer loggerMu.RUnlock()
	if logger == nil {
		// Return default slog logger if our logger hasn't been set up yet
		return slog.Default()
	}
	return logger
}

// fireLoggingSetupHooks calls all registered logging setup hooks.
func fireLoggingSetupHooks(l *slog.Logger) {
	loggingHooksMu.RLock()
	hooks := make([]func(*slog.Logger), len(loggingSetupHooks))
	copy(hooks, loggingSetupHooks)
	loggingHooksMu.RUnlock()

	for _, hook := range hooks {
		hook(l)
	}
}

// fireLoggingChangeHooks calls all registered logging change hooks.
func fireLoggingChangeHooks(l *slog.Logger) {
	loggingHooksMu.RLock()
	hooks := make([]func(*slog.Logger), len(loggingChangeHooks))
	copy(hooks, loggingChangeHooks)
	loggingHooksMu.RUnlock()

	for _, hook := range hooks {
		hook(l)
	}
}

// GetLogLevel returns the current log level setting.
func GetLogLevel() string {
	return logLevel
}

// GetLogFormat returns the current log format setting.
func GetLogFormat() string {
	return logFormat
}

// GetLogOutput returns the current log output setting.
func GetLogOutput() string {
	return logOutput
}
