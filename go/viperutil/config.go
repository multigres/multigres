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

package viperutil

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/mitchellh/mapstructure"

	"github.com/multigres/multigres/go/viperutil/funcs"
	"github.com/multigres/multigres/go/viperutil/internal/value"
)

type ViperConfig struct {
	configPaths                  Value[[]string]
	configType                   Value[string]
	configName                   Value[string]
	configFile                   Value[string]
	configFileNotFoundHandling   Value[ConfigFileNotFoundHandling]
	configPersistenceMinInterval Value[time.Duration]
}

func NewViperConfig(reg *Registry) *ViperConfig {
	vc := &ViperConfig{
		configPaths: Configure(
			reg,
			"config.paths",
			Options[[]string]{
				GetFunc:  funcs.GetPath,
				EnvVars:  []string{"VT_CONFIG_PATH"},
				FlagName: "config-path",
			},
		),
		configType: Configure(
			reg,
			"config.type",
			Options[string]{
				EnvVars:  []string{"MT_CONFIG_TYPE"},
				FlagName: "config-type",
			},
		),
		configName: Configure(
			reg,
			"config.name",
			Options[string]{
				Default:  "mtconfig",
				EnvVars:  []string{"MT_CONFIG_NAME"},
				FlagName: "config-name",
			},
		),
		configFile: Configure(
			reg,
			"config.file",
			Options[string]{
				EnvVars:  []string{"MT_CONFIG_FILE"},
				FlagName: "config-file",
			},
		),
		configFileNotFoundHandling: Configure(
			reg,
			"config.notfound.handling",
			Options[ConfigFileNotFoundHandling]{
				Default:  WarnOnConfigFileNotFound,
				GetFunc:  getHandlingValue,
				FlagName: "config-file-not-found-handling",
			},
		),
		configPersistenceMinInterval: Configure(
			reg,
			"config.persistence.min_interval",
			Options[time.Duration]{
				Default:  time.Second,
				EnvVars:  []string{"MT_CONFIG_PERSISTENCE_MIN_INTERVAL"},
				FlagName: "config-persistence-min-interval",
			},
		),
	}

	// Use MTDATAROOT environment variable if set, otherwise fall back to pwd/multigres_local
	baseDir := os.Getenv("MTDATAROOT")
	if baseDir == "" {
		if cur, err := os.Getwd(); err != nil {
			slog.Warn("failed to get working directory", "err", err)
			return vc
		} else {
			baseDir = filepath.Join(cur, "multigres_local")
		}
	}

	vc.configPaths.(*value.Static[[]string]).DefaultVal = []string{baseDir}
	// Need to re-trigger the SetDefault call done during Configure.
	reg.static.SetDefault(vc.configPaths.Key(), vc.configPaths.Default())
	return vc
}

// RegisterFlags installs the flags that control viper config-loading behavior.
// It is exported to be called by servenv before parsing flags for all binaries.
//
// It cannot be registered here via servenv.OnParse since this causes an import
// cycle.
func (vc *ViperConfig) RegisterFlags(fs *pflag.FlagSet) {
	fs.StringSlice("config-path", vc.configPaths.Default(), "Paths to search for config files in.")
	fs.String("config-type", vc.configType.Default(), "Config file type (omit to infer config type from file extension).")
	fs.String("config-name", vc.configName.Default(), "Name of the config file (without extension) to search for.")
	fs.String("config-file", vc.configFile.Default(), "Full path of the config file (with extension) to use. If set, --config-path, --config-type, and --config-name are ignored.")
	fs.Duration("config-persistence-min-interval", vc.configPersistenceMinInterval.Default(), "minimum interval between persisting dynamic config changes back to disk (if no change has occurred, nothing is done).")

	h := vc.configFileNotFoundHandling.Default()
	fs.Var(&h, "config-file-not-found-handling", fmt.Sprintf("Behavior when a config file is not found. (Options: %s)", strings.Join(handlingNames, ", ")))

	BindFlags(fs, vc.configPaths, vc.configType, vc.configName, vc.configFile, vc.configFileNotFoundHandling, vc.configPersistenceMinInterval)
}

// LoadConfig attempts to find, and then load, a config file for viper-backed
// config values to use.
//
// Config searching follows the behavior used by viper [1], namely:
//   - --config-file (full path, including extension) if set will be used to the
//     exclusion of all other flags.
//   - --config-type is required if the config file does not have one of viper's
//     supported extensions (.yaml, .yml, .json, and so on)
//
// An additional --config-file-not-found-handling flag controls how to treat the
// situation where viper cannot find any config files in any of the provided
// paths (for ex, users may want to exit immediately if a config file that
// should exist doesn't for some reason, or may wish to operate with flags and
// environment variables alone, and not use config files at all).
//
// If a config file is successfully loaded, then the dynamic registry will also
// start watching that file for changes. In addition, in-memory changes to the
// config (for example, from a vtgate or vttablet's debugenv) will be persisted
// back to disk, with writes occuring no more frequently than the
// --config-persistence-min-interval flag.
//
// A cancel function is returned to stop the re-persistence background thread,
// if one was started.
//
// [1]: https://github.com/spf13/viper#reading-config-files.
func (vc *ViperConfig) LoadConfig(reg *Registry) (context.CancelFunc, error) {
	var err error
	switch file := vc.configFile.Get(); file {
	case "":
		if name := vc.configName.Get(); name != "" {
			reg.static.SetConfigName(name)

			for _, path := range vc.configPaths.Get() {
				reg.static.AddConfigPath(path)
			}

			if cfgType := vc.configType.Get(); cfgType != "" {
				reg.static.SetConfigType(cfgType)
			}

			err = reg.static.ReadInConfig()
		}
	default:
		reg.static.SetConfigFile(file)
		err = reg.static.ReadInConfig()
	}

	if err != nil {
		if isConfigFileNotFoundError(err) {
			msg := "Failed to read in config %s: %s"
			switch vc.configFileNotFoundHandling.Get() {
			case WarnOnConfigFileNotFound:
				// TODO: @rafael (add warning and point to docs)
				fallthrough // after warning, ignore the error
			case IgnoreConfigFileNotFound:
				return func() {}, nil
			case ErrorOnConfigFileNotFound:
				slog.Error(fmt.Sprintf(msg, reg.static.ConfigFileUsed(), err.Error()))
			case ExitOnConfigFileNotFound:
				slog.Error(fmt.Sprintf(msg, reg.static.ConfigFileUsed(), err.Error()))
			}
		}
	}

	if err != nil {
		return nil, err
	}

	return reg.dynamic.Watch(context.Background(), reg.static, vc.configPersistenceMinInterval.Get())
}

// isConfigFileNotFoundError checks if the error is caused because the file wasn't found.
func isConfigFileNotFoundError(err error) bool {
	if errors.As(err, &viper.ConfigFileNotFoundError{}) {
		return true
	}
	return errors.Is(err, os.ErrNotExist)
}

// NotifyConfigReload adds a subscription that the dynamic registry will attempt
// to notify on config changes. The notification fires after the updated config
// has been loaded from disk into the live config.
//
// Analogous to signal.Notify, notifications are sent non-blocking, so users
// should account for this when writing code to consume from the channel.
//
// This function must be called prior to LoadConfig; it will panic if called
// after the dynamic registry has started watching the loaded config.
func NotifyConfigReload(reg *Registry, ch chan<- struct{}) {
	reg.dynamic.Notify(ch)
}

// ConfigFileNotFoundHandling is an enum to control how LoadConfig treats errors
// of type viper.ConfigFileNotFoundError when loading a config.
type ConfigFileNotFoundHandling int

const (
	// IgnoreConfigFileNotFound causes LoadConfig to completely ignore a
	// ConfigFileNotFoundError (i.e. not even logging it).
	IgnoreConfigFileNotFound ConfigFileNotFoundHandling = iota
	// WarnOnConfigFileNotFound causes LoadConfig to log a warning with details
	// about the failed config load, but otherwise proceeds with the given
	// process, which will get config values entirely from defaults,
	// envirnoment variables, and flags.
	WarnOnConfigFileNotFound
	// ErrorOnConfigFileNotFound causes LoadConfig to return the
	// ConfigFileNotFoundError after logging an error.
	ErrorOnConfigFileNotFound
	// ExitOnConfigFileNotFound causes LoadConfig to log.Fatal on a
	// ConfigFileNotFoundError.
	ExitOnConfigFileNotFound
)

var (
	handlingNames         []string
	handlingNamesToValues = map[string]int{
		"ignore": int(IgnoreConfigFileNotFound),
		"warn":   int(WarnOnConfigFileNotFound),
		"error":  int(ErrorOnConfigFileNotFound),
		"exit":   int(ExitOnConfigFileNotFound),
	}
	handlingValuesToNames map[int]string
)

func getHandlingValue(v *viper.Viper) func(key string) ConfigFileNotFoundHandling {
	return func(key string) (h ConfigFileNotFoundHandling) {
		if err := v.UnmarshalKey(key, &h, viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(decodeHandlingValue))); err != nil {
			h = IgnoreConfigFileNotFound
			slog.Warn(fmt.Sprintf("failed to unmarshal %s: %s; defaulting to %s", key, err.Error(), h.String()))
		}

		return h
	}
}

func decodeHandlingValue(from, to reflect.Type, data any) (any, error) {
	var h ConfigFileNotFoundHandling
	if to != reflect.TypeOf(h) {
		return data, nil
	}

	switch {
	case from == reflect.TypeOf(h):
		return data.(ConfigFileNotFoundHandling), nil
	case from.Kind() == reflect.Int:
		return ConfigFileNotFoundHandling(data.(int)), nil
	case from.Kind() == reflect.String:
		if err := h.Set(data.(string)); err != nil {
			return h, err
		}

		return h, nil
	}

	return data, fmt.Errorf("invalid value for ConfigHandlingType: %v", data)
}

func init() {
	handlingNames = make([]string, 0, len(handlingNamesToValues))
	handlingValuesToNames = make(map[int]string, len(handlingNamesToValues))

	for name, val := range handlingNamesToValues {
		handlingValuesToNames[val] = name
		handlingNames = append(handlingNames, name)
	}

	sort.Slice(handlingNames, func(i, j int) bool {
		return handlingNames[i] < handlingNames[j]
	})
}

func (h *ConfigFileNotFoundHandling) Set(arg string) error {
	larg := strings.ToLower(arg)
	if v, ok := handlingNamesToValues[larg]; ok {
		*h = ConfigFileNotFoundHandling(v)
		return nil
	}

	return fmt.Errorf("unknown handling name %s", arg)
}

func (h *ConfigFileNotFoundHandling) String() string {
	if name, ok := handlingValuesToNames[int(*h)]; ok {
		return name
	}

	return "<UNKNOWN>"
}

func (h *ConfigFileNotFoundHandling) Type() string { return "ConfigFileNotFoundHandling" }
