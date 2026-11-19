// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package local

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServiceEnv_DropsRepositoryCredentials(t *testing.T) {
	secrets := []string{
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
		"AWS_ACCESS_KEY", "AWS_SECRET_KEY", "AWS_DEFAULT_PROFILE",
		"AWS_SHARED_CREDENTIALS_FILE", "AWS_CONFIG_FILE", "AWS_PROFILE",
		"AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN",
		"PGBACKREST_REPO1_S3_KEY", "PGBACKREST_REPO2_S3_KEY_SECRET", "PGBACKREST_REPO1_CIPHER_PASS",
	}
	for _, name := range secrets {
		t.Setenv(name, "dummy-"+name)
	}
	t.Setenv("MULTIGRES_KEPT", "yes")

	env := serviceEnv("PGDATA=/x")
	for _, kv := range env {
		name, _, _ := strings.Cut(kv, "=")
		assert.NotContains(t, secrets, name, "a repository credential reached a service environment")
		assert.NotContains(t, kv, "dummy-")
	}
	assert.Contains(t, env, "MULTIGRES_KEPT=yes", "the rest of the environment is inherited")
	assert.Contains(t, env, "PGDATA=/x")
	assert.True(t, slices.ContainsFunc(env, func(kv string) bool { return strings.HasPrefix(kv, "PATH=") }))
}

// Every process this package starts must be given an explicit environment: a
// command without one inherits the launcher's, S3 keys included. The rule is
// checked on the source so a start added later cannot skip it.
func TestEveryStartedProcessGetsAnExplicitEnvironment(t *testing.T) {
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	fset := token.NewFileSet()
	starts := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		require.NoError(t, err)
		if name != "service_env.go" {
			assert.NotContains(t, string(src), "os.Environ()", "%s: build a child environment with serviceEnv", name)
		}
		file, err := parser.ParseFile(fset, name, src, 0)
		require.NoError(t, err)
		var stack []ast.Node
		ast.Inspect(file, func(n ast.Node) bool {
			if n == nil {
				stack = stack[:len(stack)-1]
				return true
			}
			stack = append(stack, n)
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Command" {
				return true
			}
			pkg, ok := sel.X.(*ast.Ident)
			if !ok || (pkg.Name != "executil" && pkg.Name != "exec") {
				return true
			}
			starts++
			if pkg.Name == "exec" {
				// os/exec has no SetEnv; its one use sets Env on the next line.
				assert.Equal(t, "service_env.go", name, "%s: start processes with executil, or through binaryVersionOutput", fset.Position(call.Pos()))
				return true
			}
			// The command must be the receiver of an immediate .SetEnv(...).
			explicit := false
			if len(stack) >= 3 {
				if parent, ok := stack[len(stack)-2].(*ast.SelectorExpr); ok && parent.Sel.Name == "SetEnv" {
					_, explicit = stack[len(stack)-3].(*ast.CallExpr)
				}
			}
			assert.True(t, explicit, "%s: %s.Command without .SetEnv(...)", fset.Position(call.Pos()), pkg.Name)
			return true
		})
	}
	require.GreaterOrEqual(t, starts, 8, "the check must see the service starts")
}
