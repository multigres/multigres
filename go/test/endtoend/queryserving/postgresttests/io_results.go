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

package postgresttests

import (
	"regexp"
	"strings"
)

// parsePytestOutput extracts per-test outcomes from `pytest -v` stdout and maps
// them onto specResult (shared with the hspec suite so the report/classifier
// code is common). It reads the numbered per-test verbose lines rather than the
// final "N passed, M failed" summary, so the failing set is the exact node ids
// — which is what the divergence report needs. FAILED and ERROR both count as
// failures; SKIPPED/XFAIL count as pending; PASSED/XPASS pass.
//
// A verbose line looks like:
//
//	test/io/test_io.py::test_role_settings PASSED                    [  6%]
//	test/io/test_io.py::test_statement_timeout FAILED                [ 13%]
func parsePytestOutput(out string) *specResult {
	out = stripANSI(out)
	res := &specResult{}

	for line := range strings.SplitSeq(out, "\n") {
		m := pytestLineRE.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		node, status := m[1], m[2]
		res.Total++
		switch status {
		case "FAILED", "ERROR":
			res.Failures++
			res.Failing = append(res.Failing, node)
		case "SKIPPED", "XFAIL":
			res.Pending++
		}
	}
	return res
}

// pytestLineRE matches a verbose per-test result line: a node id (path::test,
// optionally with [params]) followed by the outcome word. Anchored on the node
// id shape so it ignores traceback lines that merely mention a status word.
var pytestLineRE = regexp.MustCompile(`^(\S+\.py::\S+?)\s+(PASSED|FAILED|ERROR|SKIPPED|XFAIL|XPASS)\b`)
