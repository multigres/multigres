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

package backup

import (
	"context"
	"fmt"
	"time"
)

// DefaultIOTimeout is pgBackRest's own default for --io-timeout.
const DefaultIOTimeout = time.Minute

// IOTimeoutArgs returns --io-timeout as two thirds of the time left on ctx, in
// whole seconds and at least one, when that is under pgBackRest's default, so
// pgbackrest reports an unreachable or stalled repository itself before the
// context kills it. Only for commands that use the repository: start and stop
// reject the option.
func IOTimeoutArgs(ctx context.Context) []string {
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil
	}
	budget := time.Until(deadline) * 2 / 3
	if budget >= DefaultIOTimeout {
		return nil
	}
	return []string{fmt.Sprintf("--io-timeout=%d", max(int(budget/time.Second), 1))}
}
