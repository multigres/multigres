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

package plpgsql

import "fmt"

// checkLabels validates a block's end label against its start label, mirroring
// PG's check_labels (pl_gram.y). An END label is allowed only when the block
// has a matching BEGIN label; the error messages match PG's verbatim.
func checkLabels(startLabel, endLabel string) error {
	if endLabel == "" {
		return nil
	}
	if startLabel == "" {
		return fmt.Errorf("end label %q specified for unlabeled block", endLabel)
	}
	if startLabel != endLabel {
		return fmt.Errorf("end label %q differs from block's label %q", endLabel, startLabel)
	}
	return nil
}
