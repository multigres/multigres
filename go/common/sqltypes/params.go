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

package sqltypes

// ParamsToProto converts [][]byte params to proto Portal format (lengths+values).
// Encoding: -1 = NULL, 0 = empty string, >0 = actual length.
func ParamsToProto(params [][]byte) (lengths []int64, values []byte) {
	lengths = make([]int64, len(params))
	var totalLen int
	for i, p := range params {
		if p == nil {
			lengths[i] = -1
		} else {
			lengths[i] = int64(len(p))
			totalLen += len(p)
		}
	}

	values = make([]byte, 0, totalLen)
	for _, p := range params {
		if p != nil {
			values = append(values, p...)
		}
	}

	return lengths, values
}

// ParamsFromProto converts proto Portal params (lengths+values) to [][]byte.
// Decoding: -1 = NULL, 0 = empty string, >0 = actual length.
func ParamsFromProto(lengths []int64, values []byte) [][]byte {
	params := make([][]byte, len(lengths))
	offset := 0
	for i, length := range lengths {
		switch length {
		case -1:
			params[i] = nil // NULL
		case 0:
			params[i] = []byte{} // empty string, not NULL
		default:
			params[i] = values[offset : offset+int(length)]
			offset += int(length)
		}
	}
	return params
}
