// Copyright 2026 DoltHub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the root
// of this repository or at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eventkit

import "encoding/json"

type Codec interface {
	Marshal(*LogEventsRequest) ([]byte, error)
	Unmarshal([]byte, *LogEventsRequest) error
}

type JSONCodec struct{}

func (JSONCodec) Marshal(req *LogEventsRequest) ([]byte, error) {
	return json.Marshal(req)
}

func (JSONCodec) Unmarshal(data []byte, req *LogEventsRequest) error {
	return json.Unmarshal(data, req)
}

var DefaultCodec Codec = JSONCodec{}
