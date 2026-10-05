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

import "context"

type contextKey struct{}

var eventContextKey = contextKey{}

func NewContextForEvent(ctx context.Context, evt *Event) context.Context {
	return context.WithValue(ctx, eventContextKey, evt)
}

func GetEventFromContext(ctx context.Context) *Event {
	evt, _ := ctx.Value(eventContextKey).(*Event)
	return evt
}
