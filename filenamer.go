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

import (
	"crypto/md5"
	"encoding/base64"
	"path/filepath"
)

const DefaultFileExt = ".evtq"

func md5Name(data []byte) string {
	sum := md5.Sum(data)
	return base64.URLEncoding.EncodeToString(sum[:])[:22]
}

func Filename(data []byte, ext string) string {
	return md5Name(data) + ext
}

func CheckFilenameMD5(data []byte, path, ext string) bool {
	name := filepath.Base(path)
	fileExt := filepath.Ext(name)
	if fileExt != ext {
		return false
	}
	expected := name[:len(name)-len(fileExt)]
	return expected == md5Name(data)
}
