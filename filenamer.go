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
