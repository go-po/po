package db

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"strings"
)

func bindata_read(data []byte, name string) ([]byte, error) {
	gz, err := gzip.NewReader(bytes.NewBuffer(data))
	if err != nil {
		return nil, fmt.Errorf("Read %q: %v", name, err)
	}

	var buf bytes.Buffer
	_, err = io.Copy(&buf, gz)
	gz.Close()

	if err != nil {
		return nil, fmt.Errorf("Read %q: %v", name, err)
	}

	return buf.Bytes(), nil
}

var __1_create_records_down_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x01\x00\x00\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00")

func _1_create_records_down_sql() ([]byte, error) {
	return bindata_read(
		__1_create_records_down_sql,
		"1_create_records.down.sql",
	)
}

var __1_create_records_up_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\xa4\x93\x41\x4f\xe3\x30\x10\x85\xef\xfe\x15\xef\xd8\x4a\xad\xc4\x9d\x53\x00\x03\x96\xba\x8e\xb6\x38\x0b\xda\x4b\x64\x12\x93\x5a\x4a\x6c\xcb\x76\x56\xf0\xef\x57\x4d\x48\x9a\x2d\x8d\x5a\xb4\x3e\x66\xe2\x6f\xde\xcc\x7b\x5e\xaf\xa1\x8d\x8e\x5a\xd6\x08\xc5\x4e\x35\x12\x6f\xd6\x23\xee\x14\x1a\x15\x82\xac\x54\x20\xb7\x5b\x9a\x08\x0a\x91\xdc\x6c\x28\xd8\x3d\x78\x2a\x40\x5f\xd8\x93\x78\x82\xb3\x79\x13\xaa\x40\x16\x84\x00\x40\xe1\x95\x8c\xaa\x44\x77\xa2\x6e\x54\x88\xb2\x71\x78\x66\xe2\x11\x82\xfd\xa0\xf8\x9d\x72\x8a\x3b\x7a\x9f\x64\x1b\x01\x9e\x3e\x2f\x96\x1d\x8d\x67\x9b\xcd\xaa\x23\xb4\xae\xfc\x4f\x42\x88\x5e\xc9\xa6\x07\xe0\x57\xb2\xbd\x7d\x4c\xb6\x38\x73\xfe\x25\x18\x3b\x29\xdd\xb0\x07\xc6\xc5\x89\x3b\x83\x86\xab\x13\x84\xca\xbb\xc3\x8f\x7f\xa4\x2f\x76\xd2\x5f\xac\x01\xeb\x35\x2a\x6f\x5b\x07\x6d\xf0\x60\x07\x60\x3e\xca\x9a\x95\x74\x04\x1c\xe5\x14\xd6\x44\x65\x62\x1e\x3f\x9c\xfa\xb6\x9c\x8e\x50\xca\x28\xc7\xd2\xeb\x47\x54\xf2\x6c\xfb\x91\x40\x96\xd7\x64\xc8\x10\xe3\x77\xf4\xe5\x74\x86\xf2\xde\xb8\x5c\x9b\x52\xbd\x23\xe5\xc3\x77\x2c\xfa\xc2\xf2\xfa\x12\xc8\x7e\x51\x5f\x09\x95\x77\x87\xeb\x19\x67\x3f\xb3\x8b\xa4\x98\xb6\x79\x55\x3e\x6f\xe7\x24\xad\x60\xec\xb7\xb8\x7b\x75\x9d\x95\xb3\xe0\xca\xbb\xd5\xa7\xdb\x93\xb5\xcd\x3f\xbd\x7e\x58\xb2\x98\x46\xff\x4b\xe8\x8f\xe2\xad\xde\xe3\xde\x45\x5d\x69\x13\x51\xaa\x37\xd9\xd6\x11\x57\x30\x36\xc2\xb4\x75\x3d\xf5\xeb\xec\x4c\x7d\xff\x61\x61\xc7\x03\x7d\x5a\x71\x70\x90\xfc\x0d\x00\x00\xff\xff\x0e\x3b\xbc\xa1\x6f\x04\x00\x00")

func _1_create_records_up_sql() ([]byte, error) {
	return bindata_read(
		__1_create_records_up_sql,
		"1_create_records.up.sql",
	)
}

// Asset loads and returns the asset for the given name.
// It returns an error if the asset could not be found or
// could not be loaded.
func Asset(name string) ([]byte, error) {
	cannonicalName := strings.Replace(name, "\\", "/", -1)
	if f, ok := _bindata[cannonicalName]; ok {
		return f()
	}
	return nil, fmt.Errorf("Asset %s not found", name)
}

// AssetNames returns the names of the assets.
func AssetNames() []string {
	names := make([]string, 0, len(_bindata))
	for name := range _bindata {
		names = append(names, name)
	}
	return names
}

// _bindata is a table, holding each asset generator, mapped to its name.
var _bindata = map[string]func() ([]byte, error){
	"1_create_records.down.sql": _1_create_records_down_sql,
	"1_create_records.up.sql": _1_create_records_up_sql,
}
// AssetDir returns the file names below a certain
// directory embedded in the file by go-bindata.
// For example if you run go-bindata on data/... and data contains the
// following hierarchy:
//     data/
//       foo.txt
//       img/
//         a.png
//         b.png
// then AssetDir("data") would return []string{"foo.txt", "img"}
// AssetDir("data/img") would return []string{"a.png", "b.png"}
// AssetDir("foo.txt") and AssetDir("notexist") would return an error
// AssetDir("") will return []string{"data"}.
func AssetDir(name string) ([]string, error) {
	node := _bintree
	if len(name) != 0 {
		cannonicalName := strings.Replace(name, "\\", "/", -1)
		pathList := strings.Split(cannonicalName, "/")
		for _, p := range pathList {
			node = node.Children[p]
			if node == nil {
				return nil, fmt.Errorf("Asset %s not found", name)
			}
		}
	}
	if node.Func != nil {
		return nil, fmt.Errorf("Asset %s not found", name)
	}
	rv := make([]string, 0, len(node.Children))
	for name := range node.Children {
		rv = append(rv, name)
	}
	return rv, nil
}

type _bintree_t struct {
	Func func() ([]byte, error)
	Children map[string]*_bintree_t
}
var _bintree = &_bintree_t{nil, map[string]*_bintree_t{
	"1_create_records.down.sql": &_bintree_t{_1_create_records_down_sql, map[string]*_bintree_t{
	}},
	"1_create_records.up.sql": &_bintree_t{_1_create_records_up_sql, map[string]*_bintree_t{
	}},
}}
