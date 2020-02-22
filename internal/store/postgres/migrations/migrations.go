package migrations

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

var __1_create_records_up_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\xa4\x93\x41\x4f\xe3\x30\x10\x85\xef\xfe\x15\xef\xd8\x4a\xad\xc4\x9d\x53\x00\x03\x96\xb2\x8e\xb6\x38\x0b\xda\x4b\x64\x12\x93\x5a\x4a\x6c\xcb\x76\x56\xf0\xef\x57\x4d\x08\xc9\x42\xab\xb6\xcb\x1c\x33\x99\x2f\x6f\xe6\xbd\xac\xd7\xd0\x46\x47\x2d\x1b\x84\x72\xab\x5a\x89\x17\xeb\x11\xb7\x0a\xad\x0a\x41\xd6\x2a\x90\xeb\x0d\x4d\x04\x85\x48\xae\x52\x0a\x76\x0b\x9e\x09\xd0\x27\xf6\x20\x1e\xe0\x6c\xd1\x86\x3a\x90\x05\x21\x00\x50\x7a\x25\xa3\xaa\xd0\x57\xd4\xad\x0a\x51\xb6\x0e\x8f\x4c\xdc\x43\xb0\x1f\x14\xbf\x33\x4e\x71\x43\x6f\x93\x3c\x15\xe0\xd9\xe3\x62\xd9\xd3\x78\x9e\xa6\xab\x9e\xd0\xb9\xea\x9b\x84\x10\xbd\x92\xed\x00\xc0\xaf\x64\x73\x7d\x9f\x6c\x70\xa4\xfe\x25\x18\x3b\x6b\x5d\xb1\x3b\xc6\xc5\x9e\x99\x51\xc3\xc5\x1e\x42\xed\xdd\xf4\xe2\x1f\xe9\xcb\xad\xf4\x27\x6b\xc0\x7a\x8d\xda\xdb\xce\x41\x1b\xdc\xd9\x11\x58\x7c\xc8\xfa\x2f\x49\xa5\x35\x51\x99\x58\xc4\x37\xa7\xce\x96\xd4\x13\x2a\x19\xe5\x47\xeb\xf9\x2d\x2a\x79\x6c\x7e\x22\x90\xe5\x25\x19\x73\xc4\xf8\x0d\x7d\xda\x9f\xa3\x62\x30\xaf\xd0\xa6\x52\xaf\xc8\xf8\xf8\x1c\x8b\xa1\xb1\xbc\x3c\x05\xb2\x3b\xd6\x57\x42\xed\xdd\x34\x9e\x73\xf6\x33\x3f\x49\x8a\xe9\xda\x67\xe5\x8b\xee\x90\xa4\x15\x8c\x3d\x8b\xbb\x53\xd7\xdb\x79\x10\x5c\x7b\xb7\x7a\x77\x7c\x76\xb6\xc3\xbf\xdf\xb0\x2c\x59\xcc\xe3\xff\x25\xf8\x9f\x22\xae\x5e\xe3\xce\x45\x5d\x6b\x13\x51\xa9\x17\xd9\x35\x11\x17\x30\x36\xc2\x74\x4d\x33\xf7\xeb\xe8\x4e\xc3\xf7\xc7\x83\x7d\x5e\xe8\xdd\x8a\xc9\x41\xf2\x37\x00\x00\xff\xff\x4d\x73\x2a\xd4\x73\x04\x00\x00")

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
	"1_create_records.up.sql":   _1_create_records_up_sql,
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
	Func     func() ([]byte, error)
	Children map[string]*_bintree_t
}

var _bintree = &_bintree_t{nil, map[string]*_bintree_t{
	"1_create_records.down.sql": &_bintree_t{_1_create_records_down_sql, map[string]*_bintree_t{}},
	"1_create_records.up.sql":   &_bintree_t{_1_create_records_up_sql, map[string]*_bintree_t{}},
}}
