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

var __1_create_records_up_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\xa4\x94\x4f\x6f\xe2\x30\x10\xc5\xef\xf9\x14\x73\x2b\x48\x20\xed\x9d\x53\xda\x86\x36\x12\x1b\xb4\x10\xb6\xd5\x5e\x22\x03\x43\xb0\x94\x78\x2c\xdb\xd9\x6d\xf7\xd3\xaf\xf2\xc7\x4e\x61\x09\x04\xc8\x2d\xb2\xe7\xf9\xd9\xf3\x9b\x37\x1e\x03\x17\xdc\x70\x96\x81\xde\xec\x31\x67\xb0\x23\x05\x66\x8f\x90\xa3\xd6\x2c\x45\xed\x3d\x2d\x02\x3f\x0e\x20\xf6\x1f\x67\x01\x84\x53\x88\xe6\x31\x04\xef\xe1\x32\x5e\x82\xa4\x24\xd7\xa9\xf6\x06\x1e\x00\x00\xdf\x42\xfb\xad\x79\xaa\x51\x95\xba\x67\xbf\x52\x2c\x5a\xcd\x66\xa3\x4a\x61\xa3\x90\x19\x6c\x64\x0c\xcf\x51\x1b\x96\x4b\x78\x0b\xe3\x57\x88\xc3\xef\x01\xfc\x9a\x47\x01\x3c\x07\x53\x7f\x35\x8b\x21\x9a\xbf\x0d\x86\x47\x0a\x85\xdc\xde\xa9\xa0\x8d\x42\x96\x37\xf6\x7e\xfa\x8b\xa7\x57\x7f\x71\xfe\x0e\xc7\xb7\x10\xf4\x65\xe9\x31\x7c\x09\xa3\xf8\x44\x8d\xf5\xf0\xed\x84\x42\xaa\x64\xbb\xf1\x37\x53\x9b\x3d\x53\xbd\x3d\xc0\x78\x0c\xa9\xa2\x42\x02\x17\xf0\x42\x56\x30\x71\xb6\x3a\x2d\x1d\x09\xb6\x6d\x21\x61\x50\x98\xc4\x7c\x4a\xbc\xda\x4e\xa5\xb0\x65\x86\xb9\xa5\xf5\xa7\x41\x76\xf1\x78\xa7\xe0\x0d\x27\x9e\xb7\xa1\x3c\x47\x61\x80\x04\x18\xb6\xce\xd0\xa2\x07\x5c\xc3\x43\xe9\x8f\x71\xa1\x1d\xb3\x0f\x13\xcf\x62\x1b\x46\xcf\xc1\xfb\x69\x6c\x93\xba\xd5\x09\x17\x5b\xfc\x80\x79\xe4\x34\x07\xf5\xc2\x70\xd2\x47\xa4\x7c\xda\xff\x15\x52\x25\xdb\xf2\x55\x14\xfe\x58\xf5\xb2\x22\x8a\x7c\x8d\x2a\x29\xba\x2c\x8d\x40\xd0\x55\xba\xa5\xbb\xaa\xf9\x9d\xc2\xa9\x92\xa3\x86\x8f\x61\xfb\x6c\xdd\xd3\x5e\x5f\xb6\x19\x79\x3b\x6e\xed\xa4\xfd\xe1\x66\x5f\xfd\xc2\x5f\x12\x08\x5b\xdc\xb1\x22\x33\xcd\xa4\x09\x32\x20\x8a\x2c\x3b\x9c\xf6\x9b\x8a\xed\x98\xde\x36\xa1\xf8\x61\xa0\x0e\x29\x2e\xcc\x89\xed\xf6\xe4\x7a\x38\x0f\x4f\xb6\xc3\xb9\x26\x3a\x1d\x6e\xb6\x78\xea\xcf\x96\x81\x2b\x3e\x87\x71\x03\xd0\x01\xcb\x65\x06\x57\x3e\xeb\xd6\x01\xd3\x9a\xa7\xa2\x7c\x2e\x02\xd6\x5c\xff\x0b\xe7\x17\x59\xa8\xcf\xb0\xa0\x95\x0d\x3f\x86\xa1\x71\xe1\x50\xab\x19\xbe\x40\x84\x24\x7d\xc4\x42\xf5\xf5\xec\x69\xaf\xf8\xbf\x42\xe1\x20\xbc\x6f\x4a\xaa\x8c\x6b\x83\x02\xd5\xed\x0a\x07\xf1\xdf\x49\xd8\x39\x85\x7b\xd2\xf2\xfe\xc4\xee\x04\x55\x52\x1d\xb7\x92\x34\x37\x9c\x04\xd0\xce\x91\xe8\xde\xad\x37\x92\x92\x5c\xea\xd9\xda\x86\xc8\xea\x06\x24\xec\x91\x8e\x47\xbb\xad\xf4\xf7\x2f\x00\x00\xff\xff\x7f\x25\x8c\xa6\xb8\x08\x00\x00")

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
