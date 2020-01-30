package api

import (
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
)

func TestSplitPath(t *testing.T) {
	tests := []struct {
		path   string
		prefix string
		suffix string
	}{
		{
			path:   "/",
			prefix: "",
			suffix: "/",
		},
		{
			path:   "/file.png",
			prefix: "file.png",
			suffix: "/",
		},
		{
			path:   "/my/base/path.png",
			prefix: "my",
			suffix: "/base/path.png",
		},
		{
			path:   "/api/add/a",
			prefix: "api",
			suffix: "/add/a",
		},
		{
			path:   "/add/a",
			prefix: "add",
			suffix: "/a",
		},
		{
			path:   "/a",
			prefix: "a",
			suffix: "/",
		},
		{
			path:   "a", // illegal input, should always start with /
			prefix: "",
			suffix: "/",
		},
	}
	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			// execute
			got, req := SplitPath(&http.Request{RequestURI: test.path})

			// verify
			assert.Equal(t, test.prefix, got, "wrong prefix")
			assert.Equal(t, test.suffix, req.RequestURI, "wrong suffix")
		})
	}
}
