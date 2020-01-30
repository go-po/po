package api

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type call struct {
	name string
	val  int64
}

type mockApp struct {
	declared     []call
	added        []call
	subtracted   []call
	gets         []call
	currentValue int64
	err          error
}

func (mock *mockApp) Declare(ctx context.Context, name string) error {
	mock.declared = append(mock.declared, call{name: name, val: 0})
	return mock.err
}

func (mock *mockApp) Add(ctx context.Context, name string, v int64) error {
	mock.added = append(mock.added, call{name: name, val: v})
	return mock.err
}

func (mock *mockApp) Sub(ctx context.Context, name string, v int64) error {
	mock.subtracted = append(mock.subtracted, call{name: name, val: v})
	return mock.err
}

func (mock *mockApp) GetValue(ctx context.Context, name string) (int64, error) {
	mock.gets = append(mock.gets, call{name: name, val: 0})
	return mock.currentValue, mock.err
}

func TestApi(t *testing.T) {
	type verify func(t *testing.T, w *httptest.ResponseRecorder, app *mockApp)
	all := func(v ...verify) []verify { return v }
	code := func(expected int) verify {
		return func(t *testing.T, w *httptest.ResponseRecorder, app *mockApp) {
			assert.Equal(t, expected, w.Code, "http code")
		}
	}
	declared := func(expected string) verify {
		return func(t *testing.T, w *httptest.ResponseRecorder, app *mockApp) {
			assert.Contains(t, app.declared, call{
				name: expected,
				val:  0,
			})
		}
	}
	added := func(expected string, value int64) verify {
		return func(t *testing.T, w *httptest.ResponseRecorder, app *mockApp) {
			assert.Contains(t, app.added, call{
				name: expected,
				val:  value,
			})

		}
	}
	subtracted := func(expected string, value int64) verify {
		return func(t *testing.T, w *httptest.ResponseRecorder, app *mockApp) {
			assert.Contains(t, app.subtracted, call{
				name: expected,
				val:  value,
			})

		}
	}
	body := func(expected string) verify {
		return func(t *testing.T, w *httptest.ResponseRecorder, app *mockApp) {
			b, err := ioutil.ReadAll(w.Body)
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			assert.Equal(t, expected, string(b))
		}
	}
	tests := []struct {
		url    string
		method string
		body   io.ReadCloser
		verify []verify
	}{
		{
			url:    "/not/here",
			method: http.MethodGet,
			body:   nil,
			verify: all(
				code(404),
			),
		},
		{
			url:    "/api/vars/a",
			method: http.MethodGet,
			body:   nil,
			verify: all(
				code(200),
				body("a=0"),
			),
		},
		{
			url:    "/api/vars/a",
			method: http.MethodPost,
			body:   nil,
			verify: all(
				code(200),
				declared("a"),
			),
		},
		{
			url:    "/api/vars/a",
			method: http.MethodPut,
			body:   ioutil.NopCloser(strings.NewReader("42")),
			verify: all(
				code(200),
				added("a", 42),
				body("OK"),
			),
		},
		{
			url:    "/api/vars/a",
			method: http.MethodPut,
			body:   ioutil.NopCloser(strings.NewReader("-10")),
			verify: all(
				code(200),
				subtracted("a", -10),
				body("OK"),
			),
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("%d %s %s", i, test.method, test.url), func(t *testing.T) {
			// setup
			req := &http.Request{
				Method:     test.method,
				RequestURI: test.url,
				Body:       test.body,
			}
			w := httptest.NewRecorder()
			app := &mockApp{}

			// execute
			Root(app).ServeHTTP(w, req)

			// verify
			for _, v := range test.verify {
				v(t, w, app)
			}

		})
	}
}
