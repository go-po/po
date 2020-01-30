package api

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
)

type App interface {
	Declare(ctx context.Context, name string) error
	Add(ctx context.Context, name string, v int64) error
	Sub(ctx context.Context, name string, v int64) error
	GetValue(ctx context.Context, name string) (int64, error)
}

func Root(app App) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		page, request := SplitPath(request)
		switch page {
		case "api":
			Api(app).ServeHTTP(writer, request)
		default:
			Reply(404, request.RequestURI).ServeHTTP(writer, request)
		}
	}
}

func Api(app App) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		page, request := SplitPath(request)
		switch page {
		case "vars":
			varName, request := SplitPath(request)
			Var(app, varName).ServeHTTP(writer, request)
		default:
			Reply(404, request.RequestURI).ServeHTTP(writer, request)
		}
	}
}

func Var(app App, varName string) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		if len(varName) == 0 {
			Reply(404, request.RequestURI).ServeHTTP(writer, request)
			return
		}
		var err error
		switch request.Method {
		case http.MethodGet:
			v, err := app.GetValue(request.Context(), varName)
			if err != nil {
				Reply(500, err.Error()).ServeHTTP(writer, request)
				return
			}
			out(writer, "%s=%d", varName, v)
		case http.MethodPost:
			err = app.Declare(request.Context(), varName)
			if err != nil {
				Reply(500, err.Error()).ServeHTTP(writer, request)
				return
			}
			Reply(200, "OK").ServeHTTP(writer, request)
			return
		case http.MethodPut:
			v, err := parseInt(request.Body)
			if err != nil {
				Reply(500, err.Error()).ServeHTTP(writer, request)
				return
			}
			if v < 0 {
				err = app.Sub(request.Context(), varName, v)
			} else {
				err = app.Add(request.Context(), varName, v)
			}
		default:
			Reply(404, "404").ServeHTTP(writer, request)
			return
		}
		if err != nil {
			Reply(500, err.Error()).ServeHTTP(writer, request)
		}
		Reply(200, "OK").ServeHTTP(writer, request)
	}
}

func Reply(code int, message string) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(code)
		out(writer, message)
	}
}

func out(w io.Writer, format string, args ...interface{}) {
	_, _ = fmt.Fprintf(w, format, args...)
}

func parseInt(rd io.Reader) (int64, error) {
	b, err := ioutil.ReadAll(rd)
	if err != nil {
		return 0, err
	}
	i, err := strconv.Atoi(string(b))
	if err != nil {
		return 0, err
	}
	return int64(i), nil
}
