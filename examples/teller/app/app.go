package app

import (
	"context"
	"github.com/go-po/po"
	"github.com/go-po/po/examples/teller/commands"
	"github.com/go-po/po/examples/teller/views"
)

func New(dao *po.Po) *App {
	return &App{dao: dao}
}

type App struct {
	dao *po.Po
}

func (app *App) Declare(ctx context.Context, name string) error {
	return app.dao.
		Stream(ctx, "vars:commands").
		Append(
			commands.DeclareCommand{Name: name},
		)
}

func (app *App) Add(ctx context.Context, name string, v int64) error {
	return app.dao.Stream(ctx, "vars:commands-"+name).
		Append(
			commands.AddCommand{
				Name:   name,
				Number: v,
			},
		)
}

func (app *App) Sub(ctx context.Context, name string, v int64) error {
	return app.dao.Stream(ctx, "vars:commands-"+name).
		Append(
			commands.SubCommand{
				Name:   name,
				Number: abs(v),
			},
		)
}

func (app *App) GetValue(ctx context.Context, name string) (int64, error) {
	view := views.VariableTotal{}
	err := app.dao.Project(ctx, "vars-"+name, &view)
	if err != nil {
		return 0, err
	}
	return view.Total, nil
}

func abs(v int64) int64 {
	if v >= 0 {
		return v
	}
	return -1 * v
}
