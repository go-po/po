package app

import (
	"context"
	"github.com/go-po/po"
	"github.com/go-po/po/examples/highway/domain"
	"github.com/go-po/po/internal/broker/rabbitmq"
	"github.com/go-po/po/internal/store/postgres"
	"math/rand"
)

func NewApp(databaseUrl, rabbitUrl string) (*App, error) {
	db, err := postgres.NewFromUrl(databaseUrl)
	if err != nil {
		return nil, err
	}
	broker, err := rabbitmq.New(rabbitUrl, "highway", db)
	if err != nil {
		return nil, err
	}

	return &App{
		po:      po.New(db, broker),
		counter: &domain.CarCounter{},
		monitor: &domain.SpeedMonitor{},
	}, nil

}

type App struct {
	po      *po.Po
	counter *domain.CarCounter
	monitor *domain.SpeedMonitor
}

func (app *App) Start(ctx context.Context, name string, cars int) error {

	err := app.po.Subscribe(ctx, "car-counter", "highways", app.counter)
	if err != nil {
		return err
	}
	err = app.po.Subscribe(ctx, "speed-monitor", "highways", app.monitor)
	if err != nil {
		return err
	}

	for i := 0; i < cars; i++ {
		err = app.po.Stream(context.Background(), "highways-"+name).
			Append(domain.Car{Speed: float64(rand.Int31n(100))})
		if err != nil {
			return err
		}
	}

	return nil
}
