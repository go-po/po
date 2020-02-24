package main

import (
	"context"
	"github.com/go-po/po/examples/highway/app"
	"github.com/go-po/po/examples/highway/domain"
	"log"
	"strconv"
	"sync"
	"time"
)

const (
	databaseUrl = "postgres://po:po@localhost:5431/po?sslmode=disable"
	uri         = "amqp://po:po@localhost:5671/"
	cardsPerApp = 50
	apps        = 20
)

// runs multiple workers, that each send messages and reads them again
func main() {

	start := time.Now()
	rootCtx := context.Background()
	counter := &domain.CarCounter{}
	speed := domain.NewSpeedMonitor()

	for i := 0; i < apps; i++ {
		name := "app-" + strconv.Itoa(i)
		app, err := app.NewApp(databaseUrl, uri, name, counter, speed)
		if err != nil {
			log.Fatalf("failed creating app: %s", err)
		}
		go startWorker(rootCtx, app)
	}
	var wg = &sync.WaitGroup{}
	expectedCount := apps * cardsPerApp
	ticker := time.NewTicker(time.Second)
	wg.Add(1)
	go func() {
		for range ticker.C {
			status := counter.Count()
			if status >= expectedCount {
				wg.Done()
				ticker.Stop()
			}
			log.Printf("Count: %d (%.2f/sec)", status, float64(status)/time.Since(start).Seconds())
		}
	}()
	wg.Wait()
	log.Printf("")
	log.Printf("-------")
	log.Printf("Total time: %s", time.Since(start))
	log.Printf("Messages %.2f/sec", float64(expectedCount)/time.Since(start).Seconds())
	speed.PrintStats()
}

func startWorker(ctx context.Context, app *app.App) {
	err := app.Start(ctx, cardsPerApp)
	if err != nil {
		log.Fatalf("start: %s", err)
		return
	}
}
