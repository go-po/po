package main

import (
	"context"
	"github.com/go-po/po/examples/highway/app"
	"log"
	"strconv"
	"sync"
	"time"
)

const (
	databaseUrl = "postgres://po:po@localhost:5431/po?sslmode=disable"
	uri         = "amqp://po:po@localhost:5671/"
	cardsPerApp = 10
	apps        = 2
)

// runs multiple workers, that each send messages and reads them again
func main() {

	rootCtx := context.Background()

	var wg = &sync.WaitGroup{}
	for i := 0; i < apps; i++ {
		wg.Add(1)
		go startWorker(rootCtx, wg, i)
	}
	wg.Wait()

	time.Sleep(5 * time.Second)
}

func startWorker(ctx context.Context, wg *sync.WaitGroup, n int) {
	defer wg.Done()
	app, err := app.NewApp(databaseUrl, uri)
	if err != nil {
		log.Fatalf("new app: %s", err)
		return
	}

	err = app.Start(ctx, strconv.Itoa(n), cardsPerApp)
	if err != nil {
		log.Fatalf("start: %s", err)
		return
	}
}
