package main

import (
	"context"
	"github.com/go-po/po"
	"github.com/go-po/po/examples/teller/api"
	"github.com/go-po/po/examples/teller/commands"
	"github.com/go-po/po/examples/teller/views"
	"github.com/go-po/po/internal/broker/channels"
	"github.com/go-po/po/internal/store/inmemory"
	"log"
	"net/http"
)

func main() {

	ctx := context.Background()
	store := po.New(inmemory.New(), channels.New())

	err := store.Subscribe(ctx, "command handler", "vars:commands", commands.NewCommandSubscriber(store))
	if err != nil {
		log.Fatalf("failed subscribing: %s", err)
	}

	err = store.
		Stream(ctx, "vars:commands").
		Append(
			commands.DeclareCommand{Name: "a"},
			commands.DeclareCommand{Name: "b"},
			commands.AddCommand{Name: "a", Number: 10},
			commands.AddCommand{Name: "a", Number: 15},
			commands.AddCommand{Name: "b", Number: 7},
			commands.SubCommand{Name: "b", Number: 2},
			commands.SubCommand{Name: "a", Number: 13},
			commands.AddCommand{Name: "a", Number: 12},
		)
	if err != nil {
		log.Fatalf("failed declaring a and b: %s", err)
	}

	cmds := store.Stream(ctx, "vars:commands")
	err = cmds.Load()
	if err != nil {
		log.Fatalf("failed loading commands stream: %s", err)
	}

	count := views.CommandCount{Count: 0}
	err = cmds.Project(&count)
	if err != nil {
		log.Fatalf("failed projecting count: %s", err)
	}

	log.Printf("ID: %s Size: %d", cmds.ID, count.Count)

	names := views.VariableNames{}
	err = cmds.Project(&names)
	for _, name := range names.Names {
		log.Printf("\t%s", name)
	}
	if err != nil {
		log.Fatalf("could not project: %s", err)
	}

	totalsA := views.VariableTotal{Total: 0}
	err = store.Project(ctx, "vars-a", &totalsA)
	if err != nil {
		log.Fatal("failed to project variable a")
	}
	log.Printf("Total of a: %d", totalsA.Total)

	err = http.ListenAndServe(":8000", api.Root(nil))
	log.Printf("server topped: %s", err)

}
