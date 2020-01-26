package main

import (
	"context"
	"github.com/kyuff/po"
	"github.com/kyuff/po/examples/teller/commands"
	"github.com/kyuff/po/examples/teller/views"
	"github.com/kyuff/po/internal/broker/channels"
	"github.com/kyuff/po/internal/store/inmemory"
	"log"
	"time"
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
			commands.AddCommand{Name: "a", Number: 10},
			commands.DeclareCommand{Name: "b"},
			commands.AddCommand{Name: "a", Number: 15},
			commands.AddCommand{Name: "b", Number: 7},
			commands.SubCommand{Name: "b", Number: 2},
			commands.SubCommand{Name: "a", Number: 13},
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

	// since views are eventually consistent, give it time to become that.
	time.Sleep(1000 * time.Millisecond)

	totalsA := views.VariableTotal{Total: 0}
	err = store.Project(ctx, "vars-a", &totalsA)
	if err != nil {
		log.Fatal("failed to project variable a")
	}
	log.Printf("Total of a: %d", totalsA.Total)
}
