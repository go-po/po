package main

import (
	"context"
	"github.com/kyuff/po"
	"github.com/kyuff/po/examples/teller/commands"
	"github.com/kyuff/po/internal/broker/channels"
	"github.com/kyuff/po/internal/store/inmemory"
	"log"
)

func main() {

	ctx := context.Background()
	store := po.New(inmemory.New(), channels.New())

	err := store.Subscribe(ctx, "command handler", "vars:commands", commands.NewCommandSubscriber())
	if err != nil {
		log.Fatalf("failed subscribing: %s", err)
	}

	err = declareVars(ctx, store)
	if err != nil {
		log.Fatalf("failed declaring a and b: %s", err)
	}

	cmds := store.Stream(ctx, "vars:commands")
	err = cmds.Load()
	if err != nil {
		log.Fatalf("failed loading commands stream: %s", err)
	}

	count := commands.CommandCount{Count: 0}
	err = cmds.Project(&count)
	if err != nil {
		log.Fatalf("failed projecting count: %s", err)
	}

	log.Printf("ID: %s Size: %d", cmds.ID, count.Count)

	names := commands.VariableNames{}
	err = cmds.Project(&names)
	for _, name := range names.Names {
		log.Printf("\t%s", name)
	}

	if err != nil {
		log.Fatalf("could not project: %s", err)
	}
}

func declareVars(ctx context.Context, store *po.Po) error {
	return store.Stream(ctx, "vars:commands").Append(
		commands.DeclareVar{Name: "a"},
		commands.DeclareVar{Name: "b"},
	)
}
