package views

import (
	"context"
	"github.com/go-po/po/examples/teller/commands"
	"github.com/go-po/po/examples/teller/events"
	"github.com/go-po/po/internal/stream"
)

type CommandCount struct {
	Count int64
}

func (view *CommandCount) Handle(ctx context.Context, message stream.Message) error {
	view.Count = view.Count + 1
	return nil
}

type VariableNames struct {
	Names []string
}

func (view *VariableNames) Handle(ctx context.Context, message stream.Message) error {
	declared, ok := message.Data.(commands.DeclareCommand)
	if !ok {
		return nil
	}
	view.Names = append(view.Names, declared.Name)
	return nil
}

type VariableTotal struct {
	Total int64
}

func (view *VariableTotal) Handle(ctx context.Context, msg stream.Message) error {
	switch event := msg.Data.(type) {
	case events.SubtractedEvent:
		view.Total = view.Total - event.Value
	case events.AddedEvent:
		view.Total = view.Total + event.Value
	}
	return nil
}
