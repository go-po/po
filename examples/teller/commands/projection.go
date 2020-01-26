package commands

import (
	"context"
	"github.com/kyuff/po"
)

type CommandCount struct {
	Count int64
}

func (command *CommandCount) Handle(ctx context.Context, message po.Message) error {
	command.Count = command.Count + 1
	return nil
}

type VariableNames struct {
	Names []string
}

func (variable *VariableNames) Handle(ctx context.Context, message po.Message) error {
	declared, ok := message.Data.(DeclareVar)
	if !ok {
		return nil
	}
	variable.Names = append(variable.Names, declared.Name)
	return nil
}
