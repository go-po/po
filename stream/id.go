package stream

import (
	"database/sql/driver"
	"strings"
)

type Id struct {
	Group  string
	Entity string
}

func (id Id) Value() (driver.Value, error) {
	return id.String(), nil
}

func ParseId(streamId string) Id {
	i := strings.Index(streamId, "-")
	if i < 0 {
		return Id{
			Group:  strings.TrimSpace(streamId),
			Entity: "",
		}
	}
	return Id{
		Group:  strings.TrimSpace(streamId[0:i]),
		Entity: strings.TrimSpace(streamId[i+1:]),
	}
}

func (id Id) String() string {
	if id.HasEntity() {
		return id.Group + "-" + id.Entity
	}
	return id.Group
}

func (id Id) HasEntity() bool {
	return strings.TrimSpace(id.Entity) != ""
}
