package streams

import (
	"database/sql/driver"
	"fmt"
	"strings"
)

type Id struct {
	Group  string
	Entity string
}

func (id Id) Value() (driver.Value, error) {
	return id.String(), nil
}

func ParseId(format string, args ...interface{}) Id {
	streamId := fmt.Sprintf(format, args...)
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

// convenience method to construct an entity id from another
func (id Id) WithEntity(format string, args ...interface{}) Id {
	return Id{
		Group:  id.Group,
		Entity: fmt.Sprintf(format, args...),
	}
}
