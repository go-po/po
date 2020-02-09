package po

import "strings"

type StreamId struct {
	Group  string
	Entity string
}

func ParseStreamId(streamId string) StreamId {
	i := strings.Index(streamId, "-")
	if i < 0 {
		return StreamId{
			Group:  strings.TrimSpace(streamId),
			Entity: "",
		}
	}
	return StreamId{
		Group:  strings.TrimSpace(streamId[0:i]),
		Entity: strings.TrimSpace(streamId[i+1:]),
	}
}

func (id StreamId) String() string {
	if id.HasEntity() {
		return id.Group + "-" + id.Entity
	}
	return id.Group
}

func (id StreamId) HasEntity() bool {
	return strings.TrimSpace(id.Entity) != ""
}
