package po

import "strings"

type StreamId struct {
	Type   string
	Entity string
}

func ParseStreamId(streamId string) StreamId {
	i := strings.Index(streamId, "-")
	if i < 0 {
		return StreamId{
			Type:   strings.TrimSpace(streamId),
			Entity: "",
		}
	}
	return StreamId{
		Type:   strings.TrimSpace(streamId[0:i]),
		Entity: strings.TrimSpace(streamId[i+1:]),
	}
}

func (id StreamId) String() string {
	if id.hasEntity() {
		return id.Type + "-" + id.Entity
	}
	return id.Type
}

func (id StreamId) hasEntity() bool {
	return strings.TrimSpace(id.Entity) != ""
}
