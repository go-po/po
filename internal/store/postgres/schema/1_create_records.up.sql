-- initial schema for the messages
CREATE TABLE IF NOT EXISTS po_msgs
(

    created      timestamp WITH TIME ZONE DEFAULT NOW() NOT NULL,
    updated      timestamp WITH TIME ZONE DEFAULT NOW() NOT NULL,
    stream       VARCHAR                                NOT NULL,
    no           BIGINT                   DEFAULT 0     NOT NULL,
    grp          varchar                                NOT NULL, -- group in Go
    grp_no       BIGINT                   DEFAULT 0     NOT NULL,
    content_type varchar                                NOT NULL,
    data         bytea                                  NOT NULL
);

CREATE INDEX IF NOT EXISTS po_msgs_stream_index ON po_msgs (stream);
CREATE INDEX IF NOT EXISTS po_msgs_grp_index ON po_msgs (grp);
CREATE UNIQUE INDEX IF NOT EXISTS po_msgs_stream_number_uindex ON po_msgs (stream, no);
CREATE UNIQUE INDEX IF NOT EXISTS po_msgs_grp_grp_number_uindex ON po_msgs (grp, grp_no);

CREATE TABLE IF NOT EXISTS po_msg_index
(
    stream VARCHAR          NOT NULL,
    next   bigint default 0 not null
);

CREATE UNIQUE INDEX IF NOT EXISTS po_msg_index_stream_uindex ON po_msg_index (stream);

