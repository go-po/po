-- initial schema for the messages
CREATE TABLE IF NOT EXISTS po_msgs
(

    created      timestamp WITH TIME ZONE DEFAULT NOW() NOT NULL,
    updated      timestamp WITH TIME ZONE DEFAULT NOW() NOT NULL,
    stream       VARCHAR                                NOT NULL,
    no           BIGINT                   DEFAULT 0     NOT NULL,
    grp          varchar                                NOT NULL, -- group in Go
    grp_no       BIGINT                                 NULL,
    content_type varchar                                NOT NULL,
    data         bytea                                  NOT NULL
);

comment on table po_msgs is 'contains messages';

CREATE INDEX IF NOT EXISTS po_msgs_stream_index ON po_msgs (stream);
CREATE INDEX IF NOT EXISTS po_msgs_grp_index ON po_msgs (grp);
CREATE UNIQUE INDEX IF NOT EXISTS po_msgs_stream_number_uindex ON po_msgs (stream, no);
CREATE UNIQUE INDEX IF NOT EXISTS po_msgs_grp_grp_number_uindex ON po_msgs (grp, grp_no);

CREATE TABLE IF NOT EXISTS po_msg_index
(
    updated timestamp with time zone default NOW() not null,
    created timestamp with time zone default NOW() not null,
    stream  VARCHAR                                NOT NULL,
    next    bigint                   default 0     not null
);

comment on table po_msg_index is 'contains the next number assigned to a stream';

CREATE UNIQUE INDEX IF NOT EXISTS po_msg_index_stream_uindex ON po_msg_index (stream);

CREATE TABLE IF NOT EXISTS po_pos
(
    updated      timestamp with time zone default NOW() NOT NULL,
    created      timestamp with time zone default NOW() NOT NULL,
    stream       varchar                                NOT NULL,
    listener     varchar                                NOT NULL,
    no           bigint                                 NOT NULL,
    data         bytea                                  NOT NULL,
    content_type varchar                                NOT NULL
);

comment on table po_pos is 'position of a stream listener';

CREATE UNIQUE INDEX IF NOT EXISTS po_pos_stream_listener_uindex
    on po_pos (stream, listener);

