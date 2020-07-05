-- initial schema for the messages
CREATE TABLE IF NOT EXISTS po_messages
(
    id             bigserial                              NOT NULL,
    created        timestamp WITH TIME ZONE DEFAULT NOW() NOT NULL,
    stream         VARCHAR                                NOT NULL,
    no             BIGINT                   DEFAULT 0     NOT NULL,
    grp            varchar                                NOT NULL, -- group in Go
    content_type   varchar                                NOT NULL,
    data           bytea                                  NOT NULL,
    correlation_id varchar                                NULL,
    PRIMARY KEY (id)
);

comment on table po_messages is 'contains messages';

CREATE INDEX IF NOT EXISTS po_messages_stream_index ON po_messages (stream);
CREATE INDEX IF NOT EXISTS po_messages_grp_index ON po_messages (grp);
CREATE UNIQUE INDEX IF NOT EXISTS po_messages_stream_number_uindex ON po_messages (stream, no);

CREATE TABLE IF NOT EXISTS po_subscriptions
(
    created       timestamp with time zone default NOW() NOT NULL,
    updated       timestamp with time zone default NOW() NOT NULL,
    stream        varchar                                NOT NULL,
    subscriber_id varchar                                NOT NULL,
    no            bigint                                 NOT NULL
);

comment on table po_subscriptions is 'position of a stream subscribers';

CREATE UNIQUE INDEX IF NOT EXISTS po_subscriptions_stream_subscriber_id_uindex
    on po_subscriptions (stream, subscriber_id);

CREATE TABLE IF NOT EXISTS po_snapshots
(
    created      timestamp with time zone default NOW() NOT NULL,
    updated      timestamp with time zone default NOW() NOT NULL,
    stream       varchar                                NOT NULL,
    snapshot_id  varchar                                NOT NULL,
    no           bigint                   DEFAULT -1    NOT NULL,
    data         bytea                                  NOT NULL,
    content_type varchar                                NOT NULL
);

comment on table po_snapshots is 'snapshot position and data';

CREATE UNIQUE INDEX IF NOT EXISTS po_snapshots_stream_snapshot_id_uindex
    on po_snapshots (stream, snapshot_id);
