# PO

Library to create Event Sourced Applications using Message Streams as the persistence layer.

## Goals

* Make it easy to use Message Streams for Persistence
* Be a lightweight toolkit that plays well with others

## Non-Goals

* Handle async communication between multiple services

# Features

1. Writing Messages to a stream
1. Projecting Messages onto a Handler
1. Subscribing to message streams
    1. by stream
    1. by group
1. Grouping of message streams
1. Message ordering
    1. by stream
    1. by group

## Planned

1. Snapshots
    1. Projections
    1. Subscriptions
