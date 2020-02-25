# 3. Message Ordering

Date: 2020-02-25

## Status

Accepted

## Context

When working with message streams, it is important which ordering are used.
Often it is practical to know if a message from a given group is coming before
or after another message. This holds especially true if trying to receive messages
in an idempotent way.

## Example

Two streams with the following messages from a Book Store application.

*users-joe*

1. Registered
2. OrderedBook 

*users-jane*

1. Registered
2. AddressChanged

If subscribing to the `users` group stream, the entity streams number is not enough to handle order.

## Decision

PO will support two layers of Message Ordering.

* Consequential Ordering of entity streams.
* Eventual consequential Ordering of all messages within a group.

## Consequences

In order to provide the group number, all messages within a group needs to be queued up
and assigned a group number. This means the group number is written at a later state.

Subscribers will be eventually consistent as they will rely on the group number. 
