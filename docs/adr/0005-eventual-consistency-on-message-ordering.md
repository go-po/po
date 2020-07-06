# 5. Eventual Consistency on Message Ordering

Date: 2020-06-17

## Status

Accepted

Replaes [3. Message Ordering](0003-message-ordering.md)

## Context

The assignment of group numbers in PO made all messages go through a queue before being 
available on the read side. This queue gave eventual consistency and a dependency on a transport
layer like RabbitMQ that could fail.

This raises a concern about stability of the system. 

## Decision

Remove this feature of PO where messages are ordered in a stream and a group. Instead
downgrade it to only support message ordering within a stream and a global number between
all messages.

## Consequences

It means a subscriber cannot rely on message ordering to verify having received all messages.
Instead they must rely on idempotence and at-least-once message delivery. This seems like a fair
trade-off in a distributed system. 
