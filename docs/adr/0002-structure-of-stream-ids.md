# 2. Structure of Stream IDs

Date: 2020-02-25

## Status

Accepted

Reference [1. PO is the Data Access Layer](0001-po-is-the-data-access-layer.md)

## Context

As PO is the Data Access layer, it is important how to access data.

## Decision

Data is accessed as a Key/Value store. The key is the Stream ID, which will have an internal structure
following this BNF:

````text
<stream id>     ::= <group name> [ - <id> ]
<group name>    ::= regexp([[^-].]+)
<id>            ::= <text>
<text>          ::= regexp(.+)
````

*Examples*
````text
users
users:commands
users-920bb5bb-0ed3-48dc-87dc-31479eb314b0
users:commands-920bb5bb-0ed3-48dc-87dc-31479eb314b0
````

The two first stream ids is for two separate groups: users and users:commands
The last two is for the same groups, but specific for an entity with id `920bb5bb-0ed3-48dc-87dc-31479eb314b0`.

## Consequences

All streams must have an ID and can only be referenced by it's id.
It is possible to reference groups of streams by the group name.
