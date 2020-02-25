# 2. PO is the Data Access Layer

Date: 2020-02-25

## Status

Accepted

## Context

Many applications needs to store data. Be it a monolith or one of many microservices, there is
in most cases a data access layer of some sort.

## Decision

The PO projects aim is to provide the 
[Data Access layer](https://en.wikipedia.org/wiki/Multitier_architecture#Three-tier_architecture)
of a three tier architecture by providing building blocks for
[CQRS](https://martinfowler.com/bliki/CQRS.html) and 
[Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html). 

## Consequences

The PO project is not meant to replace communication between different applications. 
