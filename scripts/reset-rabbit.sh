#!/bin/sh

IFS=$'\n' 
for q in $(docker exec po_mq rabbitmqctl -qs list_queues name); do
  docker exec po_mq rabbitmqctl delete_queue "${q}"
done;
