#!/usr/bin/env bash

rabbitmq-server &
export RABBITMQ_PID=$!
echo "Waiting for RabbitMQ to start..."
until rabbitmqctl status; do
	sleep 1
done

rabbitmqctl add_user admin admin
rabbitmqctl set_user_tags admin administrator
rabbitmqctl set_permissions -p / admin ".*" ".*" ".*"

rabbitmqctl set_user_tags guest monitoring
rabbitmqctl set_permissions -p / guest ".*" ".*" ".*"

# retry qq

# streams - max age 5 days, max segment size 50MB

# set cc to priority 2, this will catch any mirrored queues and provide the policy correctly.

# retry classic queues should not have an expires header

# dlq queues should have a message-ttl and no expires header

wait
