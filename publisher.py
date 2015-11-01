#!/usr/bin/python -u

import pika
import logging
from datetime import datetime
from socket import getfqdn
import os
from time import sleep

logging.basicConfig(level=logging.INFO)

# Create connection and open channel
credentials = pika.PlainCredentials(username='rmq_test', password='rmq_test')
rmq_node = 'rmqnode2'
parameters =  pika.ConnectionParameters(host=rmq_node, port=5672, virtual_host='/', credentials=credentials)
print "Trying to connect with RabbitMQ Node {}".format(rmq_node)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

# Declare all resources
exchange_name = 'test_exchange'
queue_name = 'test_queue'
routing_key = 'test_routing_key'
channel.exchange_declare(exchange=exchange_name, exchange_type="direct", passive=False, durable=True, auto_delete=False)
channel.queue_declare(queue=queue_name, durable=True)
channel.queue_bind(queue=queue_name, exchange=exchange_name, routing_key=routing_key)

# Start publishing messages
host = getfqdn()
local_script = os.path.basename(__file__)
pid = os.getpid()
msg_num = 1
try:
	while(True):
		message = "timestamp={}, msg_num={}, host={}, script={}, pid={}".format(datetime.now(), msg_num, host, local_script, pid)
		print("Publishing on exchange {} with routing_key {}: message [{}]"
			.format(exchange_name, routing_key, message))
		channel.basic_publish(exchange_name, routing_key, message)
		sleep(0.5)
		msg_num = msg_num + 1
except KeyboardInterrupt:
	# Close connection and channel
	print "Closing publisher cause received keyboardInterrupt"
	connection.close()

