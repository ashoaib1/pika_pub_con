#!/usr/bin/python

import pika
import logging
from datetime import datetime
from socket import getfqdn
import os
from time import sleep

logging.basicConfig(level=logging.INFO)

# Create connection and open channel
credentials = pika.PlainCredentials(username='rmq_test', password='rmq_test')
parameters =  pika.ConnectionParameters(host='192.168.101.3', port=5672, virtual_host='/', credentials=credentials)
#parameters =  pika.ConnectionParameters('192.168.101.3', credentials=credentials) #rmqnode2
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
try:
	while(True):
		message = "timestamp={}, host={}, script={}, pid={}".format(datetime.now(), host, local_script, pid)
		print("Publishing on exchange {} with routing_key {}: message [{}]"
			.format(exchange_name, routing_key, message))
		channel.basic_publish(exchange_name, routing_key, message)
		sleep(0.5)
except KeyboardInterrupt:
	# Close connection and channel
	connection.close()

