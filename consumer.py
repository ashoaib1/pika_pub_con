#!/usr/bin/python

import pika
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)


def process_message(channel, method_frame, header_frame, message):
    print("Received at {}: method_frame={}, message=[{}]".format(datetime.now(), method_frame, message))
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)

# Create connection and open channel
credentials = pika.PlainCredentials(username='rmq_test', password='rmq_test')
parameters =  pika.ConnectionParameters(host='192.168.101.2', port=5672, virtual_host='/', credentials=credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

# Declare all resources
exchange_name = 'test_exchange'
queue_name = 'test_queue'
routing_key = 'test_routing_key'
channel.exchange_declare(exchange=exchange_name, exchange_type="direct", passive=False, durable=True, auto_delete=False)
channel.queue_declare(queue=queue_name, durable=True)
channel.queue_bind(queue=queue_name, exchange=exchange_name, routing_key=routing_key)
channel.basic_qos(prefetch_count=10)

# Assign callback function to consume messages
channel.basic_consume(consumer_callback=process_message, queue=queue_name)

try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()

connection.close()