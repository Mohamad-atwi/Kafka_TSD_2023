#!/usr/bin/env python

import sys
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer, Consumer, OFFSET_BEGINNING

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    consumer_config = dict(config_parser['consumer'])

    # Create Producer instance
    producer = Producer(config)


    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    # Create Consumer instance
    consumer = Consumer(consumer_config)

    # Set up a callback to handle the '--reset' flag for consumer
    def reset_offset(consumer, partitions):
        consumer.assign(partitions)
        for p in partitions:
            p.offset = OFFSET_BEGINNING

    # Optional per-message consume callback for consumer (triggered by poll())
    def consume_callback(msg):
        if msg.error():
            print("ERROR: %s".format(msg.error()))
        else:
            print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    # Set consumer configuration
    topic = "message"  # Changed topic name to "message"
    consumer.subscribe([topic], on_assign=reset_offset)

    # Produce data by taking input from the user.
    print("Type 'end' to exit.")
    while True:
        user_input = input("Enter a message or 'end': ")  
        if user_input.lower() == 'end':
            break  # Break out of the loop if user input is "end"
        user_id = input("Enter a user ID: ")

        producer.produce(topic, value=user_input, key=user_id, callback=delivery_callback)

        # Block until the message is sent.
        producer.poll(0)
        producer.flush()

    # Consume data from Kafka and print messages.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting...")
            else:
                consume_callback(msg)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
