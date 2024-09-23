#!/usr/bin/env python

import logging
import argparse
import os

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from user import User

def read_config(filename):
  # reads the client configuration from client.properties
  # and returns it as a key-value map
  config = {}
  with open(filename) as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config

def consumer_config(args):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    params = read_config('client.properties')

    params['group.id'] = args.group
    params['auto.offset.reset'] = 'earliest'
    
    return params

def sr_config(args): 
    params = read_config('sr_client.properties')
    return params

def dict_to_user(obj, ctx):
    """
    Converts object literal(dict) to a User instance.

    Args:
        obj (dict): Object literal(dict)

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    """

    if obj is None:
        return None

    return User(name=obj['name'],
                favorite_number=obj['favorite_number'],
                favorite_color=obj['favorite_color'])


def main(args):
    topic = args.topic
    path = os.path.realpath(os.path.dirname(__file__))
    with open(f"{path}/avro/user-v1.avsc") as f:
        schema_str = f.read()


    schema_registry_conf = sr_config(args)
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_deserializer = AvroDeserializer(schema_registry_client,
                                         schema_str,
                                         dict_to_user)

    consumer_conf = consumer_config(args)

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            user = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            if user is not None:
                print("User record {}: name: {}\n"
                      "\tfavorite_number: {}\n"
                      "\tfavorite_color: {}\n"
                      .format(msg.key(), user.name,
                              user.favorite_number,
                              user.favorite_color))
        except KeyboardInterrupt:
            break

    consumer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Consumer example")
    parser.add_argument('-t', dest="topic", default="user",
                        help="Topic name")
    parser.add_argument('-g', dest="group", default="consumer-group",
                        help="Consumer group")

    main(parser.parse_args())
