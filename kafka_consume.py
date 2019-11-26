#!/usr/bin/env python3
"""kafa consume"""
from json import loads
from kafka import KafkaConsumer


def main():
    """setup consumer"""
    consumer = KafkaConsumer(
        'V20_writerCommand',
        bootstrap_servers=['localhost:9093'],
        enable_auto_commit=False,
        value_deserializer=lambda x: loads(x.decode('utf-8')))
    consumer.seek(partition=None, offset=1210)

    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print("%s:%d:%d: key=%s value=" % (message.topic, message.partition,
                                           message.offset, message.key
                                           ))


if __name__ == "__main__":
    main()
