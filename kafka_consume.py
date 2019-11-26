#!/usr/bin/env python3
"""kafa consume"""
from json import loads
from kafka import KafkaConsumer


def main():
    """setup consumer"""
    consumer = KafkaConsumer(
        'V20_writerCommand',
        bootstrap_servers=['localhost:9093'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: loads(x.decode('utf-8')))

    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                             message.offset, message.key,
                                             message.value))


if __name__ == "__main__":
    main()
