#!/usr/bin/env python3
"""kafa consume"""
import time
import json

from kafka import KafkaConsumer, TopicPartition


class KafkaManager:
    """add manager"""
    attrib = ""
    previous_command = ""

    def __init__(self):
        """init"""

    def consume(self):
        """setup consumer"""
        consumer = KafkaConsumer(
            bootstrap_servers=['localhost:9093'],
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        partition = TopicPartition('V20_writerStatus', 0)
        consumer.assign([partition])
        consumer.seek_to_end()
        last_offset = consumer.position(partition)
        print(last_offset)
        consumer.seek(partition=partition, offset=last_offset)

        for message in consumer:
            # message value and key are raw bytes -- decode if necessary!
            # e.g., for unicode: `message.value.decode('utf-8')`
            print("%s:%d:%d: key=%s value=" % (message.topic, message.partition,
                                               message.offset, message.key
                                               ))
            val = message.value
           

def main():
    """main"""
    manager = KafkaManager()
    manager.consume()


if __name__ == "__main__":
    main()
