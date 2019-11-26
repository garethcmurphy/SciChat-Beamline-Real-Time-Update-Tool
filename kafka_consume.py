#!/usr/bin/env python3
"""kafa consume"""
from json import loads
from kafka import KafkaConsumer

def main():
    """setup consumer"""
    consumer = KafkaConsumer(
    'v20_filerwriter_Cmd',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: loads(x.decode('utf-8')))


if __name__ == "__main__":
    main()