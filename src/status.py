#!/usr/bin/env python3
"""kafa consume"""
import time
import json
from datetime import datetime

from kafka import KafkaConsumer, TopicPartition


class KafkaManager:
    """add manager"""
    attrib = ""
    previous_command = ""
    file_name = ""

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
        consumer.seek(partition=partition, offset=last_offset-1000000)

        for message in consumer:
            # message value and key are raw bytes -- decode if necessary!
            # e.g., for unicode: `message.value.decode('utf-8')`
            #print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
            #                                   message.offset, message.key,
            #                                   message.value
            #                                   ))
            val = message.value
            # print(message.offset)
            if "timestamp" in val:
                #print(val["timestamp"])
                dt_object = datetime.fromtimestamp(int(val["timestamp"])/1000)
                #print("dt_object =", dt_object)
            if "type" in val:
                #print(val["type"])
                type1 = val["type"]
                if type1 == "stream_master_status":
                    pass
                elif type1 == "filewriter_status_master":
                    files = val["files"]
                    key = ""
                    key = list(files.values())[0]
                    self.file_name = key["filename"]
                    files = ""
                    key = ""
                    #print(self.file_name)
                else:
                    print(type1)
                    if "code" in val:
                        code = val["code"]
                        print(code)
                        if code == "START":
                            pass
                        elif code == "CLOSE":
                            print("closing")
                            print(self.file_name)
                            print(val)
                    #exit(0)

def main():
    """main"""
    manager = KafkaManager()
    manager.consume()


if __name__ == "__main__":
    main()
