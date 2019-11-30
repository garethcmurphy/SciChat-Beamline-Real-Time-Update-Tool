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
    files = {}
    service_id = ""
    job_id = ""
    log_all = False

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
        consumer.seek(partition=partition, offset=13565573)

        for message in consumer:
            # message value and key are raw bytes -- decode if necessary!
            # e.g., for unicode: `message.value.decode('utf-8')`
            # print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
            #                                   message.offset, message.key,
            #                                   message.value
            #                                   ))
            val = message.value
            # print(message.offset)
            if "timestamp" in val:
                # print(val["timestamp"])
                dt_object = datetime.fromtimestamp(int(val["timestamp"])/1000)
                #print("dt_object =", dt_object)
            if "type" in val:
                # print(val["type"])
                type1 = val["type"]
                if type1 == "stream_master_status":
                    pass
                elif type1 == "filewriter_status_master":
                    # print(message.offset)
                    files = val["files"]
                    if self.log_all == True:
                        print(files)
                    # print(val)
                    files_dict = ""
                    key_array = list(files.values())
                    files_dict = key_array.pop()
                    # check servcie id
                    # print("gmx",val["service_id"])
                    # print(self.service_id, val["service_id"])
                    if val["service_id"] == self.service_id:
                        if self.job_id in files:
                            print("master")
                            self.file_name = files[self.job_id]["filename"]
                    self.files = files
                    files = ""
                    files_dict = ""
                    # print(self.file_name)
                else:
                    print(type1)
                    if "code" in val:
                        code = val["code"]
                        print(code)
                        if code == "START":
                            print(val["service_id"])
                            self.service_id = val["service_id"]
                            self.job_id = val["job_id"]
                            print("job id", self.job_id)
                            self.log_all = True
                        elif code == "CLOSE":
                            print(message.offset)
                            # print(self.files)
                            print("closing")
                            print(self.file_name)
                            print(val)
                            self.log_all = False
                    # exit(0)


def main():
    """main"""
    manager = KafkaManager()
    manager.consume()


if __name__ == "__main__":
    main()
