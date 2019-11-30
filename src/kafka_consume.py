#!/usr/bin/env python3
"""kafa consume"""
import time
import json

import h5py
from kafka import KafkaConsumer, TopicPartition
from scicat_bot import ScicatBot


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
        partition = TopicPartition('V20_writerCommand', 0)
        consumer.assign([partition])
        consumer.seek_to_end()
        last_offset = consumer.position(partition)
        print(last_offset)
        consumer.seek(partition=partition, offset=last_offset - 8)

        for message in consumer:
            # message value and key are raw bytes -- decode if necessary!
            # e.g., for unicode: `message.value.decode('utf-8')`
            print("%s:%d:%d: key=%s value=" % (message.topic, message.partition,
                                               message.offset, message.key
                                               ))
            val = message.value
            if "cmd" in val:
                cmd = val["cmd"]
                print(cmd)
                if cmd == "FileWriter_new":
                    self.previous_command = cmd
                    if "file_attributes" in val:
                        if "file_name" in val["file_attributes"]:
                            self.attrib = val["file_attributes"]
                            print(self.attrib["file_name"])
                elif cmd == "FileWriter_stop":
                    if self.previous_command == "FileWriter_new":
                        self.previous_command = cmd
                        time.sleep(5)
                        bot = ScicatBot()
                        bot.login()
                        proposal_id = "QHK123"
                        proposal_id = "YC7SZ5"
                        room_alias = "#"+proposal_id+":ess"
                        room_id = bot.get_room_id(room_alias)
                        filename = self.attrib["file_name"]
                        bot.post(room_id, filename)
                        image_name = "im.png"
                        try:
                            with h5py.File(filename, "r", libver="latest", swmr=True) as file:
                                print(file["/entry/title"])
                            bot.upload_image(image_name)
                            bot.post_image(room_id)
                        except OSError as err:
                            print("OS error: {0}".format(err))
                            print("Error reading hdf5 file")


def main():
    """main"""
    manager = KafkaManager()
    manager.consume()


if __name__ == "__main__":
    main()
