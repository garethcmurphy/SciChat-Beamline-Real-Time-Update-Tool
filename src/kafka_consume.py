#!/usr/bin/env python3
"""kafa consume"""
import json
from kafka import KafkaConsumer, TopicPartition
from scicat_bot import ScicatBot


def main():
    """setup consumer"""
    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9093'],
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    partition = TopicPartition('V20_writerCommand', 0)
    consumer.assign([partition])
    consumer.seek_to_end()
    offsets = [consumer.position(tp) for tp in partition]
    print(offsets)
    consumer.seek(partition=partition, offset=1479)

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
                if "file_attributes" in val:
                    if "file_name" in val["file_attributes"]:
                        attrib = val["file_attributes"]
                        print(attrib["file_name"])
                        bot = ScicatBot()
                        bot.login()
                        proposal_id = "QHK123"
                        room_alias = "#"+proposal_id+":ess"
                        room_id = bot.get_room_id(room_alias)
                        filename = attrib["file_name"]
                        bot.post(room_id, filename)
                        filename = "im.png"
                        bot.upload_image(filename)
                        bot.post_image(room_id)


if __name__ == "__main__":
    main()
