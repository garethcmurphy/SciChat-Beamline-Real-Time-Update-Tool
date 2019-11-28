#!/usr/bin/env python3
"""scicatbot"""
import os

import urllib
import json
import requests

import visens
import search


class ScicatBot():
    """scicatbot"""
    base_url = "https://scitest.esss.lu.se/_matrix"
    url = base_url + "/client/r0"
    media_url = base_url + "/media/r0"
    username = ""
    password = ""
    token = ""
    content_uri = ""
    data_file = ""

    def read_config(self):
        """read config"""
        with open("config.json") as json_file:
            data = json.load(json_file)
            self.username = data["username"]
            self.password = data["password"]

    def login(self):
        """login"""
        url = self.url + "/login"
        self.read_config()
        data = {"type": "m.login.password",
                "user": self.username, "password": self.password}
        response = requests.post(url, json=data)
        token = response.json()
        self.token = token["access_token"]

    def post(self, room_id, filename):
        """post"""
        url = self.create_url("/rooms/"+room_id + "/send/m.room.message")
        scicat_url = "https://scicat.esss.se/"
        pid = ""
        response = scicat.search(filename)
        result = response.json()
        first = result[0]
        if "pid" in first:
            print(pid)
            new_url = scicat_url + pid
            print(new_url)
        self.data_file = filename
        data = {"msgtype": "m.text",
                "body": "The file " + filename + " was created. See " + scicat_url + " for details"}
        print(url)
        response = requests.post(url, json=data)
        token = response.json()
        print(token)

    def upload_image(self, filename):
        """post"""
        media_url = self.media_url + "/upload?filename=im.png&access_token=" + self.token
        stats = os.stat(filename)

        headers = {"Content-Type": "image/png",
                   "Content-Length": str(stats.st_size)}
        filename = self.data_file
        visens.preview(filename, log=True, save="im.png")
        files = open('im.png', 'rb').read()
        response = requests.post(media_url, data=files, headers=headers)
        response_json = response.json()
        self.content_uri = response_json["content_uri"]
        print(media_url)

    def post_image(self, room_id):
        """post image"""

        url = self.create_url("/rooms/"+room_id + "/send/m.room.message")
        data = {"msgtype": "m.image",
                "body": "plot of data",
                'url': self.content_uri}

        response = requests.post(url, json=data)
        token = response.json()
        print(token)

    def upload(self):
        """upload"""
        url = self.create_url("/upload")
        headers = {'Content-type': 'image/png'}
        data = {}
        response = requests.post(url, headers=headers, json=data)
        response_json = response.json()
        print(response_json)

    def create_room(self, proposal_id, proposal_topic):
        """create room"""
        url = self.create_url("/createRoom")
        guests = ["@garethmurphy:ess"]
        data = {"room_alias_name": proposal_id,
                "topic": proposal_topic,
                "name": proposal_id,
                "invite": guests}
        print(data)
        response = requests.post(url, json=data)
        token = response.json()
        print(token)

    def create_url(self, api_call):
        """create url"""
        url = self.url + api_call + "?access_token=" + self.token
        return url

    def get_room_id(self, room_alias):
        """get room id"""
        room_alias_encode = urllib.parse.quote(room_alias)
        url = self.create_url("/directory/room/"+room_alias_encode)
        response = requests.get(url)
        response_json = response.json()
        print(url)
        print(response_json)
        room_id = response_json.get("room_id")
        return room_id

    def invite(self, room_id, user_id):
        """invite"""
        user_id = "@garethmurphy:ess"
        url = self.create_url("/rooms/" + room_id + "/invite")
        data = {"user_id": user_id}
        response = requests.post(url, json=data)
        token = response.json()
        print(token)


def main():
    """main"""
    bot = ScicatBot()
    bot.login()
    # bot.post()
    proposal_topic = "Investigation of water"
    proposal_id = "YC7SZ5"
    proposal_id = "QHK123"
    room_alias = "#"+proposal_id+":ess"
    #bot.create_room(room_alias, proposal_id, proposal_topic)
    room_id = bot.get_room_id(room_alias)
    filename = "nicos.hdf"
    bot.post(room_id, filename)
    filename = "im.png"
    bot.upload_image(filename)
    bot.post_image(room_id)
    # username = "@garethmurphy:ess"
    # bot.invite(room_id, username)


if __name__ == "__main__":
    main()
