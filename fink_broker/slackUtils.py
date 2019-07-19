#!/usr/bin/env python3
# Copyright 2019 AstroLab Software
# Author: Abhishek Chauhan
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import slack


class FinkSlackClient:

    def __init__(self, api_token):
        self._client = slack.WebClient(token=api_token)

        try:
            self._client.auth_test()
        except Exception:
            print("Authentication Error: Invalid Token")

        # create a dict of {channelName: ID}
        channels = self._client.channels_list()['channels']
        self._channel_ids = {x['name']: x['id'] for x in channels}

        # create a dict of {userName: ID}
        members = self._client.users_list()['members']
        self._user_ids = {x['real_name']: x['id'] for x in members}

    def send_message(self, recipient, msg):
        """sends a message to a given channel/user on the slack workspace

        Parameters
        ----------
        recipient: str
            name of recipient e.g. a channel: '#general'
            or a user: 'Abhishek Chauhan'
        msg: str
            message payload to send
        """
        # if recipient is a channel e.g. #general
        if recipient[0] == '#':
            name = recipient[1:]
            if name not in self._channel_ids:
                print("Invalid Channel Name")
                return
            channel_id = self._channel_ids[name]
        else:   # user
            if recipient not in self._user_ids:
                print("User is not member of your slack workspace")
                return
            channel_id = self._user_ids[recipient]

        response = self._client.chat_postMessage(
                channel=channel_id,
                text=msg)


def get_slack_client():
    """ returns an object of class FinkSlackClient"""
    try:
        api_token = os.environ["SLACK_API_TOKEN"]
    except KeyError:
        print("please set the env variable: SLACK_API_TOKEN")
        return

    return FinkSlackClient(api_token)
