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
from fink_broker.tester import spark_unit_tests
from pyspark.sql import DataFrame
from fink_broker.loggingUtils import get_fink_logger
logger = get_fink_logger(__name__, "INFO")

class FinkSlackClient:

    def __init__(self, api_token):
        self._client = slack.WebClient(token=api_token)

        try:
            self._client.auth_test()
        except Exception:
            logger.error("Authentication Error: Invalid Token")

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
                logger.error("Invalid Channel Name")
                return
            channel_id = self._channel_ids[name]
        else:   # user
            if recipient not in self._user_ids:
                logger.error("User is not member of your slack workspace")
                return
            channel_id = self._user_ids[recipient]

        response = self._client.chat_postMessage(
            channel=channel_id, text=msg, as_user="false",
            username="fink-alert", icon_emoji="strend:")


def get_api_token():
    """returns slack api token

    Returns
    ----------
    api_token: str
        value of the env variable SLACK_API_TOKEN if set, or None
    """
    api_token = None

    try:
        api_token = os.environ["SLACK_API_TOKEN"]
    except KeyError:
        logger.error("SLACK_API_TOKEN is not set")

    return api_token

def get_slack_client():
    """ returns an object of class FinkSlackClient

    Returns
    ----------
    FinkSlackClient:
        an object of class FinkSlackClient initialized with OAuth token
    """
    api_token = get_api_token()

    if api_token:
        return FinkSlackClient(api_token)
    else:
        logger.error("please set the env variable: SLACK_API_TOKEN")

def get_show_string(
        df: DataFrame, n: int = 20,
        truncate: int = 0, vertical: bool = False) -> str:
    """returns the string printed by df.show()

    Parameters
    ----------
    df: DataFrame
        a spark dataframe
    n: int
        number of rows to print
    truncate: int
        truncate level for columns, default: 0 means no truncation
    vertical: bool
        set true to get output in vertical format (not tabular)

    Returns
    ----------
    showString: str
        string printed by DataFrame.show()

    Examples
    ----------
    >>> df = spark.sparkContext.parallelize(zip(
    ...     ["ZTF18aceatkx", "ZTF18acsbjvw"],
    ...     ["Star", "Unknown"])).toDF([
    ...       "objectId", "cross_match_alerts_per_batch"])
    >>> msg_string = get_show_string(df)
    >>> print(msg_string)
    +------------+----------------------------+
    |objectId    |cross_match_alerts_per_batch|
    +------------+----------------------------+
    |ZTF18aceatkx|Star                        |
    |ZTF18acsbjvw|Unknown                     |
    +------------+----------------------------+
    <BLANKLINE>
    """
    return(df._jdf.showString(n, truncate, vertical))

def send_slack_alerts(df: DataFrame, channels: str):
    """Send alerts to slack channel

    Parameters
    ----------
    df: DataFrame
        spark dataframe to send slack alerts
    channels: str
        path to file with list of channels to which alerts
        must be sent

    Examples
    ----------
    >>> df = spark.sparkContext.parallelize(zip(
    ...     ["ZTF18aceatkx", "ZTF18acsbjvw"],
    ...     [697251923115015002, 697251921215010004],
    ...     [20.393772, 20.4233877],
    ...     [-25.4669463, -27.0588511],
    ...     ["slacktest", "Unknown"])).toDF([
    ...       "objectId", "candid", "candidate_ra",
    ...       "candidate_dec", "cross_match_alerts_per_batch"])
    >>> df.show()
    +------------+------------------+------------+-------------+----------------------------+
    |    objectId|            candid|candidate_ra|candidate_dec|cross_match_alerts_per_batch|
    +------------+------------------+------------+-------------+----------------------------+
    |ZTF18aceatkx|697251923115015002|   20.393772|  -25.4669463|                   slacktest|
    |ZTF18acsbjvw|697251921215010004|  20.4233877|  -27.0588511|                     Unknown|
    +------------+------------------+------------+-------------+----------------------------+
    <BLANKLINE>
    >>> channels = "slacktest_channel.txt"
    >>> with open(channels, 'wt') as f:
    ...     f.write("slacktest")
    9
    >>> send_slack_alerts(df, channels)
    >>> os.remove(channels)
    """
    channels_list = []
    with open(channels, 'rt') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                channels_list.append('#' + line)

    finkSlack = get_slack_client()

    # filter out unknown object types
    df = df.filter("cross_match_alerts_per_batch!='Unknown'")
    object_types = df \
        .select("cross_match_alerts_per_batch")\
        .distinct()\
        .collect()
    object_types = [x[0] for x in object_types]
    # Send alerts to the respective channels
    for obj in object_types:
        channel_name = '#' + ''.join(e.lower() for e in obj if e.isalpha())
        if channel_name in channels_list:
            alert_text = get_show_string(
                df.filter(df.cross_match_alerts_per_batch == obj))
            slack_alert = "```\n" + alert_text + "```"

            finkSlack.send_message(channel_name, slack_alert)

if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    # Run the Spark test suite if SLACK_API_TOKEN exist
    api_token = get_api_token()

    if api_token:
        spark_unit_tests(globals())
    else:
        logger.info("Skipping Unit Tests")
