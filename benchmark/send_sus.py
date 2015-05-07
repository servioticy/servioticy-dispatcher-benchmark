import os
import json
import time
import sys

import httplib2


class Sender:
    def __init__(self, base_url, api_key, total_sus, wait, streams_json):
        self.base_url = base_url
        self.api_key = api_key
        self.total_sus = total_sus
        self.wait = wait
        self.streams_json = streams_json
        return

    def request(self, partial_url, method, body):
        headers = {
            'Authorization': self.api_key,
            'Content-Type': 'application/json; charset=UTF-8'
        }
        h = httplib2.Http()
        response, content = h.request(
            self.base_url + partial_url,
            method,
            body,
            headers)
        return response, content.decode('utf-8')

    def send_sus(self):
        h = httplib2.Http()
        h.add_credentials('Administrator', 'masterautonomic')
        for stream in self.streams_json:
            sent = False
            while not sent:
                time.sleep(float(self.wait))
                counter = 0
                while counter != 5:
                    responseCB, contentCB  = h.request(
                        'http://minerva-11:8091/pools/default/buckets/soupdates',
                        'GET'
                    )
                    contentCB = json.loads(contentCB.decode('utf-8'))
                    if contentCB['basicStats']['opsPerSec'] != 0 or int(responseCB['status']) != 200:
                        counter = 0
                    else:
                        counter=1+counter
                    time.sleep(0.05)
                su = "{\"channels\": {\"channel0\": {\"current-value\": 1}}, \"lastUpdate\":" + str(int(time.time() * 1000)) + "}"
                response, content = self.request('/' + stream[0] + '/streams/' + stream[1], 'PUT',
                                                 su)
                if int(response['status']) != 202:
                    continue
                sent = True
        return


def main():
    stream_jsons = []
    if os.path.isdir(sys.argv[1]):
        for file in os.listdir(sys.argv[1]):
            json_file = open(sys.argv[1]+'/'+file)
            streams = json.load(json_file)
            json_file.close()
            stream_jsons.append(streams)
    elif os.path.isfile(sys.argv[1]):
        json_file = open(sys.argv[1])
        streams = json.load(json_file)
        json_file.close()
        stream_jsons.append(streams)
    for i in range(int(sys.argv[4])):
        for streams in stream_jsons:
            sender = Sender(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], streams)
            sender.send_sus()
            time.sleep(0)
    return


if __name__ == '__main__':
    main()
