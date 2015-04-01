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
        for stream in self.streams_json:
            su = "{\"channels\": {\"channel0\": {\"current-value\": 1}}, \"lastUpdate\":" + str(int(time.time() * 1000)) + "}"
            response, content = self.request('/' + stream[0] + '/streams/' + stream[1], 'PUT',
                                             su)
            while int(response['status']) != 202:
                print(content + '\n')
                response, content = self.request('/' + stream[0] + '/streams/' + stream[1], 'PUT',
                                                 su)
                time.sleep(float(self.wait))
            time.sleep(float(self.wait))
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