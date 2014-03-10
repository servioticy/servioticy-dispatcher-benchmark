__author__ = 'alvaro'

import configparser
import json
import urllib

import httplib2


def new_setup(config_path):
    # Initial SO
    # While SOs
    #   Simple streams
    #   While Groups
    #       distribute members (not same SO)
    #   While Composite Streams
    #       pre & post
    #       While channels
    #            distribute GroupOperands
    #            distribute StreamOperands
    #
    config = configparser.ConfigParser()
    config.read(config_path)
    initial_streams = []
    num_topologies = round(eval(config['TOPOLOGIES']['Topologies']))
    if num_topologies < 1:
        return
    for i in range(num_topologies):
        initial_streams += put_topology(config)
    return


def put_topology(config):
    num_sos = round(eval(config['TOPOLOGIES']['SOs']))
    if num_sos < 1:
        return []
    initial_streams = put_initial_so(config)
    prev_streams = []
    num_sos -= 1
    for i in range(num_sos):
        new_initial_streams, new_prev_streams = new_so(config, prev_streams)
        initial_streams += new_initial_streams
        prev_streams = new_prev_streams
    return


def put_initial_so(config):
    initial_streams = []
    streams = []
    num_initial_streams = round(eval(config['TOPOLOGIES']['InitialStreams']))
    if num_initial_streams < 1:
        return []

    json_file = open('./jsons/initial_so.json')
    json_so = json.load(json_file)
    json_file.close()

    for i in range(num_initial_streams):
        json_stream = get_stream(config)
        if json_stream is None:
            continue
        json_so["streams"][str(i)] = json_stream
        streams += [str(i)]

    response, content = request('', 'POST', json.dumps(json_so), config)
    if int(response['status']) >= 300:
        print(content + '\n')
        return []
    json_content = json.load(content)
    id = json_content['id']
    for i in streams:
        initial_streams += [id, streams[i]]

    return initial_streams


def get_stream(config):
    num_channels = round(eval(config['TOPOLOGIES']['Channels']))
    if num_channels < 1:
        return None
    json_file = open('./jsons/initial_stream.json')
    json_stream = json.load(json_file)
    json_file.close()
    json_file = open('./jsons/initial_channel.json')
    json_channel = json.load(json_file)
    json_file.close()

    for i in range(num_channels):
        json_stream['channels'][str(i)] = json_channel

    return json_stream


def put_so(config, prev_streams):
    return


def get_groups(config, prev_streams):
    return


def get_group(config, streams):
    return


def get_cstreams(config, groups):
    return


def get_cstream(config, group_subset, stream_subset):
    return


def get_channels(config, group_subset, stream_subset):
    return


def get_channel(config, group_subsubset, stream_subsubset):
    return


def request(partial_url, method, body, config):
    headers = {
        'Authorization': config['API']['AuthToken'],
        'Content-Type': 'application/json; charset=UTF-8'
    }
    h = httplib2.Http()
    response, content = h.request(
        config['API']['BaseAddress'] + partial_url,
        method,
        urllib.urlencode(body),
        headers)
    return response, content