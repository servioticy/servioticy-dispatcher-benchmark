__author__ = 'alvaro'

import configparser
import json

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
        new_initial_streams, new_prev_streams = put_so(config, prev_streams)
        initial_streams += new_initial_streams
        prev_streams = new_prev_streams + prev_streams
    return initial_streams


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
        json_so["streams"]["stream" + str(i)] = json_stream
        streams += ["stream" + str(i)]

    response, content = request('', 'POST', json.dumps(json_so), config)
    if int(response['status']) >= 300:
        print(content + '\n')
        return []
    json_content = json.loads(content)
    so_id = json_content['id']
    for stream in streams:
        initial_streams += [[so_id, stream]]

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
    initial_stream_ids = []
    stream_ids = []
    initial_streams = []
    streams = []
    num_streams = round(eval(config['TOPOLOGIES']['Streams']))
    if num_streams < 0:
        num_streams = 0

    json_file = open('./jsons/so.json')
    json_so = json.load(json_file)
    json_file.close()

    # Get aliases

    json_so["aliases"] = get_aliases(config)

    # Get streams
    for i in range(num_streams):
        json_stream = get_stream(config)
        if json_stream is None:
            continue
        json_so["streams"]["stream" + str(i)] = json_stream
        initial_stream_ids += ["stream" + str(i)]
        stream_ids += ["stream" + str(i)]

    # Get groups
    json_so["groups"] = get_groups(config, prev_streams)

    # Get cstreams
    json_so["cstreams"] = get_cstreams(config, json_so["groups"])

    response, content = request('', 'POST', json.dumps(json_so), config)

    if int(response['status']) >= 300:
        print(content + '\n')
        return []
    json_content = json.loads(content)
    so_id = json_content['id']
    for stream_id in stream_ids:
        streams += [[so_id, stream_id]]

    for initial_stream_id in initial_stream_ids:
        initial_streams += [[so_id, initial_stream_id]]

    return initial_streams, streams


def get_aliases(config):
    json_file = open('./jsons/aliases.json')
    json_aliases = json.load(json_file)
    json_file.close()
    return json_aliases


def get_groups(config, prev_streams):
    num_groups = round(eval(config['TOPOLOGIES']['Groups']))
    member_distribution = config['TOPOLOGIES']['MemberDistribution']
    groups = {}
    if num_groups < 1:
        return None
    if member_distribution == 'deterministic':
        return get_groups_det(config, prev_streams)

    for i in range(num_groups):
        not_found = True
        while not_found:
            sel_stream = round(eval(config['TOPOLOGIES']['MemberDistribution']))
            if sel_stream < 0 or sel_stream >= len(prev_streams):
                continue

            groups["group" + i]["soIds"] = prev_streams[sel_stream][0]
            groups["group" + i]["stream"] = prev_streams[sel_stream][1]
            not_found = False
    return groups


def get_groups_det(config, prev_streams):
    num_groups = round(eval(config['TOPOLOGIES']['Groups']))
    groups = {}
    if num_groups < 1:
        return None
    for i in range(num_groups):
        groups["group" + i]["soIds"] = prev_streams[i % (len(prev_streams))][0]
        groups["group" + i]["stream"] = prev_streams[i % (len(prev_streams))][1]

    return groups


def get_cstreams(config, groups):
    return


def get_cstream(config, group_subset, stream_subset):
    return


def get_channels(config, group_subset, stream_subset):
    return


def get_channel(config, operands):
    ms = round(eval(config['TOPOLOGIES']['CurrentValueMS']))
    json_file = open('./jsons/channel.json')
    json_channel = json.load(json_file)
    json_file.close()

    json_channel["type"] = "number"
    json_channel["current-value"] = "@presleep-cv@" + ms + "@postsleep-cv@" + "0"
    for operand in operands:
        json_channel["current-value"] += "+{$" + operand + ".channels.channel0.current-value}"

    return json_channel


def request(partial_url, method, body, config):
    headers = {
        'Authorization': config['API']['AuthToken'],
        'Content-Type': 'application/json; charset=UTF-8'
    }
    h = httplib2.Http()
    response, content = h.request(
        config['API']['BaseAddress'] + partial_url,
        method,
        body,
        headers)
    return response, content.decode('utf-8')


def main():
    new_setup('../benchmark.ini')


if __name__ == "__main__":
    main()