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
        num_topologies = 1
    for i in range(num_topologies):
        initial_streams += put_topology(config)
    return


def put_topology(config):
    num_sos = round(eval(config['TOPOLOGIES']['SOs']))
    if num_sos < 1:
        num_sos = 1
    initial_streams = put_initial_so(config)
    prev_streams = initial_streams
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
        num_initial_streams = 1

    json_file = open('./jsons/initial_so.json')
    json_so = json.load(json_file)
    json_file.close()

    for i in range(num_initial_streams):
        json_stream = get_stream(config)

        json_so['streams']['stream' + str(i)] = json_stream
        streams += ['stream' + str(i)]

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
        num_channels = 1
    json_file = open('./jsons/initial_stream.json')
    json_stream = json.load(json_file)
    json_file.close()
    json_file = open('./jsons/initial_channel.json')
    json_channel = json.load(json_file)
    json_file.close()

    for i in range(num_channels):
        json_stream['channels']['channel' + str(i)] = json_channel

    return json_stream

def put_so(config, prev_streams):
    initial_stream_ids = []
    stream_ids = []
    initial_streams = []
    streams = []
    num_streams = round(eval(config['TOPOLOGIES']['Streams']))
    if num_streams < 1:
        num_streams = 1

    json_file = open('./jsons/so.json')
    json_so = json.load(json_file)
    json_file.close()

    # Get aliases

    json_so['aliases'] = get_aliases(config)

    # Get streams
    for i in range(num_streams):
        json_stream = get_stream(config)

        json_so['streams']['stream' + str(i)] = json_stream
        initial_stream_ids += ['stream' + str(i)]
        stream_ids += ['stream' + str(i)]

    # Get groups
    json_so['groups'] = get_groups(config, prev_streams)

    # Get cstreams
    json_so['streams'] = dict(
        list(json_so['streams'].items()) + list(get_cstreams(config, stream_ids, json_so['groups']).items()))
    response, content = request('', 'POST', json.dumps(json_so), config)
    if int(response['status']) >= 300:
        print(content + '\n')
        return [], []
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
        num_groups = 1
    if member_distribution == 'deterministic':
        return get_groups_det(config, prev_streams)

    for i in range(num_groups):
        not_found = True
        while not_found:
            sel_stream = round(eval(config['TOPOLOGIES']['MemberDistribution']))
            if sel_stream < 0 or sel_stream >= len(prev_streams):
                continue
            groups['group' + str(i)] = {}
            groups['group' + str(i)]['soIds'] = [prev_streams[sel_stream][0]]
            groups['group' + str(i)]['stream'] = prev_streams[sel_stream][1]
            not_found = False
    return groups


def get_groups_det(config, prev_streams):
    num_groups = round(eval(config['TOPOLOGIES']['Groups']))
    groups = {}
    if num_groups < 1:
        num_groups = 1
    for i in range(num_groups):
        groups['group' + str(i)] = {}
        groups['group' + str(i)]['soIds'] = [prev_streams[i % (len(prev_streams))][0]]
        groups['group' + str(i)]['stream'] = prev_streams[i % (len(prev_streams))][1]

    return groups


def get_cstreams(config, init_streams, groups):
    num_cstreams = round(eval(config['TOPOLOGIES']['CompositeStreams']))
    cstreams = {}
    if num_cstreams < 1:
        num_cstreams = 1
    stream_ids = []
    group_ids = []

    for i in range(num_cstreams):
        stream_ids += ['cstream' + str(i)]

    stream_ids += init_streams

    for key in groups.keys():
        group_ids += [key]

    group_distribution = config['TOPOLOGIES']['GroupDistribution']
    stream_distribution = config['TOPOLOGIES']['StreamRefsDistribution']

    for i in range(num_cstreams):
        group_set = []
        stream_set = []
        ratio = round(len(group_ids) / num_cstreams)
        if ratio < 1:
            ratio = 1
        if group_distribution == 'deterministic':
            group_set = group_ids[i * ratio:] + group_ids[:i * ratio]
        else:
            group_set = group_ids

        if stream_distribution == 'deterministic':
            stream_set = stream_ids[i * ratio:] + stream_ids[:i * ratio]
        else:
            stream_set = stream_ids

        cstreams['cstream' + str(i)] = get_cstream(config, group_set, stream_set)

    return cstreams


def get_cstream(config, group_subset, stream_subset):
    json_channels = get_channels(config, group_subset, stream_subset)

    pre_ms = round(eval(config['TOPOLOGIES']['PreFilterMS']))
    post_ms = round(eval(config['TOPOLOGIES']['PostFilterMS']))
    pre_prob = round(eval(config['TOPOLOGIES']['PreFilterProb']))
    post_prob = round(eval(config['TOPOLOGIES']['PreFilterProb']))

    if pre_ms < 0:
        pre_ms = 0
    if post_ms < 0:
        post_ms = 0

    json_file = open('./jsons/stream.json')
    json_cstream = json.load(json_file)
    json_file.close()

    json_cstream['pre-filter'] = '@presleep-filter@' + str(pre_ms) + '@postsleep-filter@'
    if pre_prob < 1:
        json_cstream['pre-filter'] += 'false'
    else:
        json_cstream[
            'pre-filter'] += '{$.lastUpdate} %' + str(pre_prob) + '==' + str(pre_prob) + ' - 1'

    json_cstream['post-filter'] = '@presleep-filter@' + str(pre_ms) + '@postsleep-filter@'
    if post_prob < 1:
        json_cstream['post-filter'] += 'false'
    else:
        json_cstream['post-filter'] += '{$.lastUpdate}+1 %' + str(post_prob) + '==' + str(post_prob) + ' - 1'

    json_cstream['channels'] = json_channels

    return json_cstream


def get_channels(config, group_subset, stream_subset):
    channels = {}
    num_channels = round(eval(config['TOPOLOGIES']['Channels']))
    if num_channels < 1:
        num_channels = 1
    group_distribution = config['TOPOLOGIES']['GroupDistribution']
    group_operands = config['TOPOLOGIES']['GroupOperands']
    stream_distribution = config['TOPOLOGIES']['StreamRefsDistribution']
    stream_operands = config['TOPOLOGIES']['StreamOperands']

    group_sets = distribute_operands(group_subset, num_channels, group_operands, group_distribution)
    stream_sets = distribute_operands(stream_subset, num_channels, stream_operands, stream_distribution)

    for i in range(num_channels):
        channels['channel' + str(i)] = get_channel(config, group_sets[i] + stream_sets[i])

    return channels


def distribute_operands_det(operands, num_sets, num_members):
    operand_sets = []
    j = 0
    for i in range(num_sets):
        nm = round(eval(num_members))
        if nm > len(operands):
            nm = len(operands)
        elif nm < 1:
            nm = 1
        operand_sets.append((operands + operands)[j:j + nm])
        j = (j + nm) % len(operands)
    return operand_sets


def distribute_operands(operands, num_sets, num_members, distribution):
    if distribution == 'deterministic':
        return distribute_operands_det(operands, num_sets, num_members)
    operand_sets = []
    for i in range(num_sets):
        operand_sets.append([])
        nm = round(eval(num_members))

        if nm < 1:
            nm = 1
        for j in range(nm):
            not_found = True
            while not_found:
                sel_operand = round(eval(distribution))
                if sel_operand < 0 or sel_operand >= len(operands):
                    continue
                operand_sets[i] += [operands[sel_operand]]
                not_found = False
    return operand_sets


def get_channel(config, operands):
    ms = round(eval(config['TOPOLOGIES']['CurrentValueMS']))
    if ms < 0:
        ms = 0
    json_file = open('./jsons/channel.json')
    json_channel = json.load(json_file)
    json_file.close()

    json_channel['current-value'] = '@presleep-cv@' + str(ms) + '@postsleep-cv@' + '0'
    for operand in operands:
        json_channel['current-value'] += '+{$' + operand + '.channels.channel0.current-value}'

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


if __name__ == '__main__':
    main()