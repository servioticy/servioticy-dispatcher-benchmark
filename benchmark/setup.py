import time

__author__ = 'alvaro'

import configparser
import json
import random
import networkx as nx
import pylab as p

import httplib2


class Setup:
    def __init__(self, config_path):
        self.topologies = []
        self.config = configparser.ConfigParser()
        self.initial_streams = []

        self.config.read(config_path)

        random.seed(1)

        num_topologies = round(eval(self.config['TOPOLOGIES']['Topologies']))
        if num_topologies < 1:
            num_topologies = 1
        for i in range(num_topologies):
            self.topologies.append(Topology(config_path))
            self.initial_streams += self.topologies[-1].initial_streams
        return

    def write_initial_streams(self, out_path):
        # Store the initial streams in the out_path
        f = open(out_path, 'w')
        f.write(json.dumps(self.initial_streams))
        f.close()
        return

class Topology:
    def __init__(self, config_path):
        self.config = configparser.ConfigParser()
        self.initial_streams = []
        self.streams = []

        self.so_graph = nx.DiGraph()
        self.stream_graph = nx.DiGraph()
        self.channel_graph = nx.DiGraph()

        self.det_operands_index = 0

        self.config.read(config_path)

        num_initsos = round(eval(self.config['TOPOLOGIES']['InitialSOs']))
        if num_initsos < 0:
            num_initsos = 0
        for i in range(num_initsos):
            self.put_initial_so()
        num_sos = round(eval(self.config['TOPOLOGIES']['SOs']))
        if num_sos < 0:
            num_sos = 0
        for i in range(num_sos):
            self.put_so()
        return

    def put_initial_so(self):
        streams = []
        num_initial_streams = round(eval(self.config['TOPOLOGIES']['InitialStreams']))
        if num_initial_streams < 1:
            num_initial_streams = 1

        json_file = open('./jsons/initial_so.json')
        json_so = json.load(json_file)
        json_file.close()

        for i in range(num_initial_streams):
            json_stream = self.make_stream()

            json_so['streams']['stream' + str(i)] = json_stream
            streams += ['stream' + str(i)]

        response, content = self.request('', 'POST', json.dumps(json_so))
        if int(response['status']) >= 300:
            print(content + '\n')
            return []
        json_content = json.loads(content)
        so_id = json_content['id']

        self.so_graph.add_node(so_id)

        for stream in streams:
            self.initial_streams += [[so_id, stream]]
            self.streams += [[so_id, stream]]

            self.stream_graph.add_node(so_id + ":" + stream)

        return

    def put_so(self):
        initial_stream_ids = []
        stream_ids = []
        initial_streams = []
        streams = []
        num_streams = round(eval(self.config['TOPOLOGIES']['Streams']))
        if num_streams < 0:
            num_streams = 0

        json_file = open('./jsons/so.json')
        json_so = json.load(json_file)
        json_file.close()

        # Streams
        for i in range(num_streams):
            json_stream = self.make_stream()

            json_so['streams']['stream' + str(i)] = json_stream
            initial_stream_ids += ['stream' + str(i)]

        # CStreams
        groups = {}
        input_sets, cstreams = self.make_cstreams(initial_stream_ids, groups)
        # Groups
        json_so['groups'] = groups
        json_so['streams'] = dict(
            list(json_so['streams'].items()) + list(cstreams.items()))
        response, content = self.request('', 'POST', json.dumps(json_so))
        if int(response['status']) >= 300:
            print(content + '\n')
            return [], []
        json_content = json.loads(content)
        so_id = json_content['id']

        self.so_graph.add_node(so_id)
        for initial_stream_id in initial_stream_ids:
            self.initial_streams += [[so_id, initial_stream_id]]
            self.stream_graph.add_node(so_id + ":" + initial_stream_id)
        for stream_id in json_so['streams'].keys():
            self.streams += [[so_id, stream_id]]
            self.stream_graph.add_node(so_id + ":" + stream_id)

        for k in json_so['groups'].keys():
            for soid in json_so['groups'][k]["soIds"]:
                self.so_graph.add_edge(soid, so_id)

        for k in input_sets.keys():
            self.stream_graph.add_node(so_id + ":" + k)
            for group_id in input_sets[k]['groups']:
                for group in json_so['groups'][group_id]:
                    for soId in json_so['groups'][group_id]["soIds"]:
                        self.stream_graph.add_edge(soId + ":" + json_so['groups'][group_id]["stream"],
                                                   so_id + ":" + k)

            for stream_id in input_sets[k]['streams']:
                self.stream_graph.add_edge(so_id + ":" + stream_id, so_id + ":" + k)


        return

    def make_stream(self):
        num_channels = 1  # round(eval(self.config['TOPOLOGIES']['Channels']))
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

    def make_group(self, stream, groups):
        groups.update({'$'+stream[0]+':'+ stream[1]: {'soIds': [stream[0]], 'stream': stream[1]}})
        return '$'+stream[0] + ':' + stream[1]

    def make_cstreams(self, init_streams, existing_groups):
        num_cstreams = round(eval(self.config['TOPOLOGIES']['CompositeStreams']))
        cstreams = {}
        input_sets = {}
        if num_cstreams < 1:
            num_cstreams = 1
        stream_ids = []
        stream_ids += init_streams

        for i in range(num_cstreams):
            input_sets['cstream' + str(i)], cstreams['cstream' + str(i)] = self.make_cstream(stream_ids, existing_groups)
            stream_ids += ['cstream' + str(i)]

        return input_sets, cstreams


    def make_cstream(self, stream_subset, existing_groups):
        input_sets, json_channels = self.make_channels(stream_subset, existing_groups)

        json_file = open('./jsons/stream.json')
        json_cstream = json.load(json_file)
        json_file.close()

        json_cstream['channels'] = json_channels

        return input_sets, json_cstream


    def make_channels(self, stream_subset, existing_groups):
        channels = {}
        num_channels = round(eval(self.config['TOPOLOGIES']['Channels']))
        if num_channels < 1:
            num_channels = 1
        operand_distribution = self.config['TOPOLOGIES']['OperandDistribution']
        operands = self.config['TOPOLOGIES']['Operands']

        group_sets, stream_sets = self.distribute_operands(stream_subset, operands, operand_distribution, existing_groups)

        input_sets = {"groups": group_sets, "streams": stream_sets}

        for i in range(num_channels):
            channels['channel' + str(i)] = self.make_channel(group_sets + stream_sets)

        return input_sets, channels


    def distribute_operands_det(self, new_streams, num_operands, existing_groups):
        operand_sets = []
        new_groups = []
        groups = list(self.streams)
        streams = list(new_streams)
        nm = round(eval(num_operands))
        if nm > len(new_streams + self.streams):
            nm = len(new_streams + self.streams)
        elif nm < 1:
            nm = 1
        for j in range(nm):
            if len(streams + groups) == 0:
                break
            sel_operand = self.det_operands_index = self.det_operands_index + j % len(streams + groups)
            if sel_operand < len(groups):
                groupid = self.make_group(groups.pop(sel_operand), existing_groups)
                new_groups += [groupid]
            else:
                sel_operand = sel_operand - len(groups)
                operand_sets += [streams.pop(sel_operand)]
        self.det_operands_index = self.det_operands_index + 1 % len(streams + groups)
        return new_groups, operand_sets


    def distribute_operands(self, new_streams, num_operands, distribution, existing_groups):
        # The operands can be repeated
        if distribution == 'deterministic':
            return self.distribute_operands_det(new_streams, num_operands, existing_groups)
        operand_sets = []
        new_groups = []

        nm = round(eval(num_operands))
        groups = list(self.streams)
        streams = list(new_streams)
        if nm < 1:
            nm = 1
        for j in range(nm):
            if len(streams + groups) == 0:
                break
            sel_operand = round(eval(distribution))
            # if sel_operand < 0 or sel_operand >= len(operands):
            #     continue
            if sel_operand < 0:
                sel_operand = 0
            elif sel_operand > 1:
                sel_operand = 1

            sel_operand = round(sel_operand * (len(streams + groups) - 1))

            if sel_operand < 0:
                continue
            elif sel_operand < len(groups):
                groupid = self.make_group(groups.pop(sel_operand), existing_groups)
                new_groups += [groupid]
            else:
                sel_operand = sel_operand - len(groups)
                operand_sets += [streams.pop(sel_operand)]
        return new_groups, operand_sets


    def make_channel(self, operands):
        ms = round(eval(self.config['TOPOLOGIES']['CurrentValueMS']))
        if ms < 0:
            ms = 0
        json_file = open('./jsons/channel.json')
        json_channel = json.load(json_file)
        json_file.close()

        json_channel['current-value'] = self.make_function_header(operands) + '{'
        # filter
        filter_prob = round(eval(self.config['TOPOLOGIES']['FilterProb']))
        if filter_prob < 0: filter_prob = 0
        elif filter_prob > 1: filter_prob = 1
        json_channel['current-value'] += 'if(Math.random() < ' + str(filter_prob) + '){return null;}'
        json_channel['current-value'] += 'var start = new Date().getTime();for(var i=0;i<1e7;i++){if((new Date().getTime()-start)>' + str(
            ms) + '){break;}} return 0'

        for operand in operands:
            json_channel['current-value'] += '+' + operand + '.channels.channel0[\'current-value\']'
        json_channel['current-value'] += ';}'

        return json_channel

    def make_function_header(self, operands):
        header = "function("

        for operand in set(operands):
            header += operand + ','

        return header[:-1] + ')'


    def request(self, partial_url, method, body):
        time.sleep(0.5)
        headers = {
            'Authorization': self.config['API']['AuthToken'],
            'Content-Type': 'application/json; charset=UTF-8'
        }
        h = httplib2.Http()
        response, content = h.request(
            self.config['API']['BaseAddress'] + partial_url,
            method,
            body,
            headers)
        return response, content.decode('utf-8')


def main():
    setup = Setup('../benchmark.ini')
    setup.write_initial_streams(setup.config['TOPOLOGIES']['InitialStreamsFile'])

    for i in range(len(setup.topologies)):
        nx.write_gml(setup.topologies[i].so_graph,
                     setup.config['TOPOLOGIES']['GraphsDir'] + 'so_graph_' + str(i) + '.gml')
        nx.write_gml(setup.topologies[i].stream_graph,
                     setup.config['TOPOLOGIES']['GraphsDir'] + 'stream_graph_' + str(i) + '.gml')
    return


if __name__ == '__main__':
    main()