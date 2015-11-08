import configparser
import json
import random
import uuid

import networkx as nx
import httplib2
from random import randint

class Setup:
    def __init__(self, config_path, deploy=True, sos=None, operands=None, isos=None):
        self.topologies = []
        self.config = configparser.ConfigParser()
        self.initial_streams = []
        self.config.read(config_path)

        random.seed(a="1")
        num_topologies = round(eval(self.config['TOPOLOGIES']['Topologies']))
        if num_topologies < 1:
            num_topologies = 1
        for i in range(num_topologies):
            self.topologies.append(Topology(config_path, deploy, sos, operands, isos))
            self.initial_streams += self.topologies[-1].initial_streams
        return

    def write_initial_streams(self, out_path):
        # Store the initial streams in the out_path
        f = open(out_path, 'w')
        f.write(json.dumps(self.initial_streams))
        f.close()
        return

class Topology:
    def __init__(self, config_path, deploy=True, sos=None, operands=None, isos=None):
        self.config = configparser.ConfigParser()
        self.initial_streams = []
        self.streams = []
        self.dependencies = {}
        self.noperands = operands

        self.deploy = deploy

        self.so_graph = nx.DiGraph()
        self.stream_graph = nx.DiGraph()
        self.channel_graph = nx.DiGraph()

        self.det_operands_index = 0

        self.config.read(config_path)
        response, self.auth = self.get_access_token()

        num_initsos = round(eval(self.config['TOPOLOGIES']['InitialSOs'] if isos == None else isos))
        if num_initsos < 0:
            num_initsos = 0
        for i in range(num_initsos):
            self.put_initial_so()
        num_sos = round(eval(self.config['TOPOLOGIES']['SOs'] if sos == None else sos))
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
        if self.deploy:
            so_id, api_token = self.post_so(json.dumps(json_so))

        else:
            so_id = str(uuid.uuid4())
            api_token = "none"

        self.so_graph.add_node(so_id)

        for stream in streams:
            self.initial_streams += [[so_id, stream, api_token]]
            self.streams += [[so_id, stream, api_token]]
            self.dependencies[len(self.streams)-1] = [len(self.streams)-1]

            self.stream_graph.add_node(so_id + ":" + stream)
            # Initialize the streams
            if self.deploy:
                self.put_initial_su(so_id, stream, api_token, json_so)
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
        input_sets, cstreams, new_dependencies = self.make_cstreams(initial_stream_ids, groups)
        self.dependencies.update(new_dependencies)
        # Groups
        json_so['groups'] = groups
        json_so['streams'] = dict(
            list(json_so['streams'].items()) + list(cstreams.items()))
        if self.deploy:
            so_id, api_token = self.post_so(json.dumps(json_so))
        else:
            so_id = str(uuid.uuid4())
            api_token = 'none'

        self.so_graph.add_node(so_id)
        for initial_stream_id in initial_stream_ids:
            self.initial_streams += [[so_id, initial_stream_id, api_token]]
            self.stream_graph.add_node(so_id + ":" + initial_stream_id)
            # Initialize the streams
            if self.deploy:
                self.put_initial_su(so_id, initial_stream_id, api_token, json_so)

        stream_keys = list(json_so['streams'].keys())
        stream_keys = sorted(stream_keys)
        for i in range(len(stream_keys)):
            stream_id = stream_keys[i]
            self.streams += [[so_id, stream_id, api_token]]
            self.stream_graph.add_node(so_id + ":" + stream_id)
            # Initialize the streams
            if self.deploy:
                su = {}
                su['channels'] = {}
                su['lastUpdate'] = 1
                for j in range(len(json_so['streams'][stream_id]['channels'])):
                    su['channels']['channel'+str(j)] = {'current-value': 0}
                # response, content = self.request(so_id+'/streams/'+stream_id, 'PUT', json.dumps(su))
                # while int(response['status']) != 202:
                #     response, content = self.request(so_id+'/streams/'+stream_id, 'PUT', json.dumps(su))
            # self.dependencies[len(self.streams)-1] = [len(self.streams)-1]
            # if stream_id in new_dependencies:
            #     for new_dependency in new_dependencies[stream_id]:
            #         if new_dependency in stream_keys:
            #             stream_pos = len(self.streams)-(i+1) + stream_keys.index(new_dependency)
            #             self.dependencies[len(self.streams)-1].append(stream_pos)
            #         else:
            #             self.dependencies[len(self.streams)-1].append(new_dependency)
            # else:
            #     self.dependencies[len(self.streams)-1].append(len(self.streams)-1)


        for k in json_so['groups'].keys():
            for soid in json_so['groups'][k]["soIds"]:
                self.so_graph.add_edge(soid, so_id)

        for k in input_sets.keys():
            self.stream_graph.add_node(so_id + ":" + k)
            for group_id in input_sets[k]['groups']:
                for soId in json_so['groups'][group_id]["soIds"]:
                    self.stream_graph.add_edge(soId + ":" + json_so['groups'][group_id]["stream"],
                                               so_id + ":" + k)

            for stream_id in input_sets[k]['streams']:
                self.stream_graph.add_edge(so_id + ":" + stream_id, so_id + ":" + k)
        return

    def make_stream(self):
        num_channels = round(eval(self.config['TOPOLOGIES']['Channels']))
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
        groups.update({'A'+stream[0]+ stream[1]: {'soIds': [stream[0]], 'stream': stream[1]}})
        return 'A'+stream[0] + stream[1]

    def make_cstreams(self, init_streams, existing_groups):
        num_cstreams = round(eval(self.config['TOPOLOGIES']['CompositeStreams']))
        cstreams = {}
        input_sets = {}
        if num_cstreams < 1:
            num_cstreams = 1
        new_streams = []
        new_streams += init_streams
        new_dependencies = {}

        for i in range(num_cstreams):
            input_sets['xstream' + str(i)], cstreams['xstream' + str(i)], local_dependencies = self.make_cstream(new_streams, new_dependencies, existing_groups)
            new_streams += ['xstream' + str(i)]
            new_dependencies[len(self.streams) + len(new_streams) - 1] = local_dependencies

        return input_sets, cstreams, new_dependencies


    def make_cstream(self, new_streams, new_dependencies, existing_groups):
        input_sets, json_channels, local_dependencies = self.make_channels(new_streams, new_dependencies, existing_groups)

        json_file = open('./jsons/stream.json')
        json_cstream = json.load(json_file)
        json_file.close()

        json_cstream['channels'] = json_channels

        return input_sets, json_cstream, local_dependencies


    def make_channels(self, new_streams, new_dependencies, existing_groups):
        channels = {}
        num_channels = round(eval(self.config['TOPOLOGIES']['Channels']))
        if num_channels < 1:
            num_channels = 1
        operand_distribution = self.config['TOPOLOGIES']['OperandDistribution']
        operands = round(eval(self.config['TOPOLOGIES']['Operands'] if self.noperands == None else self.noperands))
        if operands < 1:
            operands = 1

        group_sets, stream_sets, local_dependencies = self.distribute_operands(new_streams, new_dependencies, operands, operand_distribution, existing_groups)

        input_sets = {"groups": group_sets, "streams": stream_sets}

        for i in range(num_channels):
            channels['channel' + str(i)] = self.make_channel(group_sets + stream_sets)

        return input_sets, channels, local_dependencies

    # DEPENDENCY CONTROL DOES NOT WORK, FIX WITH THE NON-DETERMINISTIC EXAMPLE
    def distribute_operands_det(self, new_streams, new_dependencies, num_operands, existing_groups):
        sel_streams = []
        sel_groups = []
        groups = list(self.streams)
        streams = list(new_streams)
        nm = num_operands
        local_dependencies = []
        if nm > len(new_streams + self.streams):
            nm = len(new_streams + self.streams)
        for j in range(nm):
            found = False
            while not found:
                found = True
                if len(streams + groups) == 0:
                    break
                sel_operand = self.det_operands_index % len(streams + groups)
                self.det_operands_index = self.det_operands_index + 1 % len(streams + groups)
                if sel_operand < len(groups):
                    for dependency in self.dependencies[sel_operand]:
                        if dependency in local_dependencies:
                            #groups.pop(sel_operand)
                            self.det_operands_index = self.det_operands_index
                            found = False
                            break
                    if not found:
                        continue
                    local_dependencies.extend(self.dependencies[sel_operand])
                    groupid = self.make_group(groups[sel_operand], existing_groups)
                    sel_groups += [groupid]
                else:
                    sel_operand = sel_operand - len(groups)
                    if streams[sel_operand] not in new_dependencies.keys():
                        new_dependencies[streams[sel_operand]] = [streams[sel_operand]]

                    for dependency in new_dependencies[sel_operand]:
                        if dependency in local_dependencies:
                            #streams.pop(sel_operand)
                            found = False
                            break
                    if not found:
                        continue
                    local_dependencies.extend(new_dependencies[sel_operand])
                    sel_streams += [streams[sel_operand]]
        self.det_operands_index = self.det_operands_index + 1 % len(streams + groups)
        return sel_groups, sel_streams, local_dependencies

    def distribute_operands(self, new_streams, new_dependencies, num_operands, distribution, existing_groups):
        # The operands can be repeated
        if distribution == 'deterministic':
            return self.distribute_operands_det(new_streams, new_dependencies, num_operands, existing_groups)
        sel_streams = []
        sel_groups = []

        nm = num_operands
        groups = list(range(len(self.streams)))
        # local_streams = range(new_streams)
        operands = list(range(len(self.streams + new_streams)))
        local_dependencies = [len(operands)]
        for j in range(nm):
            if len(operands) == 0:
                break
            found = False
            while not found:
                found = True
                if len(operands) == 0:
                    break
                sel_operand = round(eval(distribution))
                # if sel_operand < 0 or sel_operand >= len(operands):
                #     continue
                if sel_operand < 0:
                    sel_operand = 0
                elif sel_operand > 1:
                    sel_operand = 1
                sel_operand = round(sel_operand * (len(operands) - 1))

                if operands[sel_operand] not in self.dependencies.keys():
                    self.dependencies[operands[sel_operand]] = [operands[sel_operand]]
                for dependency in self.dependencies[operands[sel_operand]]:
                    if dependency in local_dependencies:
                        operands.pop(sel_operand)
                        found = False
                        break
                if not found:
                    continue
                local_dependencies.extend(self.dependencies[operands[sel_operand]])

                if sel_operand < len(groups):
                    groups.pop(sel_operand)
                    groupid = self.make_group(self.streams[operands.pop(sel_operand)], existing_groups)
                    sel_groups += [groupid]
                else:
                    sel_streams += [new_streams[operands.pop(sel_operand)-len(self.streams)]]

        return sel_groups, sel_streams, local_dependencies


    # WOLFRAM CODE

    def generateCodeString(self, variableArray, withRandom):
        ret = ""
        operationsSimple = ["+","-", "*","/", ]
        operationsBinary = ["&", "|", "^", "<<", ">>"]
        operationsString = ["+"]
        operationsPost = ["Math.min","Math.max"]
        operationsPostOne = ["Math.round","Math.ceil","Math.floor","Math.sqrt","Math.abs"]
        operations = [operationsSimple,operationsSimple,operationsBinary, operationsString,operationsPost,operationsPostOne]
        OPERATIONPOST_pos = 4
        if withRandom:
            first = True
            i = 0
            while i < len(variableArray):
                #for s in variableArray:
                currentElement = self.getThinkToAdd(variableArray, i, operations, OPERATIONPOST_pos)
                if first == False:
                    opSet = self.getOperationSet(0, OPERATIONPOST_pos - 1, operations)
                    # Get random number to select the operation of a set
                    n = len(opSet)
                    randOpPos = randint(0,n-1)
                    # Add operation
                    ret += opSet[randOpPos]
                # Add variable
                ret += currentElement
                first = False
                i = i +1
            return ret
        else:
            first = True
            for s in variableArray:
                if first == False:
                    ret += "+"
                ret += s
                first = False
            return ret


    def getOperationSet(self, start, stop, operations):
        if start < 0:
            start = 0
        if stop > len(operations):
            stop = len(operations)
        randOpSet = randint(start,stop)
        return operations[randOpSet]


    def getThinkToAdd(self, variableArray, i, operations, OPERATIONPOST_pos):
        ret = ""
        # Select the set of operations
        randOpSet = randint(0,OPERATIONPOST_pos + 1)
        opSet =  operations[randOpSet]
        # Add Math operations if necessary
        if randOpSet == OPERATIONPOST_pos:
            # Get random number to select the operation of a set
            n = len(opSet)
            randOpPos = randint(0,n-1) # select the operator in the set
            randNumOfParameters = len(variableArray)-1 #randint(i, len(variableArray)-1)
            ret =  opSet[randOpPos] + "(" + variableArray[i]
            i = i+1
            while i < randNumOfParameters:
                ret = ret + "," + variableArray[i]
                i = i +1
            ret = ret + ")"
            opSet = self.getOperationSet(0,OPERATIONPOST_pos -1, operations)
        elif randOpSet == OPERATIONPOST_pos +1:
            # Get random number to select the operation of a set
            n = len(opSet)
            randOpPos = randint(0,n-1)
            ret = opSet[randOpPos] + "(" + variableArray[i] + ")"
            opSet = self.getOperationSet(0,OPERATIONPOST_pos -1, operations)
        else:
            ret = variableArray[i]
        return ret

    # END WOLFRAM CODE

    def make_channel(self, operands):
        json_file = open('./jsons/channel.json')
        json_channel = json.load(json_file)
        json_file.close()

        # filter
        filter_prob = round(eval(self.config['TOPOLOGIES']['FilterProb']))
        if filter_prob < 0: filter_prob = 0
        elif filter_prob > 1: filter_prob = 1


        sel_operands = []
        for operand in operands:
            sel_operands.append('{$' + operand + '.channels.channel0.current-value}');
        # json_channel['current-value'] += ';'
        json_channel['current-value'] = self.generateCodeString(sel_operands, self.config['TOPOLOGIES']['RandomOperands'])

        return json_channel

    def make_function_header(self, operands):
        header = "function("

        for operand in set(operands):
            header += operand + ','

        return header[:-1] + ')'

    def post_so(self, so):
        headers = {
            'Authorization': self.auth,
            'Content-Type': 'application/json; charset=UTF-8'
        }
        response, content = self.request_api('', 'POST', so, headers)
        while int(response['status']) != 201:
            print(content + '\n')
            response, content = self.request_api('', 'POST', so, headers)
        json_content = json.loads(content)
        return json_content['id'], json_content['api_token']

    def put_initial_su(self, so_id, stream, token, json_so):
        headers = {
            'Authorization': token,
            'Content-Type': 'application/json; charset=UTF-8'
        }
        su = {}
        su['channels'] = {}
        su['lastUpdate'] = 1
        for i in range(len(json_so['streams'][stream]['channels'])):
            su['channels']['channel'+str(i)] = {'current-value': 0}
        response, content = self.request_api(so_id+'/streams/'+stream, 'PUT', json.dumps(su), headers)
        while int(response['status']) != 202:
            response, content = self.request_api(so_id+'/streams/'+stream, 'PUT', json.dumps(su), headers)
        return

    def request_api(self, partial_url, method, body, headers):

        h = httplib2.Http()
        response, content = h.request(
            self.config['API']['BaseAddress'] + partial_url,
            method,
            body,
            headers)
        return response, content.decode('utf-8')

    def get_access_token(self):

        h = httplib2.Http()
        response, content = h.request(
            self.config['IDM']['BaseAddress'] + 'auth/user/',
            'POST',
            '{"username":"' + self.config['IDM']['User'] + '","password":"' + self.config['IDM']['Password'] + '"}',
            {'Content-Type': 'application/json;charset=UTF-8'})
        return response, json.loads(content.decode('utf-8'))['accessToken']


def main():
    setup = Setup('../benchmark.ini', True)
    setup.write_initial_streams(setup.config['TOPOLOGIES']['InitialStreamsFile'])

    for i in range(len(setup.topologies)):
        nx.write_gml(setup.topologies[i].so_graph,
                     setup.config['TOPOLOGIES']['GraphsDir'] + 'so_graph_' + str(i) + '.gml')
        nx.write_gml(setup.topologies[i].stream_graph,
                     setup.config['TOPOLOGIES']['GraphsDir'] + 'stream_graph_' + str(i) + '.gml')
    return


if __name__ == '__main__':
    main()