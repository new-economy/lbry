import json
import logging
import argparse
from urllib import urlopen
from twisted.internet import reactor, defer
from twisted.web import resource
from twisted.web.server import Site
from lbrynet import conf
from lbrynet.dht import constants
from lbrynet.dht.node import Node
from lbrynet.dht.error import TransportNotConnected
from lbrynet.core.utils import generate_id
from lbrynet.core.log_support import configure_console, configure_twisted
from lbrynet.daemon.auth.server import AuthJSONRPCServer
# configure_twisted()
conf.initialize_settings()
configure_console()
lbrynet_handler = logging.getLogger("lbrynet").handlers[0]
log = logging.getLogger("dht router")
log.addHandler(lbrynet_handler)
log.setLevel(logging.INFO)


def get_external_ip():
    response = json.loads(urlopen("https://api.lbry.io/ip").read())
    if not response['success']:
        raise ValueError("failed to get external ip")
    return response['data']['ip']


class MultiSeedRPCServer(AuthJSONRPCServer):
    def __init__(self, starting_node_port=4455, nodes=50, rpc_port=5280):
        AuthJSONRPCServer.__init__(self, False)
        self.port = None
        self.rpc_port = rpc_port
        self.external_ip = get_external_ip()
        self._nodes = [Node(node_id=generate_id(), udpPort=starting_node_port+i, externalIP=self.external_ip)
                       for i in range(nodes)]
        self._own_addresses = [(self.external_ip, starting_node_port+i) for i in range(nodes)]
        reactor.addSystemEventTrigger('after', 'startup', self.start)

    @defer.inlineCallbacks
    def start(self):
        self.announced_startup = True
        root = resource.Resource()
        root.putChild('', self)
        self.port = reactor.listenTCP(self.rpc_port, Site(root), interface='localhost')
        log.info("starting %i nodes on %s, rpc available on localhost:%i", len(self._nodes), self.external_ip, self.rpc_port)

        for node in self._nodes:
            node.start_listening()
            yield node._protocol._listening

        for node1 in self._nodes:
            for node2 in self._nodes:
                if node1 is node2:
                    continue
                try:
                    yield node1.addContact(node1.contact_manager.make_contact(node2.node_id, node2.externalIP,
                                                                              node2.port, node1._protocol))
                except TransportNotConnected:
                    pass
            node1.safe_start_looping_call(node1._change_token_lc, constants.tokenSecretChangeInterval)
            node1.safe_start_looping_call(node1._refresh_node_lc, constants.checkRefreshInterval)
            node1._join_deferred = defer.succeed(True)
        reactor.addSystemEventTrigger('before', 'shutdown', self.stop)
        log.info("finished bootstrapping the network, running %i nodes", len(self._nodes))

    @defer.inlineCallbacks
    def stop(self):
        yield self.port.stopListening()
        yield defer.DeferredList([node.stop() for node in self._nodes])

    def jsonrpc_get_node_ids(self):
        return defer.succeed([node.node_id.encode('hex') for node in self._nodes])

    def jsonrpc_node_routing_table(self, node_id):
        def format_contact(contact):
            return {
                "node_id": contact.id.encode('hex'),
                "address": contact.address,
                "port": contact.port,
                "lastReplied": contact.lastReplied,
                "lastRequested": contact.lastRequested,
                "failedRPCs": contact.failedRPCs
            }

        def format_bucket(bucket):
            return {
                "contacts": [format_contact(contact) for contact in bucket._contacts],
                "lastAccessed": bucket.lastAccessed
            }

        def format_routing(node):
            return {
                i: format_bucket(bucket) for i, bucket in enumerate(node._routingTable._buckets)
            }

        for node in self._nodes:
            if node.node_id == node_id.decode('hex'):
                return defer.succeed(format_routing(node))

    def jsonrpc_restart_node(self, node_id):
        for node in self._nodes:
            if node.node_id == node_id.decode('hex'):
                d = node.stop()
                d.addCallback(lambda _: node.start(self._own_addresses))
                return d

    @defer.inlineCallbacks
    def jsonrpc_local_node_rpc(self, from_node, query, args=()):
        def format_result(response):
            if isinstance(response, list):
                return [[node_id.encode('hex'), address, port] for (node_id, address, port) in response]
            if isinstance(response, dict):
                return {'token': response['token'].encode('hex'), 'contacts': format_result(response['contacts'])}
            return response

        for node in self._nodes:
            if node.node_id == from_node.decode('hex'):
                fn = getattr(node, query)
                self_contact = node.contact_manager.make_contact(node.node_id, node.externalIP, node.port, node._protocol)
                if args:
                    args = (str(arg) if isinstance(arg, (str, unicode)) else int(arg) for arg in args)
                    result = yield fn(self_contact, *args)
                else:
                    result = yield fn()
                # print "result: %s" % result
                defer.returnValue(format_result(result))

    @defer.inlineCallbacks
    def jsonrpc_node_rpc(self, from_node, to_node, query, args=()):
        def format_result(response):
            if isinstance(response, list):
                return [[node_id.encode('hex'), address, port] for (node_id, address, port) in response]
            if isinstance(response, dict):
                return {'token': response['token'].encode('hex'), 'contacts': format_result(response['contacts'])}
            return response

        for node in self._nodes:
            if node.node_id == from_node.decode('hex'):
                remote = node._routingTable.getContact(to_node.decode('hex'))
                fn = getattr(remote, query)
                if args:
                    args = (str(arg).decode('hex') for arg in args)
                    result = yield fn(*args)
                else:
                    result = yield fn()
                defer.returnValue(format_result(result))

    @defer.inlineCallbacks
    def jsonrpc_get_nodes_who_know(self, ip_address):
        nodes = []
        for node_id in [n.node_id.encode('hex') for n in self._nodes]:
            routing_info = yield self.jsonrpc_node_routing_table(node_id=node_id)
            for index, bucket in routing_info.iteritems():
                if ip_address in map(lambda c: c['address'], bucket['contacts']):
                    nodes.append(node_id)
                    break
        defer.returnValue(nodes)

    def jsonrpc_node_status(self):
        return defer.succeed({
            node.node_id.encode('hex'): node._join_deferred is not None and node._join_deferred.called
            for node in self._nodes
        })


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--rpc_port', default=5280)
    parser.add_argument('--starting_port', default=4455)
    parser.add_argument('--nodes', default=50)
    args = parser.parse_args()
    MultiSeedRPCServer(int(args.starting_port), int(args.nodes), int(args.rpc_port))
    reactor.run()


if __name__ == "__main__":
    main()
