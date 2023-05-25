"""
Configure RabbitMQ queues and exchanges
from JSON description of pipeline.

Reads a list: ["producer", "filter", "consumer"]
And creates queues w/ direct exchanges:
        exchange: producer-output > queue: filter-input
        exchange: filter-output > queue: consumer-input

Fanout exchanges are represented by a list,
And must only appear as the last element of a list.
"""

import argparse
from enum import Enum
import json
import logging
import os
import sys
from typing import Any, Dict

# PyPI
import pika
from rabbitmq_admin import AdminAPI

# local:
from pipeline.worker import DEFAULT_ROUTING_KEY

logger = logging.getLogger(__name__)

# using Queue, Exchange, Binding classes to represent desired configuration
# to allow better type checking
class Queue:
    def __init__(self, name: str):
        self.name = name
        logger.debug(f"create queue {name}")

    def __repr__(self):
        return f"<Queue: {self.name}>"

class EType:
    DIRECT = 'direct'
    FANOUT = 'fanout'

class Exchange:
    def __init__(self, name: str, type):
        logger.debug(f"create {type} exchange {name}")
        self.name = name
        self.type = type
        self.bindings = []      # list of dest queue names

    def __repr__(self):
        return f"<Exchange: {self.type} {self.name}>"

    def add_binding(self, dest: Queue):
        assert isinstance(dest, Queue)
        logger.debug(f"bind queue {dest.name} to exchange {self.name}")
        # XXX modify dest.source(s)??
        self.bindings.append(dest)

class BDType:                   # binding dest type
    QUEUE = 'queue'
    EXCH = 'exchange'

class Binding:
    def __init__(self, dest: str, source: str, dtype: BDType = BDType.QUEUE):
        self.dest = dest        # ie; queue name
        self.source = source    # exchange name
        self.dtype = dtype      # dest type

    def __repr__(self):
        return f"<Binding: {self.source} -> {self.dtype} {self.dest}>"


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def fatal(message):
    logger.fatal(message)
    sys.exit(1)

class Plumbing:
    def __init__(self, filename):
        
        self.queues = {}
        self.exchanges = {}
        self.processes = []     # was originally set, but want ordering
        self.bindings = []      # list of (queuename, exchangename)

        with open(filename) as f:
            j = json.load(f)
        self.process_json(None, j)

    def add_process(self, name: str):
        if name in self.processes:
            fatal(f"duplicate process: {name}")
        logger.debug(f"add process {name}")
        self.processes.append(name)

    def add_queue(self, name: str):
        if name in self.queues:
            fatal(f"duplicate queue: {name}")
        q = Queue(name)
        self.queues[name] = q
        return q

    def add_exchange(self, name: str, type: EType):
        if name in self.exchanges:
            fatal(f"duplicate exchange: {name}")
        e = Exchange(name, type)
        self.exchanges[name] = e
        return e

    def process_json(self, prev_exch, j):
        saw_fanout = False
        first = True
        logger.debug(f"process_json pe {prev_exch} {j}")

        if not isinstance(j, list):
            fatal(f"expected list: {j}")

        if len(j) == 0:
            fatal("empty list")

        if not isinstance(j[0], str):
            # can bind exchanges to exchanges, but keep simple for starters
            fatal(f"first element must be a process name: {elt}")

        prev_name = None
        for elt in j:
            if saw_fanout:
                fatal(f"fanout must be last element in pipeline, saw: {elt}")

            if isinstance(elt, list): # list of sub-pipes
                if not prev_name:
                    fatal(f"fanout must not be first element in pipeline: {elt}")
                saw_fanout = True

                # create fanout output exchange for prev process:
                exch = self.add_exchange(prev_name + '-out', EType.FANOUT)
                for sub_pipe in elt:
                    self.process_json(exch, sub_pipe)
            elif isinstance(elt, str):
                # here with str
                self.add_process(elt)

                if prev_name:
                    # create direct output exchange for previous process:
                    prev_out = prev_name + '-out'
                    prev_exch = self.add_exchange(prev_out, EType.DIRECT)

                if prev_exch:
                    # bind current process input queue:
                    inq_name = elt + '-in'
                    q = self.add_queue(inq_name)
                    prev_exch.add_binding(q)
                    self.bindings.append( Binding(inq_name, prev_exch.name) )
            else:
                fatal(f"expected name or list: {elt}")

            prev_name = elt


def get_definitions(par: pika.connection.URLParameters) -> Dict[str, Any]:
    """
    use rabbitmq_admin package to get server config via RabbitMQ admin API.
    takes pika (AMQP) parsed URL for connection params
    """
    creds = par.credentials
    port = 15672                # par.port + 10000???
    api = AdminAPI(url=f'http://{par.host}:{port}', auth=(creds.username, creds.password))
    return api.get_definitions()

def listify(l):
    return ", ".join(l)

COMMANDS = ['configure', 'delete', 'dump', 'show', 'trim']
def main():
    # XXX use rss-fetcher logargparse???
    ap = argparse.ArgumentParser("configure", "configure RabbitMQ queues")

    amqp_url = os.environ.get('RABBITMQ_URL')
    ap.add_argument('--rabbitmq-url', '-U', dest='amqp_url',
                    default=amqp_url,
                    help="set RabbitMQ URL (default {amqp_url}")

    def_file = 'plumbing.json'
    ap.add_argument('--plumbing-file', '-f', dest='plumbing_file',
                    default=def_file,
                    help="JSON plumbing file (default {def_file})")

    ap.add_argument('command', nargs=1, type=str,
                    choices=COMMANDS,
                    help='Command, one of: {listify(COMMANDS).')

    # parse command line:
    args = ap.parse_args()

    command = args.command[0]
    if command != 'show':
        p = Plumbing(args.plumbing_file)  # parse plumbing file
    else:
        p = None

    if command == 'dump':
        print(f"{args.plumbing_file}:")
        print("    processes", listify(p.processes))
        print("    queues", listify(p.queues.keys()))
        print("    exchanges", listify(p.exchanges.keys()))
        print("    bindings", p.bindings)
        sys.exit(0)

    par = pika.connection.URLParameters(args.amqp_url)

    # use RabbitMQ admin API to get current config
    # (not available via AMQP):
    if command != 'configure':
        defns = get_definitions(par)
    else:
        defns = {}

    if command == 'show':
        curr_queues = [q['name'] for q in defns['queues']]
        curr_exchanges = [(e['name'], e['type']) for e in defns['exchanges']]
        curr_bindings = []      # XXX
        print("RabbitMQ current:")
        print("    queues", curr_queues)
        print("    exchanges", curr_exchanges)
        print("    bindings", defns['bindings'])
        sys.exit(0)

    # blindly configure for now:
    logging.getLogger("pika").setLevel(logging.WARNING) # reduce blather

    # use AMQP to (de)configure: 
    with pika.BlockingConnection(par) as conn:
        chan = conn.channel()

        if command == 'configure':

            for qname in p.queues.keys():
                # durable == messages stored on disk
                chan.queue_declare(qname, durable=True)

            for exch in p.exchanges.values():
                chan.exchange_declare(exch.name, exch.type)

            for b in p.bindings:
                # XXX check b.dtype!!
                chan.queue_bind(b.dest, b.source,
                                routing_key=DEFAULT_ROUTING_KEY)

        elif command == 'delete':
            print("deleting...")

            for qname in p.queues.keys():
                chan.queue_delete(qname)

            for exch in p.exchanges.values():
                chan.exchange_delete(exch.name)

            for b in p.bindings:
                chan.queue_unbind(b.dest, exchange=b.source,
                                  routing_key=DEFAULT_ROUTING_KEY)
        elif command == 'trim':
            print("trim not yet implemented")
            # iterate over items in defns, and remove those not in config
            sys.exit(1)
        else:
            print(f"unknown command {command}")
            sys.exit(1)

if __name__ == '__main__':
    main()
