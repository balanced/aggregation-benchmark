from __future__ import unicode_literals
import os
import argparse
import random
import logging
import time
import multiprocessing

import zmq
import newrelic.agent
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker

from . import tables
from .utils import make_guid
from .accounts.original import OriginalAccountModel
from .accounts.scalar import ScalarAccountModel


def init_db_session():
    db_url = os.environ.get('TARGET_DB', 'sqlite:///')
    engine = create_engine(db_url, convert_unicode=True)
    tables.DeclarativeBase.metadata.bind = engine
    tables.DeclarativeBase.metadata.create_all()

    DBSession = scoped_session(sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine,
    ))
    return DBSession


def init_newrelic():
    logger = logging.getLogger(__name__)
    if os.environ.get('NEWRELIC_FILE') is not None:
        logger.info('Initialize newrelic from %s', os.environ['NEWRELIC_FILE'])
        newrelic.agent.initialize(os.environ['NEWRELIC_FILE'])


def model_factory(session, model_type):
    if model_type == 'original':
        model = OriginalAccountModel(session)
    elif model_type == 'scalar':
        model = ScalarAccountModel(session)
    else:
        raise ValueError('Unknown model {}'.format(model_type))
    return model


@newrelic.agent.function_trace()
def process_req(model, session, account, cmd):
    if cmd == 'debit':
        model.debit(account, random.randint(1, 65536))
        session.commit()
    elif cmd == 'credit:':
        model.debit(account, -random.randint(1, 65536))
        session.commit()
    elif cmd == 'amount':
        model.amount(account)
        session.commit()


def worker(endpoint, model_type):
    logger = logging.getLogger(__name__)
    logger.info('Worker PID: %s', os.getpid())

    init_newrelic()
    newrelic_app = newrelic.agent.application()
    DBSession = init_db_session()

    logger.info('Connecting to %s', endpoint)
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.connect(endpoint)
    while True:
        cmds = socket.recv_multipart()
        cmd, account_guid = cmds
        if cmd == 'exit':
            logger.info('Exit PID: %s', os.getpid())
            socket.send_multipart([b''])
            break
        session = DBSession()
        model = model_factory(session, model_type)
        account = session.query(tables.Account).get(account_guid)
        logger.debug('Run command %s on %s', cmd, account_guid)

        begin = time.time()
        with newrelic.agent.BackgroundTask(
            newrelic_app, name=cmd, group='benchmark'
        ):
            process_req(model, session, account, cmd)
        end = time.time()
        DBSession.remove()
        socket.send_multipart(cmds + [str(begin), str(end)])


def main():
    logger = logging.getLogger(__name__)
    parser = argparse.ArgumentParser(description='Aggregation benchmark.')
    parser.add_argument('model', metavar='MODEL', type=str, nargs=1,
                        help='which model to run (original, scalar or materialized)')
    parser.add_argument('--init-debits', dest='init_debits', action='store',
                        default=1000, type=int,
                        help='initial debits count (default: 1,000)')
    parser.add_argument('--init-credits', dest='init_credits', action='store',
                        default=1000, type=int,
                        help='initial debits count (default: 1,000)')
    parser.add_argument('--concurrent', dest='concurrent', type=int, action='store',
                        default=8,
                        help='concurrent level (default: 8)')
    parser.add_argument('--sample', dest='sample', type=int, action='store',
                        default=1000,
                        help='number of sample to send (default: 1000)')

    args = parser.parse_args()

    init_newrelic()
    DBSession = init_db_session()
    session = DBSession()

    logger.info('Model type %s', args.model[0])
    account = tables.Account(guid=make_guid())
    session.commit()

    # run benchmark here
    logger.info('Running benchmark on concurrent level %s', args.concurrent)
    endpoint = 'ipc:///tmp/benchmark'
    proces = []
    # init workers
    for _ in xrange(args.concurrent):
        proc = multiprocessing.Process(target=worker, args=(endpoint, args.model[0]))
        proces.append(proc)
        proc.start()
    context = zmq.Context()
    socket = context.socket(zmq.XREQ)
    socket.bind(endpoint)

    time.sleep(1)

    poller = zmq.Poller()
    poller.register(socket)

    def load_reqs(number, req_type):
        to_send = number
        to_receive = number
        while to_send > 0 or to_receive > 0:
            sockets = dict(poller.poll(0.1))
            if socket not in sockets:
                continue
            # good to send
            if sockets[socket] & zmq.POLLOUT and to_send:
                to_send -= 1
                socket.send_multipart([b'', req_type, str(account.guid)])
                logger.debug('Send %s', req_type)
            # good to receive
            if sockets[socket] & zmq.POLLIN:
                to_receive -= 1
                # simply receive and dump
                socket.recv_multipart()

    logger.info('Running initial work load %s debits', args.init_debits)
    load_reqs(args.init_debits, b'debit')

    logger.info('Running initial work load %s credits', args.init_credits)
    load_reqs(args.init_credits, b'credit')

    def generate_requests(number):
        for _ in xrange(number):
            yield random.choice([b'amount', b'debit', b'credit'])

    requests = generate_requests(args.sample)
    request_empty = False
    result_count = 0
    begin = time.time()
    last_update = None
    while result_count < args.sample:
        now = time.time()
        if last_update is None or (now - last_update) >= 5.0:
            last_update = now
            logger.info(
                'Progress %s / %s (%02d %%)',
                result_count,
                args.sample,
                (result_count / float(args.sample)) * 100
            )

        sockets = dict(poller.poll(0.1))
        if socket not in sockets:
            continue
        # good to send
        if sockets[socket] & zmq.POLLOUT and not request_empty:
            try:
                req = requests.next()
            except StopIteration:
                request_empty = True
            if not request_empty:
                socket.send_multipart([b'', req, str(account.guid)])
                logger.debug('Send %s', req)
        # good to receive
        if sockets[socket] & zmq.POLLIN:
            resp = socket.recv_multipart()
            _, cmd, _, begin, end = resp
            logger.debug('Elapsed %s', float(end) - float(begin))
            result_count += 1
            print cmd, begin, end

    for _ in xrange(args.concurrent):
        socket.send_multipart([b'', b'exit', b''])
        socket.recv_multipart()
    for proc in proces:
        proc.join()
    logger.info('done.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
