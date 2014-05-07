from __future__ import unicode_literals
import os
import argparse
import random
import logging
import time
import multiprocessing

import zmq

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


def model_factory(session, model_type):
    if model_type == 'original':
        model = OriginalAccountModel(session)
    elif model_type == 'scalar':
        model = ScalarAccountModel(session)
    else:
        raise ValueError('Unknown model {}'.format(model_type))
    return model


def worker(endpoint, model_type):
    logger = logging.getLogger(__name__)
    logger.info('Worker PID: %s', os.getpid())

    DBSession = init_db_session()

    logger.info('Connecting to %s', endpoint)
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.connect(endpoint)
    while True:
        cmds = socket.recv_multipart()
        cmd, account_guid = cmds
        session = DBSession()
        model = model_factory(session, model_type)
        account = session.query(tables.Account).get(account_guid)
        logger.info('Run command %s on %s', cmd, account_guid)

        begin = time.time()
        if cmd == 'debit':
            model.debit(account, random.randint(1, 65536))
            session.commit()
        elif cmd == 'credit:':
            model.debit(account, -random.randint(1, 65536))
            session.commit()
        elif cmd == 'amount':
            model.amount(account)
            session.commit()
        else:
            break
        session.remove()
        end = time.time()
        elapsed = end - begin
        socket.send_multipart(cmds + [str(elapsed)])


def main():
    logger = logging.getLogger(__name__)
    parser = argparse.ArgumentParser(description='Aggregation benchmark.')
    parser.add_argument('model', metavar='MODEL', type=str, nargs=1,
                        help='which model to run (original, scalar or materialized)')
    parser.add_argument('--init-debits', dest='init_debits', action='store',
                        default=1000,
                        help='initial debits count (default: 1,000)')
    parser.add_argument('--init-credits', dest='init_credits', action='store',
                        default=1000,
                        help='initial debits count (default: 1,000)')
    parser.add_argument('--concurrent', dest='concurrent', type=int, action='store',
                        default=8,
                        help='concurrent level (default: 8)')

    args = parser.parse_args()

    DBSession = init_db_session()
    session = DBSession()

    logger.info('Model type %s', args.model[0])
    account = tables.Account(guid=make_guid())

    logger.info('Running initial work load %s debits', args.init_debits)
    model = model_factory(session, args.model[0])
    # run initial work load here
    for _ in xrange(0, args.init_debits):
        model.debit(account, random.randint(1, 65536))
        session.commit()
    logger.info('Running initial work load %s credits', args.init_credits)
    for _ in xrange(0, args.init_debits):
        model.debit(account, -random.randint(1, 65536))
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
    socket = context.socket(zmq.REQ)
    socket.bind(endpoint)

    # submit requests
    # TODO:
    for i in xrange(100):
        socket.send_multipart([random.choice([b'amount', b'debit', b'credit']), str(account.guid)])
        resp = socket.recv_multipart()
        _, _, elapsed = resp
        logger.info('Elapsed %s', float(elapsed))
    socket.send_multipart(['exit', ''])


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
