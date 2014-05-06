from __future__ import unicode_literals
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker

from . import tables
from .utils import make_guid
from .accounts.original import OriginalAccountModel


def main():
    db_url = os.environ.get('TARGET_DB', 'sqlite:///')
    engine = create_engine(db_url, convert_unicode=True)
    tables.DeclarativeBase.metadata.bind = engine
    tables.DeclarativeBase.metadata.create_all()

    DBSession = scoped_session(sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine,
    ))
    session = DBSession()

    account = tables.Account(guid=make_guid())
    model = OriginalAccountModel(session)
    for i in range(1000):
        model.debit(account, i * 999)

if __name__ == '__main__':
    main()
