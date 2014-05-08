from __future__ import unicode_literals

from sqlalchemy import Column
from sqlalchemy import Unicode
from sqlalchemy import Integer
from sqlalchemy import DateTime
from sqlalchemy import func
from sqlalchemy.orm import relationship
from sqlalchemy.schema import ForeignKey
from sqlalchemy.ext.declarative import declarative_base


DeclarativeBase = declarative_base()


class Account(DeclarativeBase):
    """Bank account

    """
    __tablename__ = 'account'

    guid = Column(Unicode(34), primary_key=True)
    amount = Column(Integer, nullable=False, default=0, server_default='0')

    ledgers = relationship(
        'Ledger',
        backref='account',
    )


class Ledger(DeclarativeBase):
    """Ledger for account

    """
    __tablename__ = 'ledger'

    guid = Column(Unicode(34), primary_key=True)
    account_guid = Column(
        Unicode(34),
        ForeignKey('account.guid'),
        index=True,
        nullable=False,
    )
    #: for scalar approach
    amount = Column(Integer)
    #: created datetime of this ledger
    created_at = Column(
        DateTime,
        nullable=False,
        default=func.clock_timestamp(),
        index=True,
    )


class AccountAmount(DeclarativeBase):
    """Amount cache for account

    """
    __tablename__ = 'account_amount'

    account_guid = Column(
        Unicode(34),
        ForeignKey('account.guid'),
        primary_key=True,
    )
    amount = Column(Integer)
    #: the latest updated DateTime of this account amount
    updated_at = Column(DateTime, nullable=False)
