from __future__ import unicode_literals

from sqlalchemy.sql.expression import func

from .base import BaseAccountModel
from ..tables import Ledger
from ..tables import AccountAmount
from ..utils import make_guid


class MaterializedAccountModel(BaseAccountModel):
    def debit(self, account, amount):
        ledger = Ledger(
            guid=make_guid(),
            account=account,
            amount=amount,
        )
        self.session.add(ledger)

    def credit(self, account, amount):
        ledger = Ledger(
            guid=make_guid(),
            account=account,
            amount=amount,
        )
        # We need to check remaining amount here, before we credit to others
        self.session.query(AccountAmount).with_lockmode('update').one()
        self.amount(account)
        self.session.add(ledger)

    def amount(self, account):
        cache = self.session.query(AccountAmount).get(account.guid)
        query = (
            self.session
            .query(func.sum(Ledger.amount))
            .filter(Ledger.account == account)
        )
        if cache is None:
            last_amount = 0
        else:
            last_amount = cache.amount
            query = query.filter(Ledger.created_at > cache.updated_at)
        return (query.scalar() or 0) + last_amount

    def update_amount_cache(self, account, now=None):
        """Update amount cache

        """
        if now is None:
            now = self.session.query(func.clock_timestamp()).scalar()
        query = (
            self.session
            .query(func.sum(Ledger.amount))
            .filter(Ledger.account == account)
            .filter(Ledger.created_at <= now)
        )
        cache = self.session.query(AccountAmount).get(account.guid)
        if cache is None:
            cache = AccountAmount(
                account_guid=account.guid,
                amount=query.scalar(),
                updated_at=now,
            )
        else:
            cache.amount = query.scalar()
            cache.updated_at = now
        self.session.add(cache)
        return cache.amount
