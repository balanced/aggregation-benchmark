from __future__ import unicode_literals

from sqlalchemy.sql.expression import func

from .base import BaseAccountModel
from ..tables import Ledger
from ..utils import make_guid


class OriginalAccountModel(BaseAccountModel):
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
        self.amount(account)
        self.session.add(ledger)

    def amount(self, account):
        query = (
            self.session
            .query(func.sum(Ledger.amount))
            .filter(Ledger.account == account)
        )
        return query.scalar() or 0
