from __future__ import unicode_literals

from sqlalchemy.sql.expression import func

from .base import BaseAccountModel
from ..tables import Ledger


class OriginalAccountModel(BaseAccountModel):
    def debit(self, account, amount):
        ledger = Ledger(
            account=account,
            amount=amount,
        )
        self.session.add(ledger)

    def credit(self, account, amount):
        ledger = Ledger(
            account=account,
            amount=amount,
        )
        self.session.add(ledger)

    def amount(self, account):
        query = (
            self.session
            .quey(func.sum(Ledger.amount))
            .filter(Ledger.account == account)
        )
        return query.scalar()
