from __future__ import unicode_literals

from .base import BaseAccountModel
from ..tables import Ledger
from ..utils import make_guid


class ScalarsAccountModel(BaseAccountModel):
    def debit(self, account, amount):
        ledger = Ledger(
            guid=make_guid(),
            account=account,
            amount=amount,
        )
        account.amount += amount
        self.session.add(ledger)

    def credit(self, account, amount):
        ledger = Ledger(
            guid=make_guid(),
            account=account,
            amount=amount,
        )
        account.amount += amount
        self.session.add(ledger)

    def amount(self, account):
        return account.amount
