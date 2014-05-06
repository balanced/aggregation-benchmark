from __future__ import unicode_literals


class BaseAccountModel(object):
    """A bank account model

    """

    def __init__(self, session):
        self.session = session

    def debit(self, account, amount):
        """Debit a credit card or whatever it is (add the amount to the bank)
        account

        """
        raise NotImplemented

    def credit(self, account, amount):
        """Credit to other funding instrument (subtrace amount from this
        account)

        """
        raise NotImplemented

    def amount(self, account):
        """The aggregated balance amount in the given account

        """
        raise NotImplemented
