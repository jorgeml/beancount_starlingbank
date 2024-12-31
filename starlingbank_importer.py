"""Starling Bank JSON file importer

Parses a transaction feed downloaded from the Starling Bank API. Please refer to the documentation at: https://developer.starlingbank.com/docs#api-reference-temp

Thanks to Adam Gibbins <adam@adamgibbins.com> for his Monzo importer (https://github.com/adamgibbins/beancount-bits/blob/master/ingest/importers/monzo_debit.py) which I used as a reference.

"""
import datetime
import itertools
import json
import re
import logging

from os import path

from beancount.core import account
from beancount.core import amount
from beancount.core import data
from beancount.core import flags
from beancount.core import position
from beancount.core.number import D
from beancount.core.number import ZERO

import beangulp
from beangulp import mimetypes
from beangulp.testing import main

__author__ = "Jorge Martínez López <jorgeml@jorgeml.me>"
__license__ = "MIT"

VALID_STATUS = ["SETTLED", "REFUNDED", "ACCOUNT_CHECK"]

class Importer(beangulp.Importer):
    """An importer for Starling Bank JSON files."""

    def __init__(self,
                 account_id,
                 account):
        self.account_id = account_id
        self.importer_account = account

    def identify(self, filepath):
        identifier = get_account_id(filepath)
        return identifier == self.account_id

    def filename(self, filepath):
        return 'starling.{}'.format(path.basename(filepath))

    def account(self, filepath):
        return self.importer_account

    def date(self, filepath):
        transactions = get_transactions(filepath)
        return parse_transaction_time(transactions[0]["transactionTime"])

    def extract(self, filepath, existing=None):
        entries = []
        counter = itertools.count()
        default_category = get_account_default_category(filepath)
        transactions = get_transactions(filepath)

        for transaction in reversed(transactions):

            if transaction["status"] not in VALID_STATUS:
                continue

            metadata = {}

            metadata["bank_id"] = transaction["feedItemUid"]

            if transaction["categoryUid"] != default_category:
                metadata["bank_category"] = transaction["categoryUid"]
                metadata["bank_space_name"] = get_category_name(filepath, transaction["categoryUid"])

            if "reference" in transaction:
                reference = transaction["reference"]
                metadata["bank_description"] = transaction["reference"]
            else:
                reference = None
            
            metadata["bank_created_date"] = transaction["transactionTime"]
            metadata["bank_settlement_date"] = transaction["settlementTime"]
            metadata["bank_updated_date"] = transaction["updatedAt"]

            if (
                "SENDER" in transaction["counterPartyType"]
                and "STARLING_PAY_STRIPE" not in transaction["source"]
            ):
                metadata["counterparty_sort_code"] = transaction[
                    "counterPartySubEntityIdentifier"
                ]
                metadata["counterparty_account_number"] = transaction[
                    "counterPartySubEntitySubIdentifier"
                ]
            elif "PAYEE" in transaction["counterPartyType"]:
                account = get_payee_account(
                    filepath,
                    transaction["counterPartyUid"],
                    transaction["counterPartySubEntityUid"],
                )
                if account:
                    metadata["counterparty_sort_code"] = account["bankIdentifier"]
                    metadata["counterparty_account_number"] = account[
                        "accountIdentifier"
                    ]
                    metadata["counterparty_account_description"] = account[
                        "description"
                    ]
            elif "CATEGORY" in transaction["counterPartyType"]:
                metadata["counterparty_type"] = transaction["counterPartyType"]
                metadata["counterparty_uid"] = transaction["counterPartyUid"]
                metadata["counterparty_name"] = transaction["counterPartyName"]

            meta = data.new_metadata(filepath, next(counter), metadata)

            date = parse_transaction_time(transaction["transactionTime"])
            price = get_unit_price(transaction)
            payee = transaction["counterPartyName"]

            if "counterPartySubEntityName" in transaction:
                name = transaction["counterPartySubEntityName"]
            else:
                name = None

            source = transaction["source"]

            narration = " / ".join(filter(None, [name, reference, source]))

            postings = []
            unit = data.Amount(
                D(transaction["amount"]["minorUnits"]) / 100,
                transaction["amount"]["currency"],
            )

            if transaction["direction"] == "OUT":
                postings.append(
                    data.Posting(self.importer_account, -unit, None, price, None, None)
                )
                if transaction["source"] == "INTERNAL_TRANSFER":
                    postings.append(
                        data.Posting(self.importer_account, unit, None, price, None, None)
                    )    
            else:
                postings.append(
                    data.Posting(self.importer_account, unit, None, price, None, None)
                )
                if transaction["source"] == "INTERNAL_TRANSFER":
                    postings.append(
                        data.Posting(self.importer_account, -unit, None, price, None, None)
                    )    

            link = set()

            entries.append(
                data.Transaction(
                    meta, date, flags.FLAG_OKAY, payee, narration, set(), link, postings
                )
            )

        balance_date = datetime.date.today()
        try: 
            balance_date = entries[-1].date
        except IndexError:
            pass
        
        balance_date += datetime.timedelta(days=1)

        balance = get_balance(filepath)

        balance_amount = amount.Amount(
            D(balance.get("minorUnits")) / 100,
            balance.get("currency"),
        )
        
        meta = data.new_metadata(filepath, next(counter))

        balance_entry = data.Balance(
            meta, balance_date, self.importer_account, balance_amount, None, None
        )

        entries.append(balance_entry)

        return data.sorted(entries)


def get_account_id(filepath):
    mimetype, encoding = mimetypes.guess_type(filepath)
    if mimetype != 'application/json':
        return False

    with open(filepath) as data_file:
        try:
            account_data = json.load(data_file)["account"]
            if "accountUid" in account_data:
                return account_data["accountUid"]
            else:
                return False
        except KeyError:
            return False
        
def get_account_default_category(filepath):
    mimetype, encoding = mimetypes.guess_type(filepath)
    if mimetype != 'application/json':
        return False

    with open(filepath) as data_file:
        account_data = json.load(data_file)["account"]
        if "defaultCategory" in account_data:
            return account_data["defaultCategory"]
        else:
            return False

def get_balance_date(filepath):
    mimetype, encoding = mimetypes.guess_type(filepath)
    if mimetype != 'application/json':
        return False

    with open(filepath) as data_file:
        account_data = json.load(data_file)["account"]
        if "createdAt" in account_data:
            return account_data["createdAt"]
        else:
            return False

def get_transactions(filepath):
    mimetype, encoding = mimetypes.guess_type(filepath)
    if mimetype != 'application/json':
        return False

    with open(filepath) as data_file:
        transaction_data = json.load(data_file)["transactions"]
        if "feedItems" in transaction_data:
            return transaction_data["feedItems"]
        else:
            return False


def get_unit_price(transaction):
    if (
        transaction["sourceAmount"]["currency"] != transaction["amount"]["currency"]
        and transaction["sourceAmount"]["minorUnits"] != 0
    ):
        total_local_amount = D(transaction["amount"]["minorUnits"])
        total_foreign_amount = D(transaction["sourceAmount"]["minorUnits"])
        # all prices need to be positive
        unit_price = round(abs(total_foreign_amount / total_local_amount), 5)
        return data.Amount(unit_price, transaction["sourceAmount"]["currency"])
    else:
        return None


def get_payee_account(filepath, payeeUid, payeeAccountUid):
    with open(filepath) as data_file:
        payee_data = json.load(data_file)["payees"]
        for payee in payee_data:
            if payeeUid == payee["payeeUid"]:
                for account in payee["accounts"]:
                    if payeeAccountUid == account["payeeAccountUid"]:
                        return account
        return None

def get_category_name(filepath, categoryUid):
    with open(filepath) as data_file:
        spaces_data = json.load(data_file)["spaces"].get("savingsGoals")
        for space in spaces_data:
            if space["savingsGoalUid"] == categoryUid:
                return space["name"]
        return None

def get_balance(filepath):
    with open(filepath) as data_file:
        return json.load(data_file)["balance"]["totalClearedBalance"]

def parse_transaction_time(date_str):
    """Parse a time string and return a datetime object.

    Args:
      date_str: A string, the date to be parsed, in ISO format.
    Returns:
      A datetime.date() instance.
    """
    timestamp = datetime.datetime.fromisoformat(date_str)
    return timestamp.date()

