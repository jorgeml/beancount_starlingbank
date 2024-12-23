"""Starling Bank JSON file importer

Parses a transaction feed downloaded from the Starling Bank API. Please refer to the documentation at: https://developer.starlingbank.com/docs#api-reference-temp

Thanks to Adam Gibbins <adam@adamgibbins.com> for his Monzo importer (https://github.com/adamgibbins/beancount-bits/blob/master/ingest/importers/monzo_debit.py) which I used as a reference.

"""
import datetime
import itertools
import json
import re
from os import path

from beancount.ingest import importer
from beancount.core import data, flags
from beancount.core.number import D
from beancount.utils.date_utils import parse_date_liberally

__author__ = "Jorge Martínez López <jorgeml@jorgeml.me>"
__license__ = "MIT"

VALID_STATUS = ["SETTLED", "REFUNDED", "ACCOUNT_CHECK"]


def get_account_id(file):
    if not re.match(r'.*\.json', path.basename(file.name)):
        return False

    with open(file.name) as data_file:
        try:
            account_data = json.load(data_file)["account"]
            if "accountUid" in account_data:
                return account_data["accountUid"]
            else:
                return False
        except KeyError:
            return False
        
def get_account_default_category(file):
    if not re.match(r'.*\.json', path.basename(file.name)):
        return False

    with open(file.name) as data_file:
        account_data = json.load(data_file)["account"]
        if "defaultCategory" in account_data:
            return account_data["defaultCategory"]
        else:
            return False

def get_transactions(file):
    if not re.match(r'.*\.json', path.basename(file.name)):
        return False

    with open(file.name) as data_file:
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


def get_payee_account(file, payeeUid, payeeAccountUid):
    with open(file.name) as data_file:
        payee_data = json.load(data_file)["payees"]
        for payee in payee_data:
            if payeeUid == payee["payeeUid"]:
                for account in payee["accounts"]:
                    if payeeAccountUid == account["payeeAccountUid"]:
                        return account
        return None

def get_category_name(file, categoryUid):
    with open(file.name) as data_file:
        spaces_data = json.load(data_file)["spaces"].get("savingsGoals")
        for space in spaces_data:
            if space["savingsGoalUid"] == categoryUid:
                return space["name"]
        return None

def get_balance(file):
    with open(file.name) as data_file:
        return json.load(data_file)["balance"]["totalClearedBalance"]


class Importer(importer.ImporterProtocol):
    def __init__(self, account_id, account):
        self.account_id = account_id
        self.account = account

    def name(self):
        return '{}: "{}"'.format(super().name(), self.account)

    def identify(self, file):
        identifier = get_account_id(file)
        return identifier == self.account_id

    def extract(self, file, existing_entries=None):
        entries = []
        counter = itertools.count()
        default_category = get_account_default_category(file)
        transactions = get_transactions(file)

        for transaction in reversed(transactions):

            if transaction["status"] not in VALID_STATUS:
                continue

            metadata = {}

            metadata["bank_id"] = transaction["feedItemUid"]

            if transaction["categoryUid"] != default_category:
                metadata["bank_category"] = transaction["categoryUid"]
                metadata["bank_space_name"] = get_category_name(file, transaction["categoryUid"])

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
                    file,
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

            meta = data.new_metadata(file.name, next(counter), metadata)

            date = parse_date_liberally(transaction["transactionTime"])
            price = get_unit_price(transaction)
            payee = transaction["counterPartyName"]

            if "counterPartySubEntityName" in transaction:
                name = transaction["counterPartySubEntityName"]
            else:
                name = None

            source = transaction["source"]

            narration = " / ".join(filter(None, [payee, name, reference, source]))

            postings = []
            unit = data.Amount(
                D(transaction["amount"]["minorUnits"]) / 100,
                transaction["amount"]["currency"],
            )

            if transaction["direction"] == "OUT":
                postings.append(
                    data.Posting(self.account, -unit, None, price, None, None)
                )
                if transaction["source"] == "INTERNAL_TRANSFER":
                    postings.append(
                        data.Posting(self.account, unit, None, price, None, None)
                    )    
            else:
                postings.append(
                    data.Posting(self.account, unit, None, price, None, None)
                )
                if transaction["source"] == "INTERNAL_TRANSFER":
                    postings.append(
                        data.Posting(self.account, -unit, None, price, None, None)
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

        balance = get_balance(file)
        balance_amount = data.Amount(
            D(balance.get("minorUnits")) / 100,
            balance.get("currency"),
        )

        meta = data.new_metadata(file.name, next(counter))

        balance_entry = data.Balance(
            meta, balance_date, self.account, balance_amount, None, None
        )

        entries.append(balance_entry)

        return data.sorted(entries)

    def file_account(self, file):
        return self.account

    def file_name(self, file):
        return f"starlingbank.{self.category_uid}.json"

    def file_date(self, file):
        transactions = get_transactions(file)
        return parse_date_liberally(transactions[0]["transactionTime"])



