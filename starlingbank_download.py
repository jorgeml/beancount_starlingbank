#!/usr/bin/python3

from os import environ, path
from dotenv import load_dotenv
import json
import requests
import sys
import time
import getopt
from datetime import date, datetime, timedelta
from pathlib import Path

# Find .env file
basedir = path.abspath(path.dirname(__file__))
load_dotenv(path.join(basedir, ".env"))

# General Config
TOKEN_LIST = json.loads(environ["PERSONAL_ACCESS_TOKENS"])
data_folder = Path(environ.get("DATA_FOLDER"))
MAX_REQ_PS = 5


def get_accounts(token):
    time.sleep(1/MAX_REQ_PS)
    headers = {"Authorization": f"Bearer {token}"}
    params = {}
    accounts_request = requests.get(
        "https://api.starlingbank.com/api/v2/accounts", headers=headers, params=params
    )
    accounts_request.raise_for_status()
    return accounts_request.json()


def get_account_identifiers(account, token):
    time.sleep(1/MAX_REQ_PS)
    headers = {"Authorization": f"Bearer {token}", "account_id": account.get("id")}
    params = {}
    accountUid = account.get("accountUid")
    identifiers_request = requests.get(
        f"https://api.starlingbank.com/api/v2/accounts/{accountUid}/identifiers",
        headers=headers,
        params=params,
    )
    identifiers_request.raise_for_status()
    return identifiers_request.json()


def get_account_balance(account, token):
    time.sleep(1/MAX_REQ_PS)
    headers = {"Authorization": f"Bearer {token}", "account_id": account.get("id")}
    params = {}
    accountUid = account.get("accountUid")
    balance_request = requests.get(
        f"https://api.starlingbank.com/api/v2/accounts/{accountUid}/balance",
        headers=headers,
        params=params,
    )
    balance_request.raise_for_status()
    return balance_request.json()

def get_account_spaces(account, token):
    time.sleep(1/MAX_REQ_PS)
    headers = {"Authorization": f"Bearer {token}", "account_id": account.get("id")}
    params = {}
    accountUid = account.get("accountUid")
    spaces_request = requests.get(
        f"https://api.starlingbank.com/api/v2/account/{accountUid}/spaces",
        headers=headers,
        params=params,
    )
    spaces_request.raise_for_status()
    return spaces_request.json()

def get_account_transactions(account, token, fromdate):
    time.sleep(1/MAX_REQ_PS)
    headers = {"Authorization": f"Bearer {token}"}
    accountUid = account.get("accountUid")
    categoryUid = account.get("defaultCategory")
    params = {"changesSince": fromdate.strftime("%Y-%m-%dT%H:%M:%SZ")}
    transactions = {'feedItems':[]}
    transactions_request = requests.get(
        f"https://api.starlingbank.com/api/v2/feed/account/{accountUid}/category/{categoryUid}",
        headers=headers,
        params=params,
    )
    transactions_request.raise_for_status()
    transactions['feedItems'].extend(transactions_request.json().get("feedItems"))
    
    time.sleep(1/MAX_REQ_PS)
    spaces_request = requests.get(
        f"https://api.starlingbank.com/api/v2/account/{accountUid}/spaces",
        headers=headers,
        params=params,
    )
    spaces_request.raise_for_status()

    try:
        spaces_categories = [
        space["savingsGoalUid"] for space in spaces_request.json()["savingsGoals"]
        ]
    except KeyError:
        spaces_categories = []

    for space_category in spaces_categories:
        categoryUid = space_category
        time.sleep(1/MAX_REQ_PS)
        transactions_request = requests.get(
            f"https://api.starlingbank.com/api/v2/feed/account/{accountUid}/category/{categoryUid}",
            headers=headers,
            params=params,
        )
        transactions_request.raise_for_status()
        transactions['feedItems'].extend(transactions_request.json().get("feedItems"))

    return transactions


def get_account_payees(account, token):
    time.sleep(1/MAX_REQ_PS)
    headers = {"Authorization": f"Bearer {token}"}
    params = {}
    payees_request = requests.get(
        "https://api.starlingbank.com/api/v2/payees",
        headers=headers,
        params=params,
    )
    payees_request.raise_for_status()
    return payees_request.json().get("payees")


def main(argv):
    fromdate = datetime.now() - timedelta(90)
    try:
        opts, _ = getopt.getopt(argv, "hd:", "date=")
    except getopt.GetoptError:
        print("starling-download.py -d <date>")
        sys.exit(2)
    for opt, arg in opts:
        if opt == "-h":
            print("starling-download.py -d <date>")
            sys.exit()
        elif opt in ("-d", "--date"):
            fromdate = datetime.fromisoformat(arg)
    for token in TOKEN_LIST:
        accounts = get_accounts(token)
        for account in accounts.get("accounts"):
            entries = {}
            entries["account"] = account
            entries["identifiers"] = get_account_identifiers(account, token)
            entries["spaces"] = get_account_spaces(account, token)
            entries["transactions"] = get_account_transactions(account, token, fromdate)
            entries["balance"] = get_account_balance(account, token)
            entries["payees"] = get_account_payees(accounts, token)
            account_name = account.get("name")
            filename = data_folder / f"{date.today()}-starlingbank-{account_name}.json"
            with open(filename, "w") as json_file:
                json.dump(entries, json_file, indent=2)


if __name__ == "__main__":
    main(sys.argv[1:])


