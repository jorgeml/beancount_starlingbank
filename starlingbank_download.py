#!/usr/bin/python

from os import environ, path
from dotenv import load_dotenv
import json
import requests
import sys
import getopt
from datetime import date, datetime, timedelta
from pathlib import Path

# Find .env file
basedir = path.abspath(path.dirname(__file__))
load_dotenv(path.join(basedir, ".env"))

# General Config
TOKEN_LIST = json.loads(environ["PERSONAL_ACCESS_TOKENS"])
data_folder = Path(environ.get("DATA_FOLDER"))


def get_accounts(token):
    headers = {"Authorization": f"Bearer {token}"}
    params = {}
    r = requests.get(
        "https://api.starlingbank.com/api/v2/accounts", headers=headers, params=params
    )
    r.raise_for_status()
    return r.json()


def get_accounts_balance(accounts, token):
    for account in accounts.get("accounts"):
        headers = {"Authorization": f"Bearer {token}", "account_id": account.get("id")}
        params = {}
        accountUid = account.get("accountUid")
        categoryUid = account.get("defaultCategory")
        balance_request = requests.get(
            f"https://api.starlingbank.com/api/v2/accounts/{accountUid}/balance",
            headers=headers,
            params=params,
        )
        balance_request.raise_for_status()
        balance = balance_request.json().get("amount").get("minorUnits") / 100
        filename = data_folder / f"starlingbank-balance-{categoryUid}.json"
        with open(filename, "w") as json_file:
            json.dump(balance_request.json(), json_file, indent=2)
        currency = balance_request.json().get("amount").get("currency")
        identifier_request = requests.get(
            f"https://api.starlingbank.com/api/v2/accounts/{accountUid}/identifiers",
            headers=headers,
            params=params,
        )
        identifier_request.raise_for_status()
        account_name = account.get("name")
        sort_code = identifier_request.json().get("bankIdentifier")
        account_number = identifier_request.json().get("accountIdentifier")
        print(f"{sort_code} {account_number} {account_name}: {balance} {currency}")
    return


def get_accounts_transactions(accounts, token, fromdate):
    for account in accounts.get("accounts"):
        headers = {"Authorization": f"Bearer {token}"}
        accountUid = account.get("accountUid")
        categoryUid = account.get("defaultCategory")
        params = {"changesSince": fromdate.strftime("%Y-%m-%dT%H:%M:%SZ")}
        r = requests.get(
            f"https://api.starlingbank.com/api/v2/feed/account/{accountUid}/category/{categoryUid}",
            headers=headers,
            params=params,
        )
        r.raise_for_status()
        account_name = account.get("name")
        filename = data_folder / f"{date.today()}-starlingbank-{account_name}.json"
        with open(filename, "w") as json_file:
            json.dump(r.json(), json_file, indent=2)
    return


def get_account_payees(accounts, token):
    for account in accounts.get("accounts"):
        headers = {"Authorization": f"Bearer {token}"}
        categoryUid = account.get("defaultCategory")
        params = {}
        r = requests.get(
            "https://api.starlingbank.com/api/v2/payees",
            headers=headers,
            params=params,
        )
        r.raise_for_status()
        account_name = account.get("name")
        filename = data_folder / f"starlingbank-payees-{categoryUid}.json"
        with open(filename, "w") as json_file:
            json.dump(r.json(), json_file, indent=2)
    return


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
        print("## Getting account")
        accounts = get_accounts(token)
        print("## Getting balance")
        get_accounts_balance(accounts, token)
        print("## Getting transactions")
        get_accounts_transactions(accounts, token, fromdate)
        print("## Getting payees")
        get_account_payees(accounts, token)


if __name__ == "__main__":
    main(sys.argv[1:])
