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
TOKEN = environ.get("PERSONAL_ACCESS_TOKEN")
data_folder = Path(environ.get("DATA_FOLDER"))

def get_accounts(token):
    headers = {"Authorization": f"Bearer {token}"}
    params = {}
    r = requests.get("https://api.starlingbank.com/api/v2/accounts", headers=headers, params=params)
    r.raise_for_status()
    return r.json()


def get_accounts_balance(accounts, token):
    for account in accounts.get("accounts"):
        headers = {"Authorization": f"Bearer {token}", "account_id": account.get("id")}
        params = {}
        accountUid = account.get("accountUid")
        r = requests.get(
            f"https://api.starlingbank.com/api/v2/accounts/{accountUid}/balance", headers=headers, params=params
        )
        r.raise_for_status()
        balance = r.json().get("amount").get("minorUnits")/100
        currency = r.json().get("amount").get("currency")
        r = requests.get(
            f"https://api.starlingbank.com/api/v2/accounts/{accountUid}/identifiers", headers=headers, params=params
        )
        r.raise_for_status()
        account_name = account.get("name")
        sort_code = r.json().get("bankIdentifier")
        account_number = r.json().get("accountIdentifier")
        print(f"{sort_code} {account_number} {account_name}: {balance} {currency}")
    return

def get_accounts_transactions(accounts, token, fromdate):
    for account in accounts.get("accounts"):
        headers = {"Authorization": f"Bearer {token}"}
        accountUid = account.get("accountUid")
        categoryUid = account.get("defaultCategory")
        params = {"changesSince": fromdate.strftime('%Y-%m-%dT%H:%M:%SZ')}
        r = requests.get(
            f"https://api.starlingbank.com/api/v2/feed/account/{accountUid}/category/{categoryUid}", headers=headers, params=params
        )
        r.raise_for_status()
        account_name = account.get("name")
        filename = data_folder / f"{date.today()}-starlingbank-{account_name}.json"
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
    print("## Getting accounts")
    accounts = get_accounts(TOKEN)
    print("## Getting balances")
    get_accounts_balance(accounts, TOKEN)
    print("## Getting transactions")
    get_accounts_transactions(accounts, TOKEN, fromdate)


if __name__ == "__main__":
    main(sys.argv[1:])
