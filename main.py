import dotenv
import os
from monzo.authentication import Authentication
from monzo.handlers.storage import Storage
from monzo.exceptions import MonzoAuthenticationError, MonzoServerError
from monzo.endpoints.account import Account
from monzo.endpoints.transaction import Transaction
from monzo.endpoints.pot import Pot
from datetime import datetime
import pandas as pd
from IPython.display import display
from functions import *

class EnvWriter(Storage):

    def store(self, access_token, client_id, client_secret, expiry, refresh_token):
        os.environ["ACCESS_TOKEN"] = access_token
        os.environ["CLIENT_ID"] = client_id
        os.environ["CLIENT_SECRET"] = client_secret
        os.environ["EXPIRY"] = str(expiry)
        os.environ["REFRESH_TOKEN"] = refresh_token

        dotenv.set_key(dotenv_file, "ACCESS_TOKEN", os.environ["ACCESS_TOKEN"])
        dotenv.set_key(dotenv_file, "CLIENT_ID", os.environ["CLIENT_ID"])
        dotenv.set_key(dotenv_file, "CLIENT_SECRET", os.environ["CLIENT_SECRET"])
        dotenv.set_key(dotenv_file, "EXPIRY", os.environ["EXPIRY"])
        dotenv.set_key(dotenv_file, "REFRESH_TOKEN", os.environ["REFRESH_TOKEN"])


dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)

client_id = os.getenv("CLIENT_ID")  # Client ID obtained when creating Monzo client
client_secret = os.getenv("CLIENT_SECRET")  # Client secret obtained when creating Monzo client
redirect_uri = 'http://127.0.0.1/monzo'  # URL requests via Monzo will be redirected in a browser
access_token = os.getenv("ACCESS_TOKEN")
expiry = os.getenv("EXPIRY")
refresh_token = os.getenv("REFRESH_TOKEN")

DATABASE = False

if DATABASE:
    # Enter database info
    pass
else:
    # Set file type and folder name
    FOLDER_PATH = "files"
    FILE_TYPE = "csv"

# If we have the access token already, then setup auth object. If not, then get access token prior to this 
if os.getenv("ACCESS_TOKEN"):
    monzo = Authentication(
        client_id=client_id, 
        client_secret=client_secret, 
        redirect_url=redirect_uri,
        access_token=access_token,
        access_token_expiry=float(expiry),
        refresh_token=refresh_token
    )

    # Instantiate handler
    handler = EnvWriter()
    monzo.register_callback_handler(handler)
else:
    monzo = Authentication(
        client_id=client_id,
        client_secret=client_secret,
        redirect_url=redirect_uri
    )

    # The user should visit this url
    print(monzo.authentication_url)
    return_url = str(input("Paste the redirect URL here: "))
    auth_code, state = url_to_auth_code(return_url) 

    # Instantiate handler
    handler = EnvWriter()
    monzo.register_callback_handler(handler)

    # Retrieve access token
    try:
        monzo.authenticate(authorization_token=auth_code, state_token=state)
    except MonzoAuthenticationError:
        print('State code does not match')
        exit(1)
    except MonzoServerError:
        print('Monzo Server Error')
        exit(1)

merchant_fields = {
    "MerchantID":[],
    "GroupID":[],
    "MerchantName":[],
    "IsOnline":[],
    "Longitude":[],
    "Latitude":[],
    "City":[],
    "Region":[],
    "Country":[],
    "Category":[],
    "IsATM":[],
    "Logo":[]
}

group_fields = {
    "GroupID":[],
    "MerchantGroupName":[]
}

transaction_fields = {
    "TransactionID":[],
    "Date":[],
    "GBPAmount":[],
    "AccountID":[],
    "MerchantID":[],
    "LocalAmount":[],
    "LocalCurrency":[],
    "Category":[],
    "Categories":[],
    "IsTransfer":[],
    "Description":[]
}

account_fields = {
    "AccountID":[],
    "AccountType":[],
    "Description":[]
}

pot_fields = {
    "PotID":[],
    "AccountID":[],
    "PotName":[],
    "PotCurrency":[],
    "Deleted":[]
}

balance_fields = {
    "Account/PotID":[],
    "Date":[],
    "Balance":[],
    "Currency":[]
}

# Set time of update
update_time = datetime.now().strftime("%Y/%m/%d %H:%M")

# Fetch list of account objects
accounts = Account.fetch(monzo)

# Loop through accounts and populate information
for account in accounts:
    print(account.account_type())
    
    # Populate account fields
    account_fields["AccountID"].append(account.account_id)
    account_fields["AccountType"].append(account.account_type())
    account_fields["Description"].append(account.description)

    # Populate balance fields for account
    balance_fields["Account/PotID"].append(account.account_id)
    balance_fields["Date"].append(update_time)
    balance_fields["Balance"].append(account.balance.balance)
    balance_fields["Currency"].append(account.balance.currency)

    # Fetch list of transactions and pots for the account
    transactions = Transaction.fetch(monzo, account_id=account.account_id, since=datetime.strptime("2023-01-01", "%Y-%m-%d"), expand=["merchant"])
    pots = Pot.fetch(monzo, account_id=account.account_id)

    for transaction in transactions:
        # Populate transaction fields
        transaction_fields["TransactionID"].append(transaction.transaction_id)
        transaction_fields["Date"].append(transaction.created)
        transaction_fields["GBPAmount"].append(transaction.amount)
        transaction_fields["AccountID"].append(transaction.account_id)
        transaction_fields["MerchantID"].append((transaction.merchant["id"] if transaction.merchant else None))
        transaction_fields["LocalAmount"].append(transaction.local_amount)
        transaction_fields["LocalCurrency"].append(transaction.local_currency)
        transaction_fields["Category"].append(transaction.category)
        transaction_fields["Categories"].append(transaction.categories)
        transaction_fields["IsTransfer"].append(transaction.is_load)
        transaction_fields["Description"].append(transaction.description)

        # Populate merchant fields if there is a merchant
        if transaction.merchant:
            merchant = transaction.merchant

            merchant_fields["MerchantID"].append(merchant["id"])
            merchant_fields["GroupID"].append(merchant["group_id"])
            merchant_fields["MerchantName"].append(merchant["name"])
            merchant_fields["Category"].append(merchant["category"])
            merchant_fields["IsOnline"].append(merchant["online"])
            merchant_fields["Longitude"].append(merchant["address"]["longitude"])
            merchant_fields["Latitude"].append(merchant["address"]["latitude"])
            merchant_fields["City"].append(merchant["address"]["city"])
            merchant_fields["Region"].append(merchant["address"]["region"])
            merchant_fields["Country"].append(merchant["address"]["country"])
            merchant_fields["IsATM"].append(merchant["atm"])
            merchant_fields["Logo"].append(merchant["logo"])

            group_fields["GroupID"].append(merchant["group_id"])
            group_fields["MerchantGroupName"].append(merchant["name"])

    # Populate pot info for each pot in the account
    for pot in pots:
        pot_fields["PotID"].append(pot.pot_id)
        pot_fields["AccountID"].append(account.account_id)
        pot_fields["PotName"].append(pot.name)
        pot_fields["PotCurrency"].append(pot.currency)
        pot_fields["Deleted"].append(pot.deleted)

        # Populate balance fields for pot
        balance_fields["Account/PotID"].append(pot.pot_id)
        balance_fields["Date"].append(update_time)
        balance_fields["Balance"].append(pot.balance)
        balance_fields["Currency"].append(pot.currency)


# Create dataframes
df_accounts = pd.DataFrame(data=account_fields)
df_pots = pd.DataFrame(data=pot_fields)
df_transactions = pd.DataFrame(data=transaction_fields)
df_merchants = pd.DataFrame(data=merchant_fields)
df_balances = pd.DataFrame(data=balance_fields)
df_groups = pd.DataFrame(data=group_fields)

df_transactions["PotID"] = df_transactions.Description.apply(lambda x: x if x[:4] == "pot_" else None)
df_transactions["GBPAmount"] = df_transactions["GBPAmount"] / 100
df_transactions["LocalAmount"] = df_transactions["LocalAmount"] / 100
df_balances["Balance"] = df_balances["Balance"] / 100
df_merchants = df_merchants.drop_duplicates(["MerchantID"], keep="last")
df_groups = df_groups.drop_duplicates(["group_id"], keep="last")

# File paths
accounts_path = os.path.join(FOLDER_PATH, "accounts")
groups_path = os.path.join(FOLDER_PATH, "groups")
merchants_path = os.path.join(FOLDER_PATH, "merchants")
transactions_path = os.path.join(FOLDER_PATH, "transactions")
pots_path = os.path.join(FOLDER_PATH, "pots")
balances_path = os.path.join(FOLDER_PATH, "balances")

# Load in existing dataframes
df_accounts_old = dataframe_from_file(accounts_path, format=FILE_TYPE)
df_groups_old = dataframe_from_file(groups_path, format=FILE_TYPE)
df_merchants_old = dataframe_from_file(merchants_path, format=FILE_TYPE)
df_transactions_old = dataframe_from_file(transactions_path, format=FILE_TYPE)
df_pots_old = dataframe_from_file(pots_path, format=FILE_TYPE)
df_balances_old = dataframe_from_file(balances_path, format=FILE_TYPE)

# Union new data to old data and drop duplicates
df_accounts_union = union_dataframes(df_accounts_old, df_accounts, drop_duplicates=True, drop_duplicates_columns=["AccountID"])
df_groups_union = union_dataframes(df_groups_old, df_groups, drop_duplicates=True, drop_duplicates_columns=["GroupID"])
df_merchants_union = union_dataframes(df_merchants_old, df_merchants, drop_duplicates=True, drop_duplicates_columns=["MerchantID"])
df_transactions_union = union_dataframes(df_transactions_old, df_transactions, drop_duplicates=True, drop_duplicates_columns=["TransactionID"])
df_pots_union = union_dataframes(df_pots_old, df_pots, drop_duplicates=True, drop_duplicates_columns=["PotID"])
df_balances_union = union_dataframes(df_balances_old, df_balances, drop_duplicates=True, drop_duplicates_columns=["Account/PotID", "Date"])

# Write to files
