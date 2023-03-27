import dotenv
import os
from monzo.authentication import Authentication
from monzo.handlers.storage import Storage
from monzo.exceptions import MonzoAuthenticationError, MonzoServerError
from monzo.endpoints.account import Account
from monzo.endpoints.transaction import Transaction
from datetime import datetime

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


def url_to_auth_code(url):
    url_split = url.split("&")
    token = url_split[0][29:]
    state = url_split[1][6:]
    return token, state

def get_accounts(monzo):
    accounts = Account.fetch(monzo)
    return accounts

dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)

client_id = os.getenv("CLIENT_ID")  # Client ID obtained when creating Monzo client
client_secret = os.getenv("CLIENT_SECRET")  # Client secret obtained when creating Monzo client
redirect_uri = 'http://127.0.0.1/monzo'  # URL requests via Monzo will be redirected in a browser
access_token = os.getenv("ACCESS_TOKEN")
expiry = os.getenv("EXPIRY")
refresh_token = os.getenv("REFRESH_TOKEN")

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


accounts = get_accounts(monzo)

for account in accounts:
    print(account.account_id)
    print(account.account_type())
    print(account.balance.total_balance)
    print("TRANSACTIONS\n")

    transactions = Transaction.fetch(monzo, account_id=account.account_id, since=datetime.strptime("2020-01-01", "%Y-%m-%d"))

    transactions_list = list(map(lambda x: (x.amount, x.created, x.description, x.decline_reason, x.category), transactions))

    with open("transactions.txt", "a+") as f:
        for transaction in transactions:
            f.write(f"AMOUNT : {transaction.amount}\nDATE : {datetime.strftime(transaction.created, '%d-%m-%Y')}\n DESCRIPTION : {transaction.description}\nCATEGORY : {transaction.category}\nDECLINE REASON : {transaction.decline_reason}\n\n")

