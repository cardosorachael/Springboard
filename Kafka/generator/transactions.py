# Transactions Python script

# - Before starting app via docker-compose.yml file, we need to populate the
# message with an actual value representing an action.
# - Thus, a utility function called "create_random_transaction()" will be used to
# create randomised transactions. These will have a source account, a target
# account, an amount and a currency.

from random import choices, randint
from string import ascii_letters, digits

account_chars: str = digits + ascii_letters


def _random_account_id():
    """Return a random account number made of 12 characters."""
    return "".join(choices(account_chars, k=12))


def _random_amount():
    """Return a random amount between 1.00 and 1000.00."""
    return randint(100, 1000000) / 100


def create_random_transaction():
    """Create a fake, randomised transaction."""
    return {"source": _random_account_id(), "target": _random_account_id(), "amount": _random_amount(), "currency": "USD"}
