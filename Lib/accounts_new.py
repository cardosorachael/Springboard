import pandas as pd

file_accounts = 'accounts1.csv'
df_a = pd.read_csv(file_accounts)

#TODO handle differences between methods based on account type
#TODO error handling

class Account:

    def __init__(self, acct_num, customerid):
        self.acct_num = acct_num
        self.customer_id = customerid
        self.balance = int(df_a.loc[df_a['accountnum'] == acct_num, 'balance'])

    def deposit(self):
        amount = float(input("Enter amount to be Deposited: "))
        self.balance += amount
        df_a.loc[df_a.accountnum == self.acct_num, "balance"] = self.balance
        df_a.to_csv(file_accounts,index=False)
        return('depoisted:{} current balance: {}'.format(amount, self.balance))

    def withdraw(self):
        amount = float(input("Enter amount to be Withdrawn: "))
        if self.balance >= amount:
            self.balance -= amount
            df_a.loc[df_a.accountnum == self.acct_num, "balance"] = self.balance
            df_a.to_csv(file_accounts, index=False)
            return ('withdrew:{} current balance: {}'.format(amount, self.balance))
        else:
            print("\n Insufficient balance  ")

    def show_balance(self):
        print("\n Net Available Balance=", self.balance)

    def transfer(self):
        amount = int(input("How much would you like to transfer?"))
        accounttotransfer = input('please select account to transfer')
        custidtransfer = int(input('please select customer id to transfer'))
        account2 = Account(acct_num=accounttotransfer, customerid=custidtransfer)
        if self.balance >= amount:
            self.balance = self.balance - amount
            df_a.loc[df_a.accountnum == self.acct_num, "balance"] = self.balance
            df_a.to_csv(file_accounts, index=False)
            account2.balance = account2.balance + amount
            df_a.loc[df_a.accountnum == accounttotransfer, "balance"] = account2.balance
            df_a.to_csv(file_accounts, index=False)

            print("\n You Transferred:", amount)
        else:
            print("\n Insufficient balance to transfer ")





class CheckingAccount(Account):
    def __init__(self,acct_num, customer_id):
        Account.__init__(self, acct_num, customer_id)
        self.overdraft_fee = 100

    # overdraft protection pulls from saving account to keep min balance > amt
    # inputs: savings account num,
    #
    # login to users account
    # if checking -> run


class SavingsAccount(Account):

    def __init__(self, acct_num, customer_id):
        Account.__init__(self, acct_num, customer_id)
        self.transaction_limit = 8
        self.num_transactions = 0
        self.min_balance = 100

    #def add interest payment()
    # every month add rate*balance from account
    #
    # if savings -> run interest payment if account is selected

