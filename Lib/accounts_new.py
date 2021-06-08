from OOP_miniproject.Lib.helper_functions import read_int, define_df, check_transfer_acct_creds, define_file


class Account:

    def __init__(self, acct_num, customerid):
        self.acct_num = acct_num
        self.customer_id = customerid
        self.balance = self.get_balance()

    def deposit(self):
        amount = read_int("Enter amount to be Deposited: ")
        self.balance += amount
        self.update_balance()
        return('depoisted:{} current balance: {}'.format(amount, self.balance))


    def withdraw(self):
        amount = read_int("Enter amount to be Withdrawn: ")
        if self.balance >= amount:
            self.balance -= amount
            self.update_balance()
            return ('withdrew:{} current balance: {}'.format(amount, self.balance))
        else:
            print("\n Insufficient balance  ")

    def show_balance(self):
        print("\n Net Available Balance=", self.balance)

    def transfer(self):
        amount = read_int("How much would you like to transfer?")
        accounttotransfer, custidtransfer = check_transfer_acct_creds()
        account2 = Account(acct_num=accounttotransfer, customerid=custidtransfer)
        if self.balance >= amount:
            self.balance = self.balance - amount
            account2.balance = account2.balance + amount

            self.update_balance(transfer=True,acccounttotransfer=accounttotransfer, account2balance=account2.balance )
            # df_a.loc[df_a.accountnum == accounttotransfer, "balance"] = account2.balance
            # push_to_csv('account')

            print("\n You Transferred:", amount)
        else:
            print("\n Insufficient balance to transfer ")

    def update_balance(self, transfer=False, acccounttotransfer=None, account2balance=None):
        df = define_df('account')
        if transfer==False:
            df.loc[df.accountnum == self.acct_num, "balance"] = self.balance
            df.to_csv(define_file('account'), index=False)

        else:
            df.loc[df.accountnum == self.acct_num, "balance"] = self.balance
            df.loc[df.accountnum == acccounttotransfer, "balance"] = account2balance
            df.to_csv(define_file('account'), index=False)


    def get_balance(self):
        df = define_df('account')
        return int(df.loc[df['accountnum'] == self.acct_num, 'balance'])




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

