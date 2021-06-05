import pandas as pd
from datetime import date
from dateutil.relativedelta import *


file_creditcard = 'creditcard.csv'
file_loan = 'loan.csv'
file_accounts = 'accounts1.csv'
df_cc = pd.read_csv(file_creditcard)
df_l = pd.read_csv(file_loan)
df_a = pd.read_csv(file_accounts)

#TODO Link credit card payment to account
#TODO link loan payment to account
#TODO link loan creation to account (deposit amount)


class Services:
    def __init__(self, customer_id):
        self.customer_id = customer_id


class Loan(Services):
    def __init__(self, loannum, customer_id):
        Services.__init__(self, customer_id)
        self.loannum = loannum
        self.loan_amount = int(df_l.loc[df_l['loannum'] == self.loannum, 'loan_amt'])
        self.loan_time = int(df_l.loc[df_l['loannum'] == self.loannum, 'loan_time'])
        self.loan_interest = int(df_l.loc[df_l['loannum'] == self.loannum, 'loan_interest'])
        self.loan_monthly_payment= int(df_l.loc[df_l['loannum'] == self.loannum, 'monthly_payment'])
        self.total_payment = int(df_l.loc[df_l['loannum'] == self.loannum, 'total_payment'])


    def show_loan_details(self):
        print('You took a loan of {}, interest rate {}, over {} years.\n Total payment = {}\n Monthly payment = {}\n'.format(self.loan_amount, self.loan_interest, self.loan_time, self.total_payment, self.loan_monthly_payment))

    def next_payment_date(self):
        next_payment_date = df_l.loc[df_l['loannum'] == self.loannum, 'next_payment']
        print('next payment date:{}'.format(next_payment_date))


    def make_payment(self):
        if self.total_payment > 0:
            payment_amt = self.loan_monthly_payment
            self.total_payment -= payment_amt
            df_l.loc[df_l.loannum == self.loannum, "total_payment"] = self.total_payment

            today = date.today()
            nextdate = today +relativedelta(months=+1)
            df_l.loc[df_l.loannum == self.loannum, "last_payment"] = today.strftime("%m/%d/%Y")
            df_l.loc[df_l.loannum == self.loannum, "next_payment"] = nextdate.strftime("%m/%d/%Y")

            df_l.to_csv(file_loan, index=False)
            print('Amount to payoff = {}'.format(self.total_payment))
        else:
            df_l.loc[df_l.loannum == self.loannum, "next_payment"] = 'CLOSED'
            df_l.loc[df_l.loannum == self.loannum, "monthly_payment"] = 0
            print('Loan already paid off')

        #TODO penalty if monthly payment is not made on time

class Insurance(Services):
    pass

class CreditCard(Services):
    #todo calculate late fees (how many days since last payment)  balance * interest
    #todo make credit limit a function of income
    #todo make interest rate higher if unemployed
    def __init__(self, creditcardnum, customer_id):
        Services.__init__(self, customer_id)
        self.creditcardnum = creditcardnum
        self.available_credit = int(df_cc.loc[df_cc['creditcardnum'] == self.creditcardnum, 'creditavailable'])
        self.interest_rate = 10
        self.currentbalance = int(df_cc.loc[df_cc['creditcardnum'] == self.creditcardnum, 'currentbalance'])
        self.accountnum = str(df_cc.loc[df_cc['creditcardnum'] == self.creditcardnum, 'accountnum'])


    def show_balance(self):
        print("\n Available Credit Card Balance=", self.currentbalance)

    def show_credit_available(self):
        print("\n Available Credit Available=", self.available_credit)

    def make_purchase(self):
        amount = int(input("Enter amount to make purchase: "))

        self.available_credit -= amount  # deduct from available credit
        self.currentbalance +=amount
        print(self.creditcardnum)
        df_cc.loc[df_cc.creditcardnum == self.creditcardnum, "creditavailable"] = self.available_credit
        df_cc.loc[df_cc.creditcardnum == self.creditcardnum, "currentbalance"] = self.currentbalance
        today = date.today()
        df_cc.loc[df_cc.creditcardnum == self.creditcardnum, "lastpurchase"] = today.strftime("%m/%d/%Y")
        df_cc.to_csv(file_creditcard, index=False)



        print('current balance = {}'.format(self.currentbalance)) # increment current balance


    def make_payment(self):  # write a payment function
        payment = int(input('Enter amount for payment'))
        try:
            if (payment <= self.currentbalance):
                self.available_credit += payment   # increases available_credit by amount
                self.currentbalance -= payment  # reduces current balance by amount
                df_cc.loc[df_cc.creditcardnum == self.creditcardnum, "creditavailable"] = self.available_credit
                df_cc.loc[df_cc.creditcardnum == self.creditcardnum, "currentbalance"] = self.currentbalance

                today = date.today()
                df_cc.loc[df_cc.creditcardnum == self.creditcardnum, "lastpayment"] = today.strftime("%m/%d/%Y")

                df_cc.to_csv(file_creditcard, index=False)
                # acct_bal = int(df_a.loc[df_a.accountnum == self.accountnum, "balance"])
                # print(acct_bal)
                # df_a.loc[df_a.accountnum == self.accountnum, "balance"] = acct_bal
                # df_a.to_csv(file_accounts, index=False)
                print('current balance = {}'.format(self.currentbalance))  # increment current balance
            else:
                raise Exception('Payment larger than outstanding balance!')
        except Exception as error:
            print('Payment Error: ' + str(error) + '\n')






