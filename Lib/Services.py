from datetime import date
from dateutil.relativedelta import *

from OOP_miniproject.Lib.helper_functions import read_int, define_df, define_file


class Services:
    def __init__(self, customer_id):
        self.customer_id = customer_id


class Loan(Services):
    def __init__(self, loannum, customer_id):
        Services.__init__(self, customer_id)
        self.loannum = loannum
        self.loan_amount = int(self.get_loan_attr_from_db('loan_amt'))
        self.loan_time = int(self.get_loan_attr_from_db('loan_time'))
        self.loan_interest = int(self.get_loan_attr_from_db('loan_interest'))
        self.loan_monthly_payment= int(self.get_loan_attr_from_db('monthly_payment'))
        self.total_payment = int(self.get_loan_attr_from_db('total_payment'))


    def show_loan_details(self):
        print('You took a loan of {}, interest rate {}, over {} years.\n Total payment = {}\n Monthly payment = {}\n'.format(self.loan_amount, self.loan_interest, self.loan_time, self.total_payment, self.loan_monthly_payment))

    def next_payment_date(self):
        next_payment_date = self.get_loan_attr_from_db('next_payment')
        print('next payment date:{}'.format(next_payment_date))


    def make_payment(self):
        if self.total_payment > 0:
            payment_amt = self.loan_monthly_payment


            self.total_payment -= payment_amt




            self.set_loan_attr_in_db(attr= "total_payment", attr_to_set= self.total_payment)


            today = date.today()
            nextdate = today +relativedelta(months=+1)
            self.set_loan_attr_in_db(attr="last_payment", attr_to_set=today.strftime("%m/%d/%Y"))
            self.set_loan_attr_in_db(attr="next_payment", attr_to_set=nextdate.strftime("%m/%d/%Y"))
            print('Payment Successful! Amount to payoff = {}'.format(self.total_payment))
        else:
            self.set_loan_attr_in_db(attr="next_payment", attr_to_set='CLOSED')
            self.set_loan_attr_in_db(attr="monthly_payment", attr_to_set=0)
            print('Loan already paid off')

    def get_loan_attr_from_db(self, attr):
        df = define_df('loan')
        return df.loc[df['loannum'] == self.loannum, attr]

    def set_loan_attr_in_db(self, attr, attr_to_set):
        df = define_df('loan')
        df.loc[df.loannum == self.loannum, attr] = attr_to_set
        df.to_csv(define_file('loan'), index=False)


class CreditCard(Services):
    def __init__(self, creditcardnum, customer_id):
        Services.__init__(self, customer_id)
        self.creditcardnum = creditcardnum
        self.available_credit = int(self.get_cc_attr_from_db(attr='creditavailable'))
        self.currentbalance = int(self.get_cc_attr_from_db(attr='currentbalance'))
        self.accountnum = self.get_cc_attr_from_db(attr='accountnum')

    def show_balance(self):
        print("\n Available Credit Card Balance=", self.currentbalance)

    def show_credit_available(self):
        print("\n Available Credit Available=", self.available_credit)

    def make_purchase(self):
        amount = read_int("Enter amount to make purchase: ")
        try:
            if (amount <= self.available_credit):
                self.available_credit -= amount  # deduct from available credit
                self.currentbalance += amount
                today = date.today()

                self.set_cc_attr_in_db(attr='creditavailable', attr_to_set=self.available_credit)
                self.set_cc_attr_in_db(attr='currentbalance', attr_to_set=self.currentbalance)
                self.set_cc_attr_in_db(attr='lastpurchase', attr_to_set=today.strftime("%m/%d/%Y"))

                print('current balance = {}'.format(self.currentbalance)) # increment current balance
            else:
                raise Exception('Purchase declined: larger than credit available = {}'.format(self.available_credit))

        except Exception as error:
            print('Purchase Error: ' + str(error) + '\n')



    def make_payment(self):
        payment = read_int('Enter amount for payment')
        try:
            if (payment <= self.currentbalance):
                self.available_credit += payment   # increases available_credit by amount
                self.currentbalance -= payment  # reduces current balance by amount
                today = date.today()

                self.set_cc_attr_in_db(attr='creditavailable', attr_to_set=self.available_credit)
                self.set_cc_attr_in_db(attr='currentbalance', attr_to_set=self.currentbalance)
                self.set_cc_attr_in_db(attr='lastpayment', attr_to_set=today.strftime("%m/%d/%Y"))

                print('current balance = {}'.format(self.currentbalance))  # increment current balance
            else:
                raise Exception('Payment larger than outstanding balance!Max payment allowed = {}'.format(self.balance))
        except Exception as error:
            print('Payment Error: ' + str(error) + '\n')

    def get_cc_attr_from_db(self, attr):
        df = define_df('creditcard')
        return df.loc[df['creditcardnum'] == self.creditcardnum, attr]

    def set_cc_attr_in_db(self, attr, attr_to_set):
        df = define_df('creditcard')
        df.loc[df.creditcardnum == self.creditcardnum, attr] = attr_to_set
        df.to_csv(define_file('creditcard'), index=False)




