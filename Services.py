import pandas as pd
from datetime import date


file_creditcard = 'creditcard.csv'
df_cc = pd.read_csv(file_creditcard)


class Services:
    def __init__(self, customer_id):
        self.customer_id = customer_id


class Loan(Services):
    pass

class Insurance(Services):
    pass

class CreditCard(Services):
    def __init__(self, creditcardnum, customer_id):
        Services.__init__(self, customer_id)
        self.creditcardnum = creditcardnum
        self.available_credit = int(df_cc.loc[df_cc['creditcardnum'] == self.creditcardnum, 'creditavailable'])
        self.interest_rate = 10
        self.currentbalance = int(df_cc.loc[df_cc['creditcardnum'] == self.creditcardnum, 'currentbalance'])


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
        df_cc.loc[df_cc.creditcardnum == self.creditcardnum, "lastpurchase"] = today.strftime("%d/%m/%Y")
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
                df_cc.loc[df_cc.creditcardnum == self.creditcardnum, "lastpayment"] = today.strftime("%d/%m/%Y")

                df_cc.to_csv(file_creditcard, index=False)
                print('current balance = {}'.format(self.currentbalance))  # increment current balance
            else:
                raise Exception('Payment larger than outstanding balance!')
        except Exception as error:
            print('Payment Error: ' + str(error) + '\n')


    #todo calculate late fees (how many days since last payment)  balance * interest



