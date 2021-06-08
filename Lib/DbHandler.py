import csv


class dbhandler:
    def __init__(self, customer_file, account_file, creditcard_file, loan_file):
        self.customer_file = customer_file
        self.account_file = account_file
        self.creditcard_file = creditcard_file
        self.loan_file = loan_file

    def write_customer(self,word):
        with open(self.customer_file, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(word)

    def write_account(self,word):
        with open(self.account_file, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(word)

    def write_creditcard(self,word):
        with open(self.creditcard_file, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(word)

    def write_loan(self,word):
        with open(self.loan_file, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(word)



