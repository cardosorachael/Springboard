from OOP_miniproject.Lib.helper_functions import *
from OOP_miniproject.Lib.DbHandler import dbhandler
from uuid import uuid4
from datetime import date
from dateutil.relativedelta import *

class User:
    def __init__(self, fname, lname):
        self.fname = fname
        self.lname = lname

class Customer(User):
    def __init__(self, fname, lname,customer_id=None, dob=None, ssn=None, zipcode=None):
        User.__init__(self, fname, lname)
        self.dob = dob
        self.ssn = ssn
        self.zipcode = zipcode
        self.customer_id = customer_id
        self.db_handler = dbhandler(customer_file=define_file('customer'), account_file=define_file('account'),
                                    creditcard_file=define_file('creditcard'),
                                    loan_file=define_file('loan'))

    def new_customer_init(self):
        self.db_handler.write_customer([self.customer_id, self.fname, self.lname, self.dob, self.ssn, self.zipcode, self.fname.lower()])
        print('Success! your login is created: customer id is {} and password is {}'.format(
                self.customer_id,
                self.fname.lower()))


    def open_account(self, customerid):
        self.acct_number = str(uuid4())
        status = 'OPEN'
        account_type = input('Checking or Savings?')
        init_balance = read_int('How much would you like to deposit now?')
        self.db_handler.write_account([self.acct_number, customerid, account_type, init_balance, status])

        print('Account open! Your new account number is {}'.format(self.acct_number))

    def apply_for_loan(self, customerid):
        loannum = random_with_N_digits(5)
        loan_amt = read_int('What would you like to borrow?')
        accountnum = check_object_belong_to_cust(customerid=customerid, objectname='account')

        loan_interest = 2
        loan_time = 10
        total_payment = loan_amt*(1 + (loan_interest*loan_time*0.01))
        monthly_payment = total_payment/12
        self.db_handler.write_loan([loannum, self.customer_id, accountnum, loan_amt, loan_interest, loan_time, total_payment, monthly_payment])

        print('Success! Loan requested. Your loanid is {}, total payment = {} over {} years'.format(loannum, total_payment, loan_time))


    def apply_for_credit_card(self, customerid):

        creditcardnum = random_with_N_digits(10)
        status = 'PENDING'
        cvv = random_with_N_digits(3)

        today = date.today()
        nextdate = today + relativedelta(years=+1)
        expiration = nextdate
        credit_available = read_int('What is your ideal creditlimit?')
        current_balance = 0
        accountnum = check_object_belong_to_cust(customerid=customerid, objectname='account')

        self.db_handler.write_creditcard([creditcardnum, self.customer_id, cvv, expiration, credit_available,
                                                       current_balance, accountnum, status])


        print('Success- credit card opened. Card number:{}'.format(creditcardnum))

class BankTeller(User):
    def __init__(self, fname, lname,tellerid=None):
        User.__init__(self, fname, lname)
        self.tellerid=tellerid

    def close_account(self):
        customerid = read_int('Enter customer id of account would you like to close')
        acct_num = check_object_belong_to_cust(customerid=customerid, objectname='account')
        self.close_attr_in_db(attr=acct_num, type1='account')

    def close_loan(self):
        customerid = read_int('Enter customer id of loan would you like to close')
        loan_num = check_object_belong_to_cust(customerid=customerid, objectname='loan')
        self.close_attr_in_db(attr=loan_num, type1='loan')

    def close_creditcard(self):
        customerid = read_int('Enter customer id of credit card would you like to close')
        creditcardnum = check_object_belong_to_cust(customerid=customerid, objectname='creditcard')
        self.close_attr_in_db(attr=creditcardnum, type1='creditcard')

    def approve_loan(self):
    #Only approved loan is dob < 1990
        df = define_df('loan')
        df_c = define_df('customer')
        df_a = define_df('account')
        for i, row in df.iterrows():
            if (row['status']== 'PENDING'):
                dob = list(df_c.loc[df_c.customerid == row.customerid, "dob"])
                dob = (dob[0]) % 10000
                if dob < 1990:
                    df.at[i, 'status'] = 'OPEN'
                    df.to_csv(define_file('loan'), index=False)

                    # print(row.accountnum)
                    # print(type(row.accountnum))
                    # print(row.loan_amt)
                    # print(type(row.loan_amt))
                    df_a = define_df('account')
                    # print(int(df_a.loc[df_a['accountnum'] == row.accountnum , 'balance']))
                    df_a.loc[df_a['accountnum'] == row.accountnum , 'balance'] += row.loan_amt
                    df_a.to_csv(define_file('account'), index=False)
                else:
                    continue
        print('Finished Approving all eligible Loans!')

    def approve_card(self):
    #Only approved credit card if dob 1970 < 1995
        df = define_df('creditcard')
        df_c = define_df('customer')
        for i, row in df.iterrows():
            if (row['status'] == 'PENDING'):
                dob = list(df_c.loc[df_c.customerid == row.customerid, "dob"])
                dob = (dob[0]) % 10000
                if 1970 <= dob <= 1990:
                    df.at[i, 'status'] = 'OPEN'
                    df.to_csv(define_file('creditcard'), index=False)

                else:
                    continue
        print('Finished Approving all eligible Credit Cards!')


    def close_attr_in_db(self, attr, type1):
        df = define_df(type1)
        if type1 == 'loan':
            df.loc[df.loannum == attr, 'status'] = 'CLOSED'
        elif type1 == 'account':
            df.loc[df.accountnum == attr, 'status'] = 'CLOSED'
        else:
            df.loc[df.creditcardnum == attr, 'status'] = 'CLOSED'
        df.to_csv(define_file(type1), index=False)
        print('{} Closed!'.format(type1))
