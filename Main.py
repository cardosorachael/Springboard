from uuid import uuid4
import pandas as pd
import numpy as np
from time import time
from random import randint
import time
import sys
from Lib.accounts_new import Account
from Lib.Services import CreditCard, Loan


#TODO databse handler/class
#TODO error handling customer enters wrong function

#TODO bank teller subclass implementation
#TODO Checking + Savings account differences implementation

#TODO Bug: can't service account, credit card, loan created in same session


file_customer = 'customer1.csv'
file_accounts = 'accounts1.csv'
file_creditcard = 'creditcard.csv'
file_loan = 'loan.csv'
df_c = pd.read_csv(file_customer,  delimiter=',', encoding="utf-8-sig")
df_a = pd.read_csv(file_accounts, delimiter=',', encoding="utf-8-sig")
df_cc = pd.read_csv(file_creditcard, delimiter=',', encoding="utf-8-sig")
df_l = pd.read_csv(file_loan, delimiter=',', encoding="utf-8-sig")

class User:
    def __init__(self, fname, lname):
        self.fname = fname
        self.lname = lname


class Customer(User):
    def __init__(self, fname, lname, customer_id=None, dob=None, ssn=None, zipcode=None, acct_number=None):
        User.__init__(self, fname, lname)
        self.dob = dob
        self.ssn = ssn
        self.zipcode = zipcode
        self.acct_number = acct_number
        self.customer_id = customer_id

    def init_new_customer(self, customerid, dob, ssn, zipcode, password):
        with open(file_customer, 'a') as file:
            file.write('{},{},{},{},{},{}, {}\n'.format(customerid, self.fname, self.lname, dob, ssn, zipcode, password))

    def open_account(self, customerid):
        self.acct_number = str(uuid4())
        status = 'OPEN'
        account_type = input('Checking or Savings?')
        init_balance = input('How much would you like to deposit now?')
        with open(file_accounts, 'a') as file:
            file.write('{}, {}, {}, {}, {}\n'.format(self.acct_number, customerid, account_type, init_balance, status))

        print('Account open! Your new account number is {}'.format(self.acct_number))

    def close_account(self, customerid):
        find_accts_for_customer_id(customerid)
        acct_num = input('Which account would you like to close?')
        df_a.loc[df_a.accountnum == acct_num, "status"] = 'CLOSED'
        df_a.to_csv('accounts1.csv', index=False)
        print('Account closed!')

    def apply_for_loan(self, customerid):
        loannum = random_with_N_digits(5)
        loan_amt = int(input('What at would you like to borrow?'))
        print(find_accts_for_customer_id(customerid))
        accountnum = input('which account would you like to use?')
        status = 'PENDING'
        loan_interest = 2
        loan_time = 10
        total_payment = loan_amt*(1 + (loan_interest*loan_time*0.01))
        monthly_payment = total_payment/12

        with open(file_loan, 'a') as file:
            file.write(
                '{}, {}, {}, {}, {},{},{},{}\n'.format(loannum, customerid, accountnum, loan_amt, loan_interest, loan_time, total_payment, monthly_payment))

        print('Success! Loan requested. Your loanid is {}, total payment = {} over {} years'.format(loannum, total_payment, loan_time))



    def apply_for_insurance(self):
        pass

    def apply_for_credit_card(self, customerid):
        #TODO make variables
        creditcardnum = random_with_N_digits(10)
        status = 'PENDING'
        cvv = random_with_N_digits(3)
        expiration = '6/3/2022'
        credit_available = 10000 #int(df_c.loc[df_c['customerid'] == customerid, 'income'])
        current_balance = 0

        print(find_accts_for_customer_id(
            customerid))
        accountnum = input('which account would you like to use?')
        with open(file_creditcard, 'a') as file:
            file.write(
                '{}, {}, {}, {}, {},{},{},{}\n'.format(creditcardnum, customerid, cvv, expiration, credit_available,
                                                       current_balance, accountnum, status))
        print('Success- credit card opened. Card number:{}'.format(creditcardnum))


class BankTeller(User):
    def approve_new_account(self):
        pass

    def delete_account(self):
        pass

    def edit_customer_info(self):
        pass

    def approve_loan(self):
        pass

    def issue_card(self):
        pass

    def approve_insurance(self):
        pass

    def provide_info(self):
        pass



#Misc functions - need to clean up/maybe sort into 1 class

def find_accts_for_customer_id(customerid):
    accounts_list = np.array(df_a.loc[(df_a['customerid'] == customerid) & (df_a['status'].str.contains('OPEN')) ,'accountnum'])
  #   accounts_list = np.array(df_a.loc[(df_a['customerid'] == '17') & (df_a['status'] == 'OPEN'), 'accountnum'])
    print("available accounts: {}".format(accounts_list))


def find_creditcards_for_customer_id(customerid):
    cards_list = np.array(df_cc.loc[df_cc['customerid'] == customerid, 'creditcardnum'])
    print("available credit cards: {}".format(cards_list))

def find_loans_for_customer_id(customerid):
    loans_list = np.array(df_l.loc[df_l['customerid'] == customerid, 'loannum'])
    print("available loans: {}".format(loans_list))

def check_credentials(customerid, password):
    pass1 = list(df_c.loc[df_c['customerid'] == customerid, 'password'])
    if pass1[0] == password:
        return True
    else:
        return False

def check_login_credentials():
    counter = 0
    while (counter <=2):
        customerid = int(input('please enter customerid'))
        password = input('enter password')
        res = check_credentials(customerid, password)
        if res != True:
            counter +=1
            print("invalid login credentials, please try again. you have {} more tries".format(3-counter))
        else:
            print('login successful!')
            return(customerid)

    print('No more tries, please try again later')
    sys.exit()

def generate_customer_id():
    # last_id = df['customerid'].iloc[0]
    try:
        last_id = df_c.customerid.iat[-1]
    except IndexError:
        last_id = 0
    generated_customer_id = last_id + 1
    return (generated_customer_id)


def get_cust_info_from_db(customerid):
    fname = df_c.loc[df_c['customerid'] == customerid, 'first_name']
    lname = df_c.loc[df_c['customerid'] == customerid, 'last_name']
    return fname, lname

# def add_cust_info_to_db(customerid, **kwargs):
    # with open(file_customer, 'w') as file:
    #     file.
    #add_user_info_to_file(income, employed, gender)

def random_with_N_digits(n):
    range_start = 10 ** (n - 1)
    range_end = (10 ** n) - 1
    return randint(range_start, range_end)



class Menu:
    def __init__(self):
        pass

    def existing_user_menu(self):
        while True:
            choice = input("Enter 1 to access existing account.\n"
                           "Enter 2 to open account.\n"
                           "Enter 3 to close account.\n"
                           "Enter 4 to request additional bank service (credit card, loan, insurance)\n"
                           "Enter 5 to exit.\n"
                           )
            if choice not in ["1", "2", "3", "4", "5"]:
                print ("Please enter 1, 2, 3, 4 or 5..")
            else:
                return choice

    def new_user_menu(self):
        choice = input("Enter 1 to open account.\n"
                       "Enter 2 to request additional bank service (credit card, loan, insurance)\n"
                       "Enter 3 to exit.\n"
                       )
        if choice not in ["1", "2" , "3"]:
            print("Please enter 1, 2 or 3..")
        else:
            return choice
        pass


    def access_account_menu(self):
        while True:
            choice = input("Enter 1 to display balance.\n"
                           "Enter 2 to withdraw money.\n"
                           "Enter 3 to deposit money.\n"
                           "Enter 4 to transfer money.\n"
                           "Enter 5 to return to the main menu.\n")
            if choice not in ["1", "2", "3", "4", "5"]:
                print ("Please enter 1, 2, 3 4 or 5.")
            else:
                break
        return choice

    def new_cust_service_menu(self):
        while True:
            choice = input("Enter 1 to request credit card.\n"
                           "Enter 2 to request loan.\n"
                           "Enter 3 to apply for insurance .\n"
                           "Enter 4 to return to the main menu.\n")
            if choice not in ["1", "2", "3", "4"]:
                print("Please enter 1, 2, 3 or 4.")
            else:
                break
        return choice

    def request_service_menu(self):
        while True:
            choice = input("Enter 1 to perform actions on existing credit cards.\n"
                           "Enter 2 to perform actions on existing loan.\n"
                           "Enter 3 to perform actions on existing insurance .\n"
                           "Enter 4 to request credit card.\n"
                           "Enter 5 to request loan.\n"
                           "Enter 6 to apply for insurance .\n"
                           "Enter 7 to return to the main menu.\n")
            if choice not in ["1", "2", "3", "4","5","6","7"]:
                print("Please enter 1, 2, 3,4,5,6 or 7")
            else:
                break
        return choice

    def loan_menu(self):
        while True:
            choice = input("Enter 1 to view loan details.\n"
                           "Enter 2 view next payment date.\n"
                           "Enter 3 to make payment .\n"
                           "Enter 4 to return to the main menu.\n")
            if choice not in ["1", "2", "3", "4"]:
                print("Please enter 1, 2, 3 or 4.")
            else:
                break
        return choice

    def credit_card_menu(self):
        while True:
            choice = input("Enter 1 to view balance.\n"
                           "Enter 2 to make payment.\n"
                           "Enter 3 to make purchase .\n"
                           "Enter 4 to view credit available .\n"
                           "Enter 5 to return to the main menu.\n")
            if choice not in ["1", "2", "3", "4", "5"]:
                print("Please enter 1, 2, 3 4 or 5 .")
            else:
                break
        return choice

    def previous_page(self):
        while True:
            return input("Would you like to return to the previous page? Enter yes or no:")[0].lower() == 'y'

    def run(self):

        while True:
            user_type = input('Are you new or exisitng cutsomer enter e/c or any other key to exit?')
            if user_type == 'e':
                customerid = check_login_credentials()
                # customerid = int(input('enter id'))
                # password = df_c.get(customerid)
                # print(password)
                lname, fname = get_cust_info_from_db(customerid)
                customerobj = Customer(fname=fname,
                                       lname=lname, customer_id=customerid)

                while True:
                    choice = self.existing_user_menu()
                    if choice == "1":
                        find_accts_for_customer_id(customerid)
                        acct_of_interest = input('please select account')
                        account = Account(acct_of_interest, customerid)

                        while True:
                            user_choice = self.access_account_menu()
                            if user_choice == "1":
                                account.show_balance()
                            elif user_choice == "2":
                                print(account.withdraw())
                            elif user_choice == "3":
                                print(account.deposit())
                            elif user_choice == "4":
                                print(account.transfer())
                            elif user_choice == "5":
                                break
                            else:
                                continue
                            if not self.previous_page():
                                break

                    elif choice == "2":
                        customerobj.open_account(customerobj.customer_id)


                    elif choice == "3":
                        customerobj.close_account(customerobj.customer_id)

                    elif choice == "4":
                        print('service menu selected... ')

                        while True:
                            service_choice = self.request_service_menu()
                            if service_choice == "1":
                                find_creditcards_for_customer_id(customerobj.customer_id)
                                card_of_interest = int(input('please select card'))
                                creditcard = CreditCard(creditcardnum= card_of_interest, customer_id=customerobj.customer_id)

                                while True:
                                    creditcardchoice = self.credit_card_menu()
                                    if creditcardchoice == "1":
                                        creditcard.show_balance()
                                    elif creditcardchoice == "2":
                                        creditcard.make_payment()
                                    elif creditcardchoice == "3":
                                        creditcard.make_purchase()
                                    elif creditcardchoice == "4":
                                        creditcard.show_credit_available()
                                    elif creditcardchoice == "5":
                                        break
                                    else:
                                        continue
                                    if not self.previous_page():
                                        break
                            elif service_choice == '2':
                                print('existing loan selected')
                                find_loans_for_customer_id(customerobj.customer_id)
                                loan_of_interest = int(input('please select loan'))
                                loan = Loan(loannum=loan_of_interest,
                                                        customer_id=customerobj.customer_id)

                                while True:
                                    loanchoice = self.loan_menu()
                                    if loanchoice == "1":
                                        loan.show_loan_details()
                                    elif loanchoice == "2":
                                        loan.next_payment_date()
                                    elif loanchoice == "3":
                                        loan.make_payment()
                                    elif creditcardchoice == "4":
                                        break
                                    else:
                                        continue
                                    if not self.previous_page():
                                        break

                            elif service_choice == '3':
                                print('existing insurance selected')

                            elif service_choice == '4':
                                customerobj.apply_for_credit_card(customerobj.customer_id)
                            elif service_choice == '5':
                                print('applying loan')
                                customerobj.apply_for_loan(customerobj.customer_id)
                            elif service_choice == '6':
                                print('open new insurance')

                            elif service_choice == '7':
                                break

                            else:
                                continue
                            if not self.previous_page():
                                break

                    elif choice == "5":
                        break
                    else:
                        continue

                    if not self.previous_page():
                        break


            elif user_type == 'n':
                print('new customer..')
                fname, lname = input('enter first and last name separated by space').split(' ')
                customerobj = Customer(fname=fname, lname=lname)
                customerid = generate_customer_id()
                customerobj.customer_id = customerid
                dob, ssn, zipcode = input('Hello {}, please provide more details: dob, ssn, zipcode'.format(customerobj.fname)).split(' ')

                customerobj.init_new_customer(customerid=customerid, dob=dob, ssn=ssn, zipcode=zipcode,
                                              password=fname)
                print('Success! your login is created: customer id is {} and password is {}'.format(
                    customerobj.customer_id,
                    customerobj.fname))
                while True:
                    new_user_choice = self.new_user_menu()
                    if new_user_choice == '1':
                        #open acct
                        customerobj.open_account(customerid=customerid)

                    elif new_user_choice == '2':
                        # print('service menu selected... ')
                        addtnl_user_info = input(
                            'We require additional information to be able to process your request.Please provide: income, employed (Y/N), gender'.format(
                                customerobj.fname))
                        income, employed, gender = addtnl_user_info.split(' ')
                        # add_user_info_to_file(income, employed, gender)

                        while True:
                            service_choice = self.new_cust_service_menu()
                            if service_choice == "1":
                                haveaccount = input('do you have an account to link credit card? (Y/N)')
                                if haveaccount == 'Y':
                                    customerobj.apply_for_credit_card(customerid)
                                else:
                                    print("ok, let's make you an account!")
                                    customerobj.open_account(customerid)
                                    time.sleep(5)
                                    customerobj.apply_for_credit_card(customerid)
                            elif service_choice == "2":
                                #TODO - need to create account before new customer can apply for loan
                                customerobj.apply_for_loan(customerid)
                            elif service_choice == "3":
                                print('apply for insurance..')
                            elif service_choice == '4':
                                break
                            else:
                                continue
                            if not self.previous_page():
                                break

                    elif new_user_choice == "3":
                        break
                    else:
                        continue

                    if not self.previous_page():
                        break

            else:
                break
if __name__ == '__main__':

    menu = Menu()
    Menu.run(menu)






