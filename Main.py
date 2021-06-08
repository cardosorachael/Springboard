try:
    from OOP_miniproject.Lib.helper_functions import *
    from OOP_miniproject.Lib.helper_functions_teller import *
    from OOP_miniproject.Lib.accounts_new import *
    from OOP_miniproject.Lib.Services import *
    from OOP_miniproject.Lib.Users import *
except FileNotFoundError:
    print \
            (
            'Please make sure all the csv files are in the same folder and named as follows:\n account.csv\n creditcard.csv\n customer.csv\n loan.csv\n')



# TODO optional Checking + Savings account differences implementation

#TODO if account is closed then close all linked credit cards and loans
#TODO handle reading from db cells with leading or trailing spaces

class Menu:
    def __init__(self):
        pass

    def existing_user_menu(self):
        while True:
            choice = input("Enter 1 to access existing account.\n"
                           "Enter 2 to open account.\n"
                           "Enter 3 to request additional bank service (credit card, loan)\n"
                           "Enter 4 to exit.\n"
                           )
            if choice not in ["1", "2", "3", "4"]:
                print("Please enter 1, 2, 3 or 4..")
            else:
                return choice

    def new_user_menu(self):
        choice = input("Enter 1 to open account.\n"
                       "Enter 2 to request additional bank service (credit card, loan)\n"
                       "Enter 3 to exit.\n"
                       )
        if choice not in ["1", "2", "3"]:
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
                print("Please enter 1, 2, 3 4 or 5.")
            else:
                break
        return choice

    def new_cust_service_menu(self):
        while True:
            choice = input("Enter 1 to request credit card.\n"
                           "Enter 2 to request loan.\n"
                           "Enter 3 to return to the main menu.\n")
            if choice not in ["1", "2", "3"]:
                print("Please enter 1, 2, 3")
            else:
                break
        return choice

    def request_service_menu(self):
        while True:
            choice = input("Enter 1 to perform actions on existing credit cards.\n"
                           "Enter 2 to perform actions on existing loan.\n"
                           "Enter 3 to request credit card.\n"
                           "Enter 4 to request loan.\n"
                           "Enter 5 to return to the main menu.\n")
            if choice not in ["1", "2", "3", "4", "5"]:
                print("Please enter 1, 2, 3,4,5")
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

    def bankteller_menu(self):
        while True:
            choice = input("Enter 1 to close account.\n"
                           "Enter 2 to close credit card.\n"
                           "Enter 3 to close loan.\n"
                           "Enter 4 to approve credit card.\n"
                           "Enter 5 to approve loan .\n"
                           "Enter 6 to return to the main menu.\n")
            if choice not in ["1", "2", "3", "4", "5", "6"]:
                print("Please enter 1, 2, 3,4,5 or 6")
            else:
                break
        return choice

    def previous_page(self):
        while True:
            return input("Would you like to return to the previous page? Enter yes or no:")[0].lower() == 'y'

    def run(self):

        while True:
            user_type1 = input \
                    (
                    'Welcome To the Bank!\nIf you are an employee enter B, if you are a customer (new or interest) enter C or any other key to exit')

            if user_type1 == 'B':
                print('bank teller menu loading...')
                tellerid = check_login_credentials_teller()
                lname, fname = get_teller_name_from_db(tellerid)
                tellerobj = BankTeller(fname=fname,
                                       lname=lname, tellerid=tellerid)

                while True:
                    tellerchoice = self.bankteller_menu()
                    if tellerchoice == "1":
                        print('Close Account')
                        tellerobj.close_account()
                    elif tellerchoice == "2":
                        print('Close Credit Card')
                        tellerobj.close_creditcard()
                    elif tellerchoice == "3":
                        print('Close Loan')
                        tellerobj.close_loan()
                    elif tellerchoice == "4":
                        print('Approve CreditCard')
                        tellerobj.approve_card()
                    elif tellerchoice == "5":
                        print('Approve Loan')
                        tellerobj.approve_loan()
                    elif tellerchoice == "6":
                        break
                    else:
                        continue
                    if not self.previous_page():
                        break


            elif user_type1 == 'C':
                user_type = input('Are you new or exisitng cutsomer enter e/c or any other key to exit?')
                if user_type == 'e':
                    customerid = check_login_credentials()
                    lname, fname = get_cust_name_from_db(customerid)
                    customerobj = Customer(fname=fname,
                                           lname=lname, customer_id=customerid)

                    while True:
                        choice = self.existing_user_menu()
                        if choice == "1":
                            try:
                                acct_of_interest = check_object_belong_to_cust(customerid=customerid,
                                                                               objectname='account')
                                account = Account(acct_of_interest, customerid)
                            except TypeError:
                                print \
                                        (
                                        'The account you are trying to access was just created, please restart session to allow db to update')
                                sys.exit()

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
                            print('service menu selected... ')
                            while True:
                                service_choice = self.request_service_menu()
                                if service_choice == "1":
                                    try:
                                        card_of_interest = check_object_belong_to_cust(customerid=customerid,
                                                                                       objectname='creditcard')
                                        creditcard = CreditCard(creditcardnum=card_of_interest,
                                                                customer_id=customerobj.customer_id)
                                    except TypeError:
                                        print \
                                                (
                                                'The credit card you are trying to access was just created, please restart session to allow db to update')
                                        sys.exit()

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
                                    try:
                                        loan_of_interest = check_object_belong_to_cust(customerid=customerid,
                                                                                       objectname='loan')
                                        loan = Loan(loannum=loan_of_interest, customer_id=customerid)
                                    except TypeError:
                                        print \
                                                (
                                                'The account you are trying to access was just created, please restart session to allow db to update')
                                        sys.exit()

                                    while True:
                                        loanchoice = self.loan_menu()
                                        if loanchoice == "1":
                                            loan.show_loan_details()
                                        elif loanchoice == "2":
                                            loan.next_payment_date()
                                        elif loanchoice == "3":
                                            loan.make_payment()
                                        elif loanchoice == "4":
                                            break
                                        else:
                                            continue
                                        if not self.previous_page():
                                            break

                                elif service_choice == '3':
                                    customerobj.apply_for_credit_card(customerobj.customer_id)
                                elif service_choice == '4':
                                    customerobj.apply_for_loan(customerobj.customer_id)

                                elif service_choice == '5':
                                    break

                                else:
                                    continue
                                if not self.previous_page():
                                    break

                        elif choice == "4":
                            break
                        else:
                            continue

                        if not self.previous_page():
                            break

                elif user_type == 'n':
                    customerid, fname, lname, dob, ssn, zipcode, password = get_newuser_details()
                    customerobj = Customer(customer_id=customerid, fname=fname, lname=lname, dob=dob, ssn=ssn,
                                           zipcode=zipcode)
                    customerobj.new_customer_init()

                    while True:
                        new_user_choice = self.new_user_menu()
                        if new_user_choice == '1':
                            customerobj.open_account(customerid=customerid)
                            print \
                                    (
                                    'Please restart system and login with your credentials to access menu for '
                                    'existing customers')
                            sys.exit()

                        elif new_user_choice == '2':
                            service_choice = input \
                                    (
                                    'To request a service: request credit card or loan our bank policy dictates you '
                                    'need an account open with us.\n To open an account, enter "Y" or press any key '
                                    'to exit')

                            while True:
                                if service_choice == "Y":
                                    customerobj.open_account(customerid=customerid)
                                    print \
                                            (
                                            'Please restart system and login with your credentials to access menu for '
                                            'existing customers')
                                    sys.exit()
                                else:
                                    print('goodbye..')
                                    sys.exit()

                        elif new_user_choice == "3":
                            break
                        else:
                            continue


                else:
                    break

            else:
                break


if __name__ == '__main__':
    menu = Menu()
    Menu.run(menu)
