import pandas as pd
from random import randint
import sys


def define_file(objectname):
    file = objectname + '.csv'
    return file


def read_int(prompt_msg):
    while True:
        try:
            return int(input(prompt_msg))
        except ValueError:
            print('invalid entry, please enter integer')


def read_letters(prompt_msg):
    while True:
        try:
            return int(input(prompt_msg))
        except ValueError:
            print('invalid entry, please enter integer')


def exit_func():
    print('Maximum number of tries exceeded, please restart')
    sys.exit()


def random_with_N_digits(n):
    range_start = 10 ** (n - 1)
    range_end = (10 ** n) - 1
    return randint(range_start, range_end)


def define_df(objectname):
    file = define_file(objectname)
    df = pd.read_csv(file, delimiter=',', encoding="utf-8-sig")
    return df


def find_object_for_customer_id(customerid, objectname):
    df = define_df(objectname)
    col = objectname + 'num'
    listobj = list(df.loc[(df['customerid'] == customerid) & (df["status"].str.contains('OPEN')), col])
    print("available {}:\n {}".format(objectname + 's', listobj))
    return listobj


def check_password(customerid, password, objectname):
    df = define_df(objectname)
    pass1 = list(df.loc[df['customerid'] == customerid, 'password'])
    if pass1[0] == password:
        return True
    else:
        return False


def check_login_credentials():
    counter = 0
    while counter <= 2:
        customerid = input('please enter customerid')
        password = input('enter password')
        try:
            customerid = int(customerid)
            try:
                res = check_password(customerid=customerid, password=password, objectname='customer')
                if not res:
                    counter += 1
                    print("invalid login credentials, please try again. you have {} more tries".format(3 - counter))
                else:
                    print('login successful!')
                    return customerid

            except IndexError:
                counter += 1
                print("Id invalid, does not exist in db, please try again. you have {} more tries".format(3 - counter))

        except ValueError:
            counter += 1
            print("Id needs to be a number, please try again. you have {} more tries".format(3 - counter))
    exit_func()


def generate_customer_id():
    try:
        df = define_df(objectname='customer')
        last_id = df.customerid.iat[-1]
    except IndexError:
        last_id = 0
    generated_customer_id = last_id + 1
    return generated_customer_id


def get_cust_name_from_db(customerid):
    df = define_df(objectname='customer')
    fname = df.loc[df['customerid'] == customerid, 'first_name']
    lname = df.loc[df['customerid'] == customerid, 'last_name']
    return fname, lname


def check_transfer_acct_creds():
    counter = 0
    while counter <= 3:
        print('inside while ')
        acct_of_interest = input('please select account to xfer to')
        customerid = read_int('please select customer id to transfer to')
        df = define_df(objectname='account')
        a = list(df.loc[df["accountnum"] == acct_of_interest, "customerid"])

        try:
            print('inside try ')
            if a[0] != customerid:
                print('Id and account number do not match, please try again ')
                counter += 1
            else:
                print('inside else')
                return acct_of_interest, customerid
        except IndexError:
            print('inside except')
            print('please select valid account number')
            counter += 1
    exit_func()


def get_newuser_details():
    print('please provide the following details....')
    fname = input('Please enter first name')
    lname = input('Please enter last name')
    dob = int(check_len_input(prompt_msg='Please enter dob as MMDDYYY, eg 10061997', length=8, attribute='date'))
    ssn = int(check_len_input(prompt_msg='Please enter SSN', length=9, attribute='ssn'))
    zipcode = int(check_len_input(prompt_msg='Please enter zipcode', length=5, attribute='zipcode'))
    customerid = generate_customer_id()
    password = fname.lower()
    return customerid, fname, lname, dob, ssn, zipcode, password


def check_len_input(prompt_msg, length, attribute):
    while True:
        number = input(prompt_msg)
        if not number.isdigit():  # check if a string contains a number with .isdigit()
            print("Enter only numbers\n")
            continue
        elif len(number) != length:
            print("Invalid {},  must be {} digits\n".format(attribute, length))
            continue
        else:
            return number


def check_object_belong_to_cust(customerid, objectname):
    df = define_df(objectname)
    counter = 0

    while counter <= 10:
        list_obj = find_object_for_customer_id(customerid=customerid, objectname=objectname)
        if not list_obj:
            print('You do not have any existing {} open. Please restart and open a {}'.format(objectname, objectname))
            sys.exit()

        else:
            if objectname == 'account':
                object_of_interest = input('please select ' + objectname)
            else:
                object_of_interest = read_int('please select ' + objectname)
            col = objectname + 'num'
            a = list(df.loc[df[col] == object_of_interest, "customerid"])
            try:
                if a[0] != customerid:
                    print('You do not have permission to access this {} , please select {} from options'.format(
                        objectname, objectname))
                    counter += 1
                else:
                    return object_of_interest
            except IndexError:
                print('please select valid {}'.format(objectname))
                counter += 1
    exit_func()
