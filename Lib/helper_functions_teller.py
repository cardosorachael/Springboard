from OOP_miniproject.Lib.helper_functions import define_df, exit_func



def check_password_teller(customerid, password, objectname):
    df = define_df(objectname)
    pass1 = list(df.loc[df['tellerid'] == customerid, 'password'])
    if pass1[0] == password:
        return True
    else:
        return False


def check_login_credentials_teller():
    counter = 0
    while (counter <=2):
        customerid = input('please enter tellerid')
        password = input('enter password')
        try:
            customerid = int(customerid)
            try:
                res = check_password_teller(customerid=customerid, password=password, objectname='teller')
                if res != True:
                    counter += 1
                    print("invalid login credentials, please try again. you have {} more tries".format(3 - counter))
                else:
                    print('login successful!')
                    return (customerid)

            except IndexError:
                counter += 1
                print("Id invalid, does not exist in db, please try again. you have {} more tries".format(3 - counter))

        except ValueError:
            counter += 1
            print("Id needs to be a number, please try again. you have {} more tries".format(3 - counter))
    exit_func()

def get_teller_name_from_db(customerid):
    df = define_df(objectname='teller')
    fname = df.loc[df['tellerid'] == customerid, 'first_name']
    lname = df.loc[df['tellerid'] == customerid, 'last_name']
    return fname, lname