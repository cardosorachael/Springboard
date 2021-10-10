import sys

acc_count_info = {}

def flush():
    """
    Output of function is to return combination of make and year as key and count as value.
    :return:
    """
    for key in acc_count_info.keys():
        print(f'{key}\t{acc_count_info[key]}')


for line in sys.stdin:
    line = line.strip()
    make_year, acc_count = line.split('\t')
    acc_count = int(acc_count.replace("'", "").replace("(", "").replace(")", ""))
    if make_year not in acc_count_info:
        acc_count_info[make_year] = 0
    acc_count_info[make_year] += acc_count

flush()