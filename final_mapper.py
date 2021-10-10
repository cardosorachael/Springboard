
import sys

for line in sys.stdin:
    line = line.strip()
    data = line.split(',')
    print(f"{data[2]}\t{(data[1], data[3], data[5])}")