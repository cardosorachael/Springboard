import sys
master_info = {}

def flush():
    for key in master_info.keys():
        print(f'{(master_info[key]["make"], master_info[key]["year"])}\t{master_info[key]["accident_count"]}')
for line in sys.stdin:
    line = line.strip()
    vin_number, values = line.split('\t')
    values_list = [val.replace("'", "").replace("(", "").replace(")", "").replace(" ", "") for val in values.split(",")]
    incident_type = values_list[0]
    vehicle_make = values_list[1]
    vehicle_year = values_list[2]
    if vin_number not in master_info:
        master_info[vin_number] = {"make": None, "year": None, "accident_count": 0}
    if incident_type == "I":
        master_info[vin_number]["make"] = vehicle_make
        master_info[vin_number]["year"] = vehicle_year
    if incident_type == "A":
        master_info[vin_number]["accident_count"] += 1

flush()