import csv
import sys
from collections import defaultdict

f = open('query_time.csv', 'rb')
reader = csv.reader(f)
time_index = defaultdict(int)
count_index = defaultdict(int)
time = 0
count = 0
for row in reader:
    if len(row) >0:
        if 'csv' in row[0]:
            print(row)
        elif 'endquery' in row[0]:
            for key in time_index.iterkeys():
                print("Time: ",key, time_index[key]/count_index[key])
            time_index = defaultdict(int)
            count_index = defaultdict(int)
            print("Total: ", time/count)
            time = 0
            count = 0
        elif 'query' in row[0]:
            time_index[row[6]] += float(row[7])
            count_index[row[6]] += 1
            time += float(row[7])
            count +=1


f.close()