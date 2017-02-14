import csv
from os import listdir
from os.path import isfile, join

base_path = "/Users/vladyslav/Documents/Study/Trento/ResearchProj/data/"

files = [f for f in listdir(base_path) if isfile(join(base_path, f))]

data = []
prices = []
for file in files:
    path = base_path + file

    csv_file = open(path)
    data_sheet = csv.reader(csv_file)

    for row in data_sheet:
        row = list(map(float, row))

        if row[2] < 0:
            continue

        id = int(row[3])
        price = round(((row[0] % 1) * 1000 - 270)/5 + 1)

        if row[1] == 9876:
            price = 0
            volume = row[2]
        elif row[1] > 0:
            volume = abs(row[2])
        elif row[1] < 0:
            volume = abs(row[2]) * -1
        else:
            volume = 0

        data.append([id, price, int(volume)])

    csv_file.close()


with open('data.csv', 'w', newline='') as data_file:
    a = csv.writer(data_file, delimiter=',')
    a.writerows(data)

print("done")