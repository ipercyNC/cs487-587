
import csv
import random
import sys

def convert(unique):
    result = list("AAAAAAAxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    temp = list("AAAAAAA")
    i = 6
    cnt = 0
    while unique > 0:
        rem = unique % 26
        temp[i]=chr(ord('A')+rem)
        unique = int(unique/26)
        i = i-1
        cnt = cnt +1
    for j in range(0,cnt):
        result[j] = temp[6-j]
    return "".join(result)

def str4Select(index):
    switcher = {
        0: "AAAAxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
        1: "HHHHxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
        2: "OOOOxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
        3: "VVVVxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        }
    return switcher.get(index%4)

def filenameSelect(count):
    switcher = {
        1000: "onektup.csv",
        5000: "fivektup.csv",
        10000: "tenktup.csv",
        50000: "fiftyktup.csv",
        100000: "hundredktup.csv",
        500000: "fivehudredktup.csv",
        1000000: "onemtup.csv",
        5000000: "fivemtup.csv",
        10000000: "tenmtup.csv",
        100000000: "hundredmtup.csv",
        1000000000: "onebtup.csv",
        10000000000: "tenbtup.csv"
        }
    return switcher.get(count,str(count)+"tup.csv")

def datagen(count, filename):
    with open(filename, mode='w', newline='') as employee_file:
        tupleCount = count
        employee_writer = csv.writer(employee_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        #employee_writer.writerow(["unique1","unique2", "two", "four", "ten", "twenty", "onepercent", "tenpercent", "twentypercent", "fiftypercent", "unique3", "evenonepercent", "oddonepercent", "stringu1", "stringu2", "string4"])
        numList = random.sample(range(0,tupleCount),tupleCount)
        for i in range(tupleCount):
            unique1 = numList[i]
            unique2 = i
            two= unique1 % 2
            four = unique1 % 4
            ten = unique1 % 10
            twenty = unique1 % 20
            onePercent = unique1 % 100
            tenPercent = unique1 % 10
            twentyPercent = unique1 % 5
            fiftyPercent = unique1 % 2
            unique3 = unique1
            evenOnePercent = onePercent *2
            oddOnePercent = onePercent * 2 + 1
            stringu1 = convert(unique1)
            stringu2 = convert(unique2)
            string4 = str4Select(i)
            employee_writer.writerow([unique1, unique2, two, four, ten, twenty, onePercent, tenPercent, twentyPercent, fiftyPercent, unique3, evenOnePercent, oddOnePercent, stringu1, stringu2, string4])


def main(argv):
    if(len(sys.argv)<2):
        count = 10000
    else:
        count = int(sys.argv[1])
    filename = filenameSelect(count)
    #print(count, filename)
    datagen(count, filename)

if __name__ == "__main__":
    main(sys.argv[1:])
