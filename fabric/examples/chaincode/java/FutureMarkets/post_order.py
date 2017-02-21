from subprocess import Popen, PIPE
import sys
import time

if len(sys.argv) != 4:
    print("usage:\nscript.py chaincode_name number_of_orders trader_id")
    sys.exit()

cc_name = sys.argv[1]
count = int(sys.argv[2])
traderID = int(sys.argv[3])


pattern_limit = "peer chaincode invoke -l java -n {0} -c '{{\"Args\": [\"post_order\", \"{1}\", \"{2}\", \"{3}\"]}}';"
pattern_market = "peer chaincode invoke -l java -n {0} -c '{{\"Args\": [\"post_order\", \"{1}\", \"{2}\"]}}';"

lines = []

with open("/opt/gopath/src/github.com/hyperledger/fabric/examples/chaincode/java/FutureMarkets/data.csv") as file:
    data = [x.split(",") for x in file.read().split("\n")]

    if count == 0:
        count = len(data)

    for i in range(0, count):
        print(i)
        row = [int(x) for x in data[i]]

        if row[1] == 0:
            cmd = pattern_market.format(cc_name, row[0], row[2])
        else:
            cmd = pattern_limit.format(cc_name, row[0], row[1], row[2])

        if traderID != 0:
            if traderID == row[0]:
                pipe = Popen(cmd, shell=True)
                pipe.communicate()
        else:
            pipe = Popen(cmd, shell=True)
            pipe.communicate()
        #time.sleep(1)