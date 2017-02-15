## FutureMarkets
This is an implementation of FuturesMEX protocol for hyperledger fabric.

## Usage
### Development mode
In development mode you can see the log for debugging and test the functionality of the program. I assume you followed the Java chaincode setup guide provided by hyperledger fabric team.
Steps are following:

1. `cd /path/to/fabric/devenv` 
2. `vagrant up`, this might take few minutes 
3. `vagrant ssh` 
4. `peer node start --peer-chaincodedev`
5. In a second terminal do steps 1 and 3.
6. *cd* to $GOPATH/src/github.com/hyperledger/fabric/examples/chaincode/java/FutureMarkets/
7. `gradle run`
8. In the third terminal do steps 1 and 3. Now you should be able to use existing commands to interact with the chaincode.

### Docker mode
You can use this mode to do the parallel execution of commands or just if you don't want to run fabric in development mode.
In order to simulate the behavior of 4 traders you should do the following:

1. `cd /path/to/docker-compose.yml/`
2. `docker-compose up`. At this point you should see log of your peers
3. In another terminal do step 1.
4. To deploy FutureMarkets project, run the following command:
   `docker-compose exec vp1 peer chaincode deploy -l java -p /opt/gopath/src/github.com/hyperledger/fabric/examples/chaincode/java/FutureMarkets -c '{"Function": "init", "Args": ["1000", "1000"]}'`
   As a responce you will get a hash string like `44818ddf3bb5252669b9b2ef478b1c639e98f59522472543ee5495da0f3d6c2b40e4c7f559a8a77216a99904ee8e1e76c3c13837b07bcba3c46680a82a2a6149`
   This hash string will be used in the next commands.
5. In the same terminal copy and paste the following command without running it
   `docker-compose exec vp1 python /opt/gopath/src/github.com/hyperledger/fabric/examples/chaincode/java/FutureMarkets/post_order.py 44818ddf3bb5252669b9b2ef478b1c639e98f59522472543ee5495da0f3d6c2b40e4c7f559a8a77216a99904ee8e1e76c3c13837b07bcba3c46680a82a2a6149 0 1`
6. Do the same in other three terminal windows for three following commands:
   `docker-compose exec vp2 python /opt/gopath/src/github.com/hyperledger/fabric/examples/chaincode/java/FutureMarkets/post_order.py 44818ddf3bb5252669b9b2ef478b1c639e98f59522472543ee5495da0f3d6c2b40e4c7f559a8a77216a99904ee8e1e76c3c13837b07bcba3c46680a82a2a6149 0 2`
   `docker-compose exec vp3 python /opt/gopath/src/github.com/hyperledger/fabric/examples/chaincode/java/FutureMarkets/post_order.py 44818ddf3bb5252669b9b2ef478b1c639e98f59522472543ee5495da0f3d6c2b40e4c7f559a8a77216a99904ee8e1e76c3c13837b07bcba3c46680a82a2a6149 0 3`
   `docker-compose exec vp4 python /opt/gopath/src/github.com/hyperledger/fabric/examples/chaincode/java/FutureMarkets/post_order.py 44818ddf3bb5252669b9b2ef478b1c639e98f59522472543ee5495da0f3d6c2b40e4c7f559a8a77216a99904ee8e1e76c3c13837b07bcba3c46680a82a2a6149 0 4`
