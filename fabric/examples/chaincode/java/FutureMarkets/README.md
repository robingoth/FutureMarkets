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
