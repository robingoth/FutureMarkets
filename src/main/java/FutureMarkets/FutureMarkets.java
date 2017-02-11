package FutureMarkets;

import org.apache.commons.codec.StringDecoder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hyperledger.java.shim.ChaincodeBase;
import org.hyperledger.java.shim.ChaincodeStub;
import org.hyperledger.protos.TableProto;

import java.util.ArrayList;
import java.util.List;

import java.util.*;
import java.lang.Math;

public class FutureMarkets extends ChaincodeBase {

    private static Log log = LogFactory.getLog(FutureMarkets.class);
    private int roundCount = 0;
    private int maxRound = 0;
    static int maxPrice = 100;
    static int maxVolume = 1500;

    private HelperMethods helper = new HelperMethods();

    @java.lang.Override
    public String run(ChaincodeStub stub, String function, String[] args) {
        log.info("In run, function:" + function);

        // convert all arguments from string to int
        int[] args_i = new int[args.length];
        for (int i = 0; i < args.length; i++) {
            try {
                args_i[i] = Integer.parseInt(args[i]);
                //log.info(String.format("%1$s -> %2$d", args[i], args_i[i]));
            } catch (NumberFormatException e){
                log.error(String.format("Illegal argument %1$s. Exception message %2$s", args[i], e.getMessage()));
                return null;
            }
        }

        switch (function) {
            case "dummy":
                deleteTrader(stub, args_i[0]);

                break;
            case "init":
                // args = tradersMoney, maxPrice, maxVolume, maxRound
                this.maxRound = args_i[1];

                log.info(String.format("Global values for this run are the following:" +
                        "\nmaxPrice = %1$d\nmaxVolume = %2$d\nmaxRound = %3$d", maxPrice, maxVolume, this.maxRound));

                init(stub, args_i[0]);
                break;
            case "post_order":
                // args = traderID, price, volume
                // or traderID, volume
                log.debug("Round before #" + String.valueOf(roundCount));

                if (this.roundCount > this.maxRound) {
                    log.info("Current round number is higher than the maximum. Operation rejected.");
                } else if (this.roundCount < this.maxRound) {
                    boolean isRoundValid = helper.validateData(stub);

                    if (!isRoundValid) {
                        log.error("Current round is invalid. Check the data");
                    }

                    postOrder(stub, args_i, false);
                    buildOrderBook(stub);

                    boolean success = settleMargin(stub);

                    if (!success) {
                        log.error("Time for Mark To Market");
                        markToMarket(stub);
                    }
                }
                log.debug("Round after #" + String.valueOf(roundCount));

                if (this.roundCount == this.maxRound) {
                    log.info("Time for Mark To Market");
                    markToMarket(stub);
                    this.roundCount++;
                }

                break;
            case "update_order":
                // args = orderID, traderID, price, volume
                // or orderID, traderID, volume
                postOrder(stub, args_i, true);
                buildOrderBook(stub);
                break;
            case "cancel_order":
                // args = orderID
                log.debug("Round before #" + String.valueOf(roundCount));

                if (this.roundCount > this.maxRound){
                    log.info("Current round number is higher than the maximum. Operation rejected.");
                } else if (this.roundCount < this.maxRound) {
                    boolean isCancelled = cancelLimitOrder(stub, args_i[0]);

                    buildOrderBook(stub);

                    boolean success = settleMargin(stub);

                    if (!success) {
                        log.error("Time for Mark To Market");
                        markToMarket(stub);
                    }
                }

                log.debug("Round after #" + String.valueOf(roundCount));

                if (this.roundCount == this.maxRound) {
                    log.info("Time for Mark To Market");
                    markToMarket(stub);
                    this.roundCount++;
                }

                break;
            case "add_trader":
                deposit(stub, args_i, false);
                break;
            // TODO: values provided must represent value to be deposited(existing + provided), not a redefinition of values
            case "deposit":
                deposit(stub, args_i, true);
                break;
            case "delete":
                // args = id, option : {-1 = trader, 0 = order, 1 = market_order}
                delete(stub, args_i);
                break;
            case "build_order_book":
                buildOrderBook(stub);
                break;
            case "clean":
                clean(stub);
                break;
            default:
                log.error("No matching case for function:" + function);
        }
        return null;
    }

    private void clean(ChaincodeStub stub) {
        HashMap<String, Integer> tables = new HashMap<>();
        tables.put(HelperMethods.marketOrders, 1);
        tables.put(HelperMethods.orderBook, 0);
        tables.put(HelperMethods.userTable, -1);

        Set set = tables.entrySet();
        Iterator iterator = set.iterator();
        while(iterator.hasNext()) {
            Map.Entry table = (Map.Entry)iterator.next();
            String tableName = table.getKey().toString();

            ArrayList<int[]> rows = helper.queryTable(stub, tableName);

            if (rows.size() == 0)
                continue;

            for (int i = rows.size() - 1; i >= 0; i--) {
                delete(stub, new int[]{rows.get(i)[0], tables.get(tableName)});
            }
        }

        this.maxRound = 0;
        this.roundCount = 0;
    }

    private void deposit(ChaincodeStub stub, int[] args, boolean update) {
        int fieldID = 0;
        int money_amount = 0;
        int volume = 0;

        if (update) {
            fieldID = args[0];
            money_amount = args[1];
            volume = args[2];
        } else {
            fieldID = helper.getTableSize(stub, HelperMethods.userTable) + 1;
            money_amount = args[0];
            volume = args[1];
        }


        TableProto.Column col1 =
                TableProto.Column.newBuilder()
                        .setUint32(fieldID).build();
        TableProto.Column col2 =
                TableProto.Column.newBuilder()
                        .setInt32(money_amount).build();
        TableProto.Column col3 =
                TableProto.Column.newBuilder()
                        .setInt32(volume).build();
        List<TableProto.Column> cols = new ArrayList<TableProto.Column>();
        cols.add(col1);
        cols.add(col2);
        cols.add(col3);

        TableProto.Row row = TableProto.Row.newBuilder()
                .addAllColumns(cols)
                .build();
        try {
            boolean success = false;
            if(update)
            {
                success = stub.replaceRow(HelperMethods.userTable,row);
            }
            else
            {
                log.info(String.format("Adding trader %1$d with amount %2$d and volume %3$d", fieldID, money_amount, volume));
                success = stub.insertRow(HelperMethods.userTable, row);
            }

            if (success){
                log.info("Row successfully inserted");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String postOrder(ChaincodeStub stub, int[] args, boolean update) {
        String tableName = "";

        int orderID = 0;
        int traderID = 0;
        int price = 0;
        int volume = 0;

        List<TableProto.Column> cols = new ArrayList<TableProto.Column>();

        boolean isMarket = false;

        //log.info("Number of arguments = " + args.length);
        if ((args.length == 3 && update) || (args.length == 2 && !update)) {
            log.info("Type of order = Market");
            isMarket = true;
        }

        if (isMarket) {
            tableName = HelperMethods.marketOrders;

            if (update) {
                orderID = args[0];
                traderID = args[1];
                volume = args[2];
            } else {
                orderID = helper.getTableSize(stub, tableName) + 1;
                traderID = args[0];
                volume = args[1];
            }
        } else {
            tableName = HelperMethods.orderBook;

            if (update) {
                orderID = args[0];
                traderID = args[1];
                price = args[2];
                volume = args[3];
            } else {
                orderID = helper.getTableSize(stub, tableName) + 1;
                traderID = args[0];
                price = args[1];
                volume = args[2];
            }
        }

        int remainder = 0;
        // validation,
        // immediate match
        if (!isMarket && !update) {
            //validation
            int[] new_order = new int[]{orderID, traderID, price, volume};
            boolean isOrderValid = helper.validateOrder(stub, new_order);

            if (!isOrderValid) {
                log.error(String.format("Order %1$s is invalid", Arrays.toString(new_order)));
                return null;
            }  else {
                this.roundCount++;
            }

            do {
                //log.info("\nQuerying orders\n");
                ArrayList<int[]> orders = helper.queryTable(stub, HelperMethods.orderBook);

                int i = 0;
                for (i = 0; i < orders.size(); i++) {
                    //log.info(String.format("\ni = %1$d\n", i));
                    int[] order = orders.get(i);
                    new_order = new int[]{orderID, traderID, price, volume};

                    if (price == order[2] && volume * order[3] < 0) {
                        //log.info("\ntransaction\n");
                        remainder = transaction(stub, new_order, order);
                        //log.info(String.format("\nremainder = %1$d\n", remainder));
                        volume = remainder;
                        break;
                    }
                }

                orderID = helper.getTableSize(stub, tableName) + 1;
                //log.info(String.format("\norder id = %1$d\n", orderID));

                //log.info(String.format("\ni = %1$d\nsize = %2$d\n", i, orders.size()));
                if (i == orders.size())
                    break;
            } while (remainder != 0);

            if (volume == 0) {
                log.info("Order was totally fulfilled, therefore not posted");
                return null;
            }
        } else if (isMarket && !update) {
            int[] new_order = new int[]{orderID, traderID, volume};

            boolean isBuy = true;
            if (volume > 0)
                isBuy = false;

            int bestPrice = helper.findBestPriceOrder(stub, isBuy, traderID)[2];

            boolean isOrderValid = helper.validateOrder(stub, new int[]{orderID, traderID, bestPrice, volume});

            if (!isOrderValid) {
                log.error(String.format("Order %1$s is invalid", Arrays.toString(new_order)));
                return null;
            }
            int[] matchRes = matchMarketOrder(stub, new_order);

            remainder = matchRes[0];
            int numOfTransactions = matchRes[1];

            if (remainder == 0 && numOfTransactions != 0) {
                log.info("Order was totally fulfilled, therefore not posted");
                return null;
            } else if (remainder == 0 && numOfTransactions == 0)
                remainder = volume;
        }

        if (!isMarket) {
            TableProto.Column col1 =
                    TableProto.Column.newBuilder()
                            .setUint32(orderID).build();
            TableProto.Column col2 =
                    TableProto.Column.newBuilder()
                            .setInt32(traderID).build();
            TableProto.Column col3 =
                    TableProto.Column.newBuilder()
                            .setInt32(price).build();


            if (remainder != 0) {
                volume = remainder;
            }

            TableProto.Column col4 =
                    TableProto.Column.newBuilder()
                            .setInt32(volume).build();

            cols.add(col1);
            cols.add(col2);
            cols.add(col3);
            cols.add(col4);
        } else {
            TableProto.Column col1 =
                    TableProto.Column.newBuilder()
                            .setUint32(orderID).build();
            TableProto.Column col2 =
                    TableProto.Column.newBuilder()
                            .setInt32(traderID).build();

            if (remainder != 0) {
                volume = remainder;
            }

            TableProto.Column col3 =
                    TableProto.Column.newBuilder()
                            .setInt32(volume).build();

            cols.add(col1);
            cols.add(col2);
            cols.add(col3);
        }

        TableProto.Row row = TableProto.Row.newBuilder()
                .addAllColumns(cols)
                .build();
        try {
            boolean success = false;
            if (update) {
                stub.replaceRow(tableName, row);
            } else {
                success = stub.insertRow(tableName, row);
                if (success) {
                    log.info(String.format("Adding order %1$s for trader %2$d",
                            Arrays.toString(new int[]{orderID, traderID, price, volume}), traderID));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    private void init(ChaincodeStub stub, int money) {
        int resUserTable = helper.buildTable(stub, HelperMethods.userTable, new String[]{"ID", "Money", "Volume"});
        if (resUserTable < 0)
        {
            log.error("Error creating UserTable");
        }

        int resOrderBook = helper.buildTable(stub, HelperMethods.orderBook, new String[]{"OrderID", "TraderID", "Price", "Quantity"});
        if (resOrderBook < 0)
        {
            log.error("Error creating OrderBook");
        }

        int resMarketOrders = helper.buildTable(stub, HelperMethods.marketOrders, new String[]{"OrderID", "TraderID", "Quantity"});
        if (resMarketOrders < 0)
        {
            log.error("Error creating MarketOrders");
        }

        for (int i = 1; i <= 5; i++)
        {
            deposit(stub, new int[]{money, 0}, false);
        }
    }

    /*
    options:
    -1 - trader
    0 - limit order
    1 - market order
     */
    // TODO when deleting a trader, traderID column of limit orders have to be updated as well
    private boolean delete(ChaincodeStub stub, int[] args){
        int option = args[1];
        int fieldID = args[0];

        TableProto.Column queryCol =
                TableProto.Column.newBuilder()
                        .setUint32(fieldID).build();
        List<TableProto.Column> key = new ArrayList<>();
        key.add(queryCol);

        String tableName = "";
        switch (option)
        {
            case -1: // trader
                tableName = HelperMethods.userTable;
                break;
            case 0: // order
                tableName = HelperMethods.orderBook;
                break;
            case 1: // market order
                tableName = HelperMethods.marketOrders;
                break;
        }

        if (tableName.equals(""))
        {
            log.error(String.format("Table with name %1$s does not exist", option));
            return false;
        }

        boolean result;
        ArrayList<int[]> rows = helper.queryTable(stub, tableName);
        if (rows.size() >= fieldID) {
            result = stub.deleteRow(tableName, key);
        } else {
            log.error(String.format("Field id %1$d does not exist in table %2$s", fieldID, tableName));
            return false;
        }

        if (result)
        {
            log.info(String.format("The row with id = %1$d successfully deleted from table %2$s", fieldID, tableName));

            if (rows.size() != 1 && fieldID != rows.size()) {
                // insert fieldID + 1
                int[] firstRow = rows.get(fieldID);
                int numOfCols  = firstRow.length;

                List<TableProto.Column> cols = new ArrayList<TableProto.Column>();

                TableProto.Column col1 =
                        TableProto.Column.newBuilder()
                                .setUint32(fieldID).build();

                cols.add(col1);

                for (int j = 1; j < numOfCols; j++) {
                    TableProto.Column col =
                            TableProto.Column.newBuilder()
                                    .setInt32(firstRow[j]).build();
                    cols.add(col);
                }

                TableProto.Row rowInsert = TableProto.Row.newBuilder()
                        .addAllColumns(cols)
                        .build();

                try {
                    log.debug(String.format("inserting row %1$s", Arrays.toString(firstRow)));
                    result = stub.insertRow(tableName, rowInsert);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                // update all subsequent rows
                for (int i = fieldID + 1; i < rows.size(); i++) {
                    int[] row = rows.get(i);

                    List<TableProto.Column> updateCols = new ArrayList<TableProto.Column>();

                    TableProto.Column colID =
                            TableProto.Column.newBuilder()
                                    .setUint32(i).build();

                    updateCols.add(colID);

                    for (int j = 1; j < row.length; j++) {
                        TableProto.Column udateCol =
                                TableProto.Column.newBuilder()
                                        .setInt32(row[j]).build();
                        updateCols.add(udateCol);
                    }

                    TableProto.Row rowUpdate = TableProto.Row.newBuilder()
                            .addAllColumns(updateCols)
                            .build();

                    try {
                        log.debug(String.format("updating row %1$s", Arrays.toString(row)));
                        result = stub.replaceRow(tableName, rowUpdate);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                // delete last row
                TableProto.Column delCol =
                        TableProto.Column.newBuilder()
                                .setUint32(rows.size()).build();
                List<TableProto.Column> delKey = new ArrayList<>();
                delKey.add(delCol);

                log.debug(String.format("Deleting row with id = %1$d", rows.size()));
                result = stub.deleteRow(tableName, delKey);

                if (!result) {
                    log.error("An error happened while updating IDs");
                }
            }
        }
        else
        {
            log.error(String.format("An arror occured when deleting the row with id = %1$d from table %2$s", fieldID, tableName));
        }

        return result;
    }

    private boolean deleteTrader(ChaincodeStub stub, int traderID) {
        if (helper.getTraderOrders(stub, traderID).size() != 0){
            log.error(String.format("Trader %1$d has orders in order book. Deletion not permitted.", traderID));
            return false;
        }

        int numOfTraders = helper.getTableSize(stub, HelperMethods.userTable);
        List<Integer> tradersToUpdate = new ArrayList<>();

        //create list of traders to update
        for (int i = 1; i <= numOfTraders - traderID; i++) {
            tradersToUpdate.add(traderID + i);
        }

        ArrayList<int[]> ordersToUpdate = new ArrayList<>();
        ArrayList<int[]> orders = helper.queryTable(stub, HelperMethods.orderBook);
        for (int[] order : orders) {
            if (tradersToUpdate.contains(order[1])) {
                order[1]--;
                ordersToUpdate.add(order);
            }
        }

        for (int [] order : ordersToUpdate) {
            postOrder(stub, order, true);
        }

        delete(stub, new int[]{traderID, -1});

        return true;
    }

    private boolean cancelLimitOrder(ChaincodeStub stub, int orderID) {
        boolean isPermitted = helper.isCancelPermitted(stub, orderID);

        if (!isPermitted)
            return false;
        else
            this.roundCount++;

        return delete(stub, new int[]{orderID, 0});
    }

    private int[] matchMarketOrder(ChaincodeStub stub, int[] order) {
        int remainder = 0;
        int numOfTransactions = 0;
        int[] result = new int[]{remainder, numOfTransactions};

        boolean isBuy;

        if (order[2] > 0)
            isBuy = false;
        else
            isBuy = true;

        do {
            int[] bestPriceOrder = helper.findBestPriceOrder(stub, isBuy, order[1]);

            if (bestPriceOrder[0] == -1) {
                result[0] = remainder;
                result[1] = numOfTransactions;
                return result;
            } else if (bestPriceOrder[0] != 0) {
                remainder = transaction(stub, order, bestPriceOrder);
                order[2] = remainder;
                numOfTransactions++;

                result[0] = remainder;
            } else {
                break;
            }
        } while (remainder != 0);

        return result;
    }

    private int transaction(ChaincodeStub stub, int[] newOrder, int[] existingOrder) {
        log.info(String.format("Matching two orders:\n%1$s\n%2$s", Arrays.toString(newOrder),
                Arrays.toString(existingOrder)));

        int traderID = newOrder[1];
        int matchedTraderID = existingOrder[1];

        int[] trader = helper.getTrader(stub, newOrder[1]);
        int[] matchedTrader = helper.getTrader(stub, existingOrder[1]);

        int traderMoney = trader[1];
        int matchedTraderMoney = matchedTrader[1];

        int traderVolume = trader[2];
        int matchedTraderVolume = matchedTrader[2];

        int price = existingOrder[2];

        int volume = newOrder[newOrder.length - 1];
        int existingOrderVolume = existingOrder[3];
        int matchedVolume = Math.min(Math.abs(volume), Math.abs(existingOrderVolume));

        if (volume > 0) {
            traderMoney -= price * matchedVolume;
            traderVolume += matchedVolume;

            matchedTraderMoney += price * matchedVolume;
            matchedTraderVolume -= matchedVolume;

            volume -= matchedVolume;
            existingOrderVolume += matchedVolume;
        } else {
            traderMoney += price * matchedVolume;
            traderVolume -= matchedVolume;

            matchedTraderMoney -= price * matchedVolume;
            matchedTraderVolume += matchedVolume;

            volume += matchedVolume;
            existingOrderVolume -= matchedVolume;
        }

        if (existingOrderVolume == 0) {
            delete(stub, new int[]{existingOrder[0], 0});
        }
        else {
            existingOrder[existingOrder.length - 1] = existingOrderVolume;
            postOrder(stub, existingOrder, true);
        }

        /*
        if (volume == 0) {
            if (newOrder.length == 3)
                delete(stub, new int[]{newOrder[0], 1});
            else if (newOrder.length == 4)
                delete(stub, new int[]{newOrder[0], 0});
        } else {
            newOrder[newOrder.length - 1] = volume;
            postOrder(stub, newOrder, true);
        }
        */

        if (traderID != matchedTraderID)
        {
            // updating traders
            trader[1] = traderMoney;
            trader[2] = traderVolume;
            log.info(String.format("Updating trader who posted order:\n%1$s", Arrays.toString(trader)));

            deposit(stub, trader, true);

            matchedTrader[1] = matchedTraderMoney;
            matchedTrader[2] = matchedTraderVolume;
            log.info(String.format("Updating trader who got matched:\n%1$s", Arrays.toString(matchedTrader)));

            deposit(stub, matchedTrader, true);
        }

        return volume;
    }

    private void buildOrderBook(ChaincodeStub stub) {
        ArrayList<int[]> rows = helper.queryTable(stub, HelperMethods.orderBook);

        ArrayList<int[]> rows_buy = new ArrayList<int[]>();
        ArrayList<int[]> rows_sell = new ArrayList<int[]>();

        for (int[] row : rows)
        {
            int volume = row[3];
            if(volume > 0)
            {
                rows_buy.add(row);
            }
            else
            {
                rows_sell.add(row);
            }
        }

        Collections.sort(rows_buy, new Comparator<int[]>() {
            public int compare(int[] a, int[] b) {
                return a[2] - b[2];
            }
        });

        Collections.sort(rows_sell, new Comparator<int[]>() {
            public int compare(int[] a, int[] b) {
                return a[2] - b[2];
            }
        });

        // insert rows
        int j = 1;
        for (int[] row_buy : rows_buy) {
            row_buy[0] = j;
            postOrder(stub, row_buy, true);

            j++;
        }

        for (int[] row_sell : rows_sell) {
            row_sell[0] = j;
            postOrder(stub, row_sell, true);

            j++;
        }
    }

    private boolean settleMargin(ChaincodeStub stub) {
        ArrayList<int[]> traders = helper.queryTable(stub, HelperMethods.userTable);
        ArrayList<int[]> brokeTraders = new ArrayList<>();

        for (int[] trader : traders) {
            int netValue = helper.netValue(stub, trader[0]);

            if (netValue < 0)
                brokeTraders.add(trader);
        }

        if (brokeTraders.size() == 0) {
            log.info("No traders are broke");
            return true;
        }


        boolean canSupply = true;
        for (int[] brokeTrader : brokeTraders) {
            ArrayList<int[]> traderOrders = helper.getTraderOrders(stub, brokeTrader[0]);
            int traderVolume = brokeTrader[2];

            for (int i = traderOrders.size() - 1; i >= 0; i--) {
                delete(stub, new int[]{traderOrders.get(i)[0], 0});
            }

            int totalSellVolume = Math.abs(helper.getTotalSellVolume(stub));
            int totalBuyVolume = helper.getTotalBuyVolume(stub);

            log.info(String.format("Trader volume = %1$d", traderVolume));
            log.info(String.format("Total buy volume = %1$d", totalBuyVolume));
            log.info(String.format("Total sell volume = %1$d", totalSellVolume));

            if ((traderVolume < 0 && Math.abs(traderVolume) > totalSellVolume) || (traderVolume > 0 && traderVolume > totalBuyVolume)) {
                log.error("Market cannot supply the margin settlement for all traders");
                canSupply = false;
                break;
            }

            if (traderVolume != 0)
                postOrder(stub, new int[]{brokeTrader[0], -1 * brokeTrader[2]}, false);

            deleteTrader(stub, brokeTrader[0]);
        }

        return canSupply;
    }

    private void markToMarket (ChaincodeStub stub) {
        ArrayList<int[]> traders = helper.queryTable(stub, HelperMethods.userTable);
        int midPrice = helper.findMidPrice(stub);

        for (int[] trader: traders) {
            int traderVolume = trader[2];
            int traderMoney = trader[1];

            traderMoney += traderVolume * midPrice;
            deposit(stub, new int[]{trader[0], traderMoney, 0}, true);
        }
    }

    @java.lang.Override
    public String query(ChaincodeStub stub, String function, String[] args) {
        log.info("query");

        switch (function){
            case "order_book": {
                // query rows
                ArrayList<int[]> rows = helper.queryTable(stub, HelperMethods.orderBook);

                String result = "\norderID\t\ttraderID\t\tprice\t\tvolume\t\n";
                for (int[] row : rows) {
                    result += String.format("   %1$d\t\t   %2$d\t\t\t %3$d\t\t   %4$d\n", row[0], row[1], row[2], row[3]);
                }
                return result;
            }
            case "market_orders": {
                // query rows
                ArrayList<int[]> rows = helper.queryTable(stub, HelperMethods.marketOrders);

                String result = "\norderID\t\ttraderID\t\tvolume\t\n";
                for (int[] row : rows) {
                    result += String.format("   %1$d\t\t   %2$d\t\t\t %3$d\n", row[0], row[1], row[2]);
                }
                return result;
            }
            case "traders": {
                // query rows
                ArrayList<int[]> rows = helper.queryTable(stub, HelperMethods.userTable);

                String result = "\ntraderID\tmoney\t\t\tvolume\n";
                for (int[] row : rows) {
                    result += String.format("   %1$d\t\t   %2$d\t\t\t %3$d\n", row[0], row[1], row[2]);
                }
                return result;
            }
            default:
                log.error("No matching case for function:"+function);
                return "";
        }
    }

    @java.lang.Override
    public String getChaincodeID() {
        return "FutureMarkets";
    }

    public static void main(String[] args) throws Exception {
        log.info("starting");
        new FutureMarkets().start(args);
    }

}