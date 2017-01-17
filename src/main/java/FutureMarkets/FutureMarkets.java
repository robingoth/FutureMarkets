package FutureMarkets;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hyperledger.java.shim.ChaincodeBase;
import org.hyperledger.java.shim.ChaincodeStub;
import org.hyperledger.protos.TableProto;

import java.util.ArrayList;
import java.util.List;

import java.util.*;
import java.lang.Math;

import FutureMarkets.HelperMethods;

// TODO all args should be int, not string
public class FutureMarkets extends ChaincodeBase {


    private static Log log = LogFactory.getLog(FutureMarkets.class);
    private HelperMethods helper = new HelperMethods();


    @java.lang.Override
    public String run(ChaincodeStub stub, String function, String[] args) {
        log.info("In run, function:"+function);

        int[] args_i = new int[args.length];
        for (int i = 0; i < args.length; i++) {
            try {
                args_i[i] = Integer.parseInt(args[i]);
                //log.info(String.format("%1$s -> %2$d", args[i], args_i[i]));
            } catch (NumberFormatException e){
                log.error(String.format("Illegal argument %1$s. Exception message %2$s", args[i], e.getMessage()));
                System.exit(0);
            }
        }

        switch (function) {
            case "init":
                init(stub);
                break;
            case "post_order":
                // args = traderID, price, volume
                // or traderID, volume
                postOrder(stub, args_i, false);
                buildOrderBook(stub);
                break;
            case "update_order":
                // args = orderID, traderID, price, volume
                // or orderID, traderID, volume
                postOrder(stub, args_i, true);
                buildOrderBook(stub);
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
            default:
                log.error("No matching case for function:"+function);

        }
        return null;
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
            fieldID = helper.getTableSize(stub, helper.userTable) + 1;
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
    
    // TODO price and volume cannot be zero
    private void postOrder(ChaincodeStub stub, int[] args, boolean update) {
        String tableName = "";

        int orderID = 0;
        int traderID = 0;
        int price = 0;
        int volume = 0;

        List<TableProto.Column> cols = new ArrayList<TableProto.Column>();

        boolean isMarket = false;

        log.info("Number of arguments = " + args.length);
        if ((args.length == 3 && update) || (args.length == 2 && !update)) {
            log.info("Type of order = Market");
            isMarket = true;
        }

        if (!isMarket) {
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

            TableProto.Column col1 =
                    TableProto.Column.newBuilder()
                            .setUint32(orderID).build();
            TableProto.Column col2 =
                    TableProto.Column.newBuilder()
                            .setInt32(traderID).build();
            TableProto.Column col3 =
                    TableProto.Column.newBuilder()
                            .setInt32(price).build();
            TableProto.Column col4 =
                    TableProto.Column.newBuilder()
                            .setInt32(volume).build();

            cols.add(col1);
            cols.add(col2);
            cols.add(col3);
            cols.add(col4);
        } else {
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

            TableProto.Column col1 =
                    TableProto.Column.newBuilder()
                            .setUint32(orderID).build();
            TableProto.Column col2 =
                    TableProto.Column.newBuilder()
                            .setInt32(traderID).build();
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
                success = stub.replaceRow(tableName, row);
            } else {
                log.info(String.format("Adding order %1$d for trader %2$d",
                        orderID, traderID));
                success = stub.insertRow(tableName, row);
            }

            if (success) {
                log.info("Row successfully inserted");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


        // immediate match
        if (!isMarket && !update) {
            int[] new_order = new int[]{orderID, traderID, price, volume};
            ArrayList<int[]> orders = helper.queryTable(stub, HelperMethods.orderBook);
            for (int[] order : orders) {
                if (price == order[2] && volume * order[3] < 0 && traderID != order[1]) {
                    if (volume > 0) {
                        transaction(stub, new_order, order);
                    } else {
                        transaction(stub, order, new_order);
                    }

                }
            }
        } else if (isMarket && !update) {
            int[] new_order = new int[]{orderID, traderID, volume};
            matchMarketOrder(stub, new_order);
        }
    }

    private void init(ChaincodeStub stub) {
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
            deposit(stub, new int[]{1000, 100}, false);
        }
    }

    /*
    options:
    -1 - trader
    0 - order
    1 - market order
     */
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

        ArrayList<int[]> rows = helper.queryTable(stub, tableName);
        boolean result = stub.deleteRow(tableName, key);

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
                    log.info(String.format("inserting row %1$s", Arrays.toString(firstRow)));
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
                        log.info(String.format("updating row %1$s", Arrays.toString(row)));
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

                log.info(String.format("Deleting row with id = %1$d", rows.size()));
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

    // TODO: implement recursion for a market order,
    // i.e. perform transaction while volume of the market order is > 0
    public void matchMarketOrder(ChaincodeStub stub, int[] order) {
        if (order[order.length - 1] > 0) {
            // bying
            log.info("Bying stuff");
            int[] sellOrder = helper.findBestPrice(stub, false);
            log.info(String.format("Matching two orders:\n%1$s\n%2$s", Arrays.toString(order), Arrays.toString(sellOrder)));
            transaction(stub, order, sellOrder);
        } else {
            int[] buyOrder = helper.findBestPrice(stub, true);
            log.info(String.format("Matching two orders:\n%1$s\n%2$s", Arrays.toString(buyOrder), Arrays.toString(order)));
            transaction(stub, buyOrder, order);
        }
    }

    public void transaction(ChaincodeStub stub, int[] orderBuy, int[] orderSell) {
        log.info(String.format("Matching orders:\n%1$s\n%2$s", Arrays.toString(orderBuy), Arrays.toString(orderSell)));

        int[] buyer = helper.getTrader(stub, orderBuy[1]);
        int[] seller = helper.getTrader(stub, orderSell[1]);

        int buyerMoney = buyer[1];
        int sellerMoney = seller[1];

        int buyerVolume = buyer[2];
        int sellerVolume = seller[2];

        int requestedVolume = 0;
        int volumeForSale = 0;

        boolean isSellingOrderMarket = false;
        boolean isMarket = false;

        //set price based on market / regular order
        int price = 0;
        if (orderBuy.length == 3) {
            // buying order is a market order
            price = orderSell[2];
            requestedVolume = Math.abs(orderBuy[2]);
            volumeForSale = Math.abs(orderSell[3]);

            isMarket = true;
        } else if (orderSell.length == 3) {
            // selling order is a market order
            price = orderBuy[2];
            requestedVolume = Math.abs(orderBuy[3]);
            volumeForSale = Math.abs(orderSell[2]);

            isSellingOrderMarket = true;
            isMarket = true;
        } else {
            price = (orderBuy[2] + orderSell[2]) / 2;
            requestedVolume = Math.abs(orderBuy[3]);
            volumeForSale = Math.abs(orderSell[3]);
        }

        if (requestedVolume < 0 || volumeForSale > 0)
            log.error(String.format("One of the orders are incorrect.\nVolume for sale: %1$d\nRequested volume: %2$d",
                    volumeForSale, requestedVolume));

        // buyer has enough money?
        if (buyerMoney - requestedVolume*price > 0) {
            // seller has enough volume?
            if (Math.abs(volumeForSale) - requestedVolume >= 0) {
                sellerVolume -= requestedVolume;
                sellerMoney += requestedVolume*price;

                buyerVolume += requestedVolume;
                buyerMoney -= requestedVolume*price;

                volumeForSale += requestedVolume;

                log.info("price = " + price);
                log.info("sellerVolume = " + sellerVolume);
                log.info("buyerVolume = " + buyerVolume);
                log.info("buyerMoney = " + buyerMoney);
                log.info("volumeForSale = " + volumeForSale);
                log.info("requestedVolume = " + requestedVolume);

                requestedVolume = 0;
            } else {
                sellerMoney += price*volumeForSale;
                sellerVolume -= volumeForSale;

                buyerMoney -= price*volumeForSale;
                buyerVolume += volumeForSale;

                requestedVolume -= Math.abs(volumeForSale);

                log.info("price = " + price);
                log.info("sellerVolume = " + sellerVolume);
                log.info("buyerVolume = " + buyerVolume);
                log.info("buyerMoney = " + buyerMoney);
                log.info("volumeForSale = " + volumeForSale);
                log.info("requestedVolume = " + requestedVolume);

                volumeForSale = 0;
            }

            // updating orders
            orderBuy[1] = buyer[0];
            orderBuy[orderBuy.length - 1] = requestedVolume;

            orderSell[1] = seller[0];
            orderSell[orderSell.length - 1] = volumeForSale;

            log.info(String.format("Updating orders:\n%1$s\n%2$s", Arrays.toString(orderBuy), Arrays.toString(orderSell)));

            postOrder(stub, orderBuy, true);
            postOrder(stub, orderSell, true);

            //delete sell order
            int deletion_option = 0;
            if (volumeForSale == 0) {
                if (isSellingOrderMarket && isMarket)
                    deletion_option = 1; // market order

                log.info(String.format("Deleting order:\n%1$s", Arrays.toString(orderSell)));
                delete(stub, new int[]{orderSell[0], deletion_option});
            }

            //delete buy order
            deletion_option = 0;
            if (requestedVolume == 0) {
                if (!isSellingOrderMarket && isMarket)
                    deletion_option = 1; //market order

                log.info(String.format("Deleting order:\n%1$s", Arrays.toString(orderBuy)));
                delete(stub, new int[]{orderBuy[0], deletion_option});
            }

            // updating traders
            buyer[1] = buyerMoney;
            buyer[2] = buyerVolume;
            log.info(String.format("Updating trader who bought:\n%1$s", Arrays.toString(buyer)));

            seller[1] = sellerMoney;
            seller[2] = sellerVolume;
            log.info(String.format("Updating trader who sold:\n%1$s", Arrays.toString(seller)));

            deposit(stub, buyer, true);
            deposit(stub, seller, true);

            //buildOrderBook(stub);
        } else {
            int requiredAmount = price*requestedVolume;
            log.error(String.format("The required amount for this transaction is %1$d", requiredAmount));
        }
    }

    // TODO check behavior when two prices are same
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