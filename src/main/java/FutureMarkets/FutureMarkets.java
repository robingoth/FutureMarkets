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

public class FutureMarkets extends ChaincodeBase {

    private static final String userTable = "UserTable";
    private static final String orderBook = "OrderBook";
    private static final String marketOrders = "MarketOrders";
    private static Log log = LogFactory.getLog(FutureMarkets.class);

    @java.lang.Override
    public String run(ChaincodeStub stub, String function, String[] args) {
        log.info("In run, function:"+function);
        switch (function) {
            case "init":
                init(stub, function, args);
                break;
            case "post_order":
                // args = orderID, traderID, price, volume
                // or orderID, traderID, volume
                postOrder(stub, args, false);
                break;
            case "update_order":
                postOrder(stub, args, true);
                break;
            case "add_trader":
                deposit(stub, args, false);
                break;
            case "deposit":
                deposit(stub, args, true);
                break;
            case "delete":
                delete(stub, args);
                break;
            case "build_order_book":
                buildOrderBook(stub);
                break;
            case "match_market":
                matchMarketOrders(stub);
                break;
            default:
                log.error("No matching case for function:"+function);

        }
        return null;
    }

    private void deposit(ChaincodeStub stub, String[] args, boolean update) {
        int fieldID = 0;
        int money_amount = 0;
        int volume = 0;


        try {
            fieldID = Integer.parseInt(args[0]);
            money_amount = Integer.parseInt(args[1]);
            volume = Integer.parseInt(args[2]);
        }catch (NumberFormatException e){
            log.error("Illegal field id -" + e.getMessage());
            return;
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
                success = stub.replaceRow(userTable,row);
            }
            else
            {
                log.info(String.format("Adding trader %1$d with amount %2$d and volume %3$d", fieldID, money_amount, volume));
                success = stub.insertRow(userTable, row);
            }

            if (success){
                log.info("Row successfully inserted");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // TODO check if buyers have enough money for their order
    // TODO check if sellers have enough volume for their order
    // TODO price and volume cannot be zero
    private void postOrder(ChaincodeStub stub, String[] args, boolean update) {
        boolean error = false;
        String tableName = "";

        int orderID = 0;
        int traderID = 0;
        int price = 0;
        int volume = 0;

        List<TableProto.Column> cols = new ArrayList<TableProto.Column>();

        if (args.length == 4) {
            tableName = orderBook;
            try {
                orderID = Integer.parseInt(args[0]);
                traderID = Integer.parseInt(args[1]);
                price = Integer.parseInt(args[2]);
                volume = Integer.parseInt(args[3]);
            }catch (NumberFormatException e){
                log.error("Illegal field id -" + e.getMessage());
                return;
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
        }else if (args.length == 3) {
            tableName = marketOrders;
            try {
                orderID = Integer.parseInt(args[0]);
                traderID = Integer.parseInt(args[1]);
                volume = Integer.parseInt(args[2]);
            }catch (NumberFormatException e){
                log.error("Illegal field id -" + e.getMessage());
                return;
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
        } else {
            log.error("The number of arguments must be 3 or 4");
            error = true;
        }

        if (!error) {
            TableProto.Row row = TableProto.Row.newBuilder()
                    .addAllColumns(cols)
                    .build();
            try {
                boolean success = false;
                if(update) {
                    success = stub.replaceRow(tableName,row);
                } else {
                    log.info(String.format("Adding order %1$d for trader %2$d",
                            orderID, traderID));
                    success = stub.insertRow(tableName, row);
                }

                if (success){
                    log.info("Row successfully inserted");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void init(ChaincodeStub stub, String function, String[] args) {
        int resUserTable = buildTable(stub, userTable, new String[]{"ID", "Money", "Volume"});
        if (resUserTable < 0)
        {
            log.error("Error creating UserTable");
        }

        int resOrderBook = buildTable(stub, orderBook, new String[]{"OrderID", "TraderID", "Price", "Quantity"});
        if (resOrderBook < 0)
        {
            log.error("Error creating OrderBook");
        }

        int resMarketOrders = buildTable(stub, marketOrders, new String[]{"OrderID", "TraderID", "Quantity"});
        if (resMarketOrders < 0)
        {
            log.error("Error creating MarketOrders");
        }

        for (int i = 1; i <= 5; i++)
        {
            deposit(stub, new String[]{String.valueOf(i), "1000", "100"}, false);
        }
    }

    /*
    0 - good
    -1 - error when deleting a table
    -2 - error when creating a table
    NOTE: first column is always a key
     */
    public int buildTable(ChaincodeStub stub, String tableName, String[] columnNames) {
        int result = 0;
        List<TableProto.ColumnDefinition> cols = new ArrayList<TableProto.ColumnDefinition>();
        log.info("creating table " + tableName);


        log.info("column name is " + columnNames[0]);
        cols.add(TableProto.ColumnDefinition.newBuilder()
                .setName(columnNames[0])
                .setKey(true)
                .setType(TableProto.ColumnDefinition.Type.UINT32)
                .build()
        );

        for (int i = 1; i < columnNames.length; i++)
        {
            log.info("column name is " + columnNames[i]);
            cols.add(TableProto.ColumnDefinition.newBuilder()
                    .setName(columnNames[i])
                    .setKey(false)
                    .setType(TableProto.ColumnDefinition.Type.INT32)
                    .build()
            );
        }

        try {
            try {
                stub.deleteTable(tableName);
            } catch (Exception e) {
                e.printStackTrace();
                result = -1;
            }
            stub.createTable(tableName,cols);
        } catch (Exception e) {
            e.printStackTrace();
            result = -2;
        }
        if (result < 0)
        {
            log.error("Error creatig a table " + tableName);
        }
        return result;
    }

    // TODO update IDs after deletion?
    private boolean delete(ChaincodeStub stub, String[] args){
        int fieldID = 0;
        String option = args[1];

        try {
            fieldID = Integer.parseInt(args[0]);
        }catch (NumberFormatException e){
            log.error("Illegal field id -" + e.getMessage());
            return false;
        }


        TableProto.Column queryCol =
                TableProto.Column.newBuilder()
                        .setUint32(fieldID).build();
        List<TableProto.Column> key = new ArrayList<>();
        key.add(queryCol);

        String tableName = "";
        switch (option)
        {
            case "trader":
                tableName = userTable;
                break;
            case "order":
                tableName = orderBook;
                break;
        }

        if (tableName == "")
        {
            log.error(String.format("No matching name for option %1$s", option));
            return false;
        }

        boolean result = stub.deleteRow(tableName, key);

        if (result)
        {
            log.info(String.format("The row with id = %1$d successfully deleted from table %2$s", fieldID, tableName));
        }
        else
        {
            log.error(String.format("An arror occured when deleting the row with id = %1$d from table %2$s", fieldID, tableName));
        }

        return result;
    }

    // TODO handle deleted rows
    public ArrayList<int[]> queryTable(ChaincodeStub stub, String tableName) {
        //get the size of the table
        int size = getTableSize(stub, tableName);
        log.debug("Size of the table is " + String.valueOf(size));

        // query rows
        ArrayList<int[]> rows = new ArrayList<int[]>();
        for (int fieldID = 1; fieldID <= size; fieldID++)
        {
            TableProto.Column queryCol =
                    TableProto.Column.newBuilder()
                            .setUint32(fieldID).build();
            List<TableProto.Column> key = new ArrayList<>();
            key.add(queryCol);

            try {
                TableProto.Row tableRow = stub.getRow(tableName, key);

                int numOfCols = tableRow.getColumnsCount();
                int[] columns = new int[numOfCols];
                columns[0] = fieldID;

                for (int i = 1; i < numOfCols; i++)
                {
                    columns[i] = tableRow.getColumns(i).getInt32();
                }

                rows.add(columns);
            } catch (Exception invalidProtocolBufferException) {
                invalidProtocolBufferException.printStackTrace();
            }
        }

        return rows;
    }

    public ArrayList<int[]> sortTable(ChaincodeStub stub, String[] args) {
        String tableName = args[0];
        final int orderBy = Integer.parseInt(args[1]);

        // query rows
        ArrayList<int[]> rows = queryTable(stub, tableName);


        // sort the rows
        Collections.sort(rows, new Comparator<int[]>() {
            public int compare(int[] a, int[] b) {
                return a[orderBy] - b[orderBy];
            }
        });

        return rows;
        /*
        for (int[] ints : rows) {
            log.info(Arrays.toString(ints));
        }
        */
    }

    // TODO check behavior when two prices are same
    public void buildOrderBook(ChaincodeStub stub) {
        ArrayList<int[]> rows = queryTable(stub, orderBook);

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

        int j = 1;
        // insert rows
        for (int i = 0; i < rows_buy.size(); i++) {
            rows_buy.get(i)[0] = j;
            String[] cols = Arrays.toString(rows_buy.get(i)).split("[\\[\\]]")[1].split(", ");
            postOrder(stub, cols, true);

            j++;
        }

        for (int i = 0; i < rows_sell.size(); i++) {
            rows_sell.get(i)[0] = j;
            String[] cols = Arrays.toString(rows_sell.get(i)).split("[\\[\\]]")[1].split(", ");
            postOrder(stub, cols, true);

            j++;
        }
    }

    private int getTableSize(ChaincodeStub stub, String tableName) {
        log.info(String.format("Attempting to get the size of table %1$s", tableName));

        String queryRes = "";
        int id = 1;
        do {
            TableProto.Column queryCol =
                    TableProto.Column.newBuilder()
                            .setUint32(id).build();
            List<TableProto.Column> key = new ArrayList<>();
            key.add(queryCol);

            try {
                TableProto.Row tableRow = stub.getRow(tableName,key);
                if (tableRow.getSerializedSize() > 0) {
                    queryRes = tableRow.getColumns(1).getString();
                }else
                {
                    queryRes = "No record found !";
                }
            } catch (Exception invalidProtocolBufferException) {
                invalidProtocolBufferException.printStackTrace();
            }
            id++;
        }while (queryRes != "No record found !");
        return (id - 2);
    }

    public void matchMarketOrders(ChaincodeStub stub) {
        ArrayList<int[]> orders = queryTable(stub, marketOrders);

        for (int[] order : orders) {
            if (order[order.length - 1] > 0) {
                // bying
                log.info("Bying stuff");
                int[] sellOrder = findBestPrice(stub, false);
                log.info(String.format("Matching two orders:\n%1$s\n%2$s", Arrays.toString(order), Arrays.toString(sellOrder)));
                transaction(stub, order, sellOrder);
            } else {
                int[] buyOrder = findBestPrice(stub, true);
                log.info(String.format("Matching two orders:\n%1$s\n%2$s", Arrays.toString(order), Arrays.toString(buyOrder)));
                transaction(stub, buyOrder, order);
            }
        }


    }

    // how the price is set for regular orders?
    public void transaction(ChaincodeStub stub, int[] orderBuy, int[] orderSell) {
        int[] buyer = getTrader(stub, orderBuy[1]);
        int[] seller = getTrader(stub, orderSell[1]);

        int buyerMoney = buyer[1];
        int sellerMoney = seller[1];

        int buyerVolume = buyer[2];
        int sellerVolume = seller[2];

        int requestedVolume = 0;
        int volumeForSale = 0;

        //set price based on market / regular order
        int price = 0;
        if (orderBuy.length == 3) {
            // buying order is a market order
            price = orderSell[2];
            requestedVolume = orderBuy[2];
            volumeForSale = orderSell[3];
        } else if (orderSell.length == 3) {
            // selling order is a market order
            price = orderBuy[2];
            requestedVolume = orderBuy[3];
            volumeForSale = orderSell[2];
        } else {
            price = (orderBuy[2] + orderSell[2]) / 2;
            requestedVolume = orderBuy[3];
            volumeForSale = orderSell[3];
        }

        if (requestedVolume < 0 || volumeForSale > 0)
            log.error(String.format("One of the orders are incorrect.\nVolume for sale: %1$d\nRequested volume: %2$d",
                    volumeForSale, requestedVolume));

        // buyer has enought money?
        if (buyerMoney - requestedVolume*price > 0) {
            // seller has enough volume?
            if (Math.abs(volumeForSale) - requestedVolume >= 0) {
                sellerVolume -= requestedVolume;
                sellerMoney += requestedVolume*price;

                buyerVolume += requestedVolume;
                buyerMoney -= requestedVolume*price;

                volumeForSale += requestedVolume;
                requestedVolume = 0;
            } else {
                requestedVolume -= Math.abs(volumeForSale);
                sellerMoney += price*volumeForSale;
                sellerVolume -= volumeForSale;

                buyerMoney -= price*volumeForSale;
                buyerVolume += volumeForSale;

                volumeForSale = 0;
            }

            // updating orders
            String[] buyer_order_arguments = Arrays.toString(orderBuy).split("[\\[\\]]")[1].split(", ");
            String[] seller_order_arguments = Arrays.toString(orderSell).split("[\\[\\]]")[1].split(", ");

            buyer_order_arguments[0] = String.valueOf(orderBuy[0]);
            buyer_order_arguments[1] = String.valueOf(buyer[0]);
            buyer_order_arguments[orderBuy.length - 1] = String.valueOf(requestedVolume);

            seller_order_arguments[0] = String.valueOf(orderSell[0]);
            seller_order_arguments[1] = String.valueOf(seller[0]);
            seller_order_arguments[orderSell.length - 1] = String.valueOf(volumeForSale);

            log.info(String.format("Updating orders:\n%1$s\n%2$s", Arrays.toString(buyer_order_arguments), Arrays.toString(seller_order_arguments)));

            postOrder(stub, buyer_order_arguments, true);
            postOrder(stub, seller_order_arguments, true);

            // updating traders
            String[] buyer_arguments = Arrays.toString(buyer).split("[\\[\\]]")[1].split(", ");
            String[] seller_arguments = Arrays.toString(seller).split("[\\[\\]]")[1].split(", ");

            buyer_arguments[1] = String.valueOf(buyerMoney);
            buyer_arguments[2] = String.valueOf(buyerVolume);

            seller_arguments[1] = String.valueOf(sellerMoney);
            seller_arguments[2] = String.valueOf(sellerVolume);

            deposit(stub, buyer_arguments, true);
            deposit(stub, seller_arguments, true);
        } else {
            int requiredAmount = price*requestedVolume;
            log.error(String.format("The required amount for this transaction is %1$d", requiredAmount));
        }
    }

    public int[] getTrader(ChaincodeStub stub, int id) {
        int[] trader = new int[3];
        TableProto.Column queryCol =
                TableProto.Column.newBuilder()
                        .setUint32(id).build();
        List<TableProto.Column> key = new ArrayList<>();
        key.add(queryCol);

        try {
            TableProto.Row tableRow = stub.getRow(userTable,key);
            if (tableRow.getSerializedSize() > 0) {
                trader[0] = tableRow.getColumns(0).getUint32();
                trader[1] = tableRow.getColumns(1).getInt32();
                trader[2] = tableRow.getColumns(2).getInt32();
            }else
            {
                return null;
            }
        } catch (Exception invalidProtocolBufferException) {
            invalidProtocolBufferException.printStackTrace();
            return null;
        }
        return trader;
    }

    /*
    is_best_buy : 1 - buy
                  0 - sell
     */
    public int[] findBestPrice(ChaincodeStub stub, boolean is_best_buy) {
        log.debug("entering findBestPrice");

        ArrayList<int[]> rows = queryTable(stub, orderBook);
        int[] bestPrice = new int[4];

        if (is_best_buy)
        {
            // if 1st data entry is negative, no one is bying
            if (rows.get(0)[3] < 0)
            {
                log.debug("value of first entry is " + String.valueOf(rows.get(0)[3]));
                log.error("no traders are bying");
                return null;
            }

            // if last entry is positive, no one is selling, so take the best price from the last row
            if (rows.get(rows.size() - 1)[3] > 0)
            {
                log.debug("size is" + String.valueOf(rows.size()));
                log.debug("value of last entry is " + String.valueOf(rows.get(rows.size() - 1)[3]));
                bestPrice = rows.get(rows.size() - 1);
            }
            else
            {
                for (int i = 0; i < rows.size(); i++)
                {
                    int[] cur = rows.get(i);
                    int[] next = rows.get(i + 1);

                    if (cur[3] > 0 && next[3] < 0)
                    {
                        bestPrice = cur;
                        break;
                    }
                }
            }

        }
        else
        {
            // if last data entry is positive, no one is selling
            if (rows.get(rows.size() - 1)[3] > 0)
            {
                log.error("no traders are selling");
                return null;
            }

            // if fist entry is negative, no one is bying, so take the best price from the first row
            if (rows.get(0)[3] < 0)
            {
                bestPrice = rows.get(0);
            }
            else
            {
                for (int i = rows.size() - 1; i >= 0; i--)
                {
                    int[] cur = rows.get(i);
                    int[] next = rows.get(i - 1);

                    if (cur[3] < 0 && next[3] > 0)
                    {
                        bestPrice = cur;
                        break;
                    }
                }
            }
        }
        return bestPrice;
    }

    @java.lang.Override
    public String query(ChaincodeStub stub, String function, String[] args) {
        log.info("query");
        int fieldID = 0;

        try {
            fieldID = Integer.parseInt(args[0]);
            log.info("field ID = " + String.valueOf(fieldID));
        }catch (NumberFormatException e){
            log.error("Illegal field id -" + e.getMessage());
            return "ERROR querying ";
        }
        TableProto.Column queryCol =
                TableProto.Column.newBuilder()
                        .setUint32(fieldID).build();
        List<TableProto.Column> key = new ArrayList<>();
        key.add(queryCol);
        switch (function){
            case "get_trader": {
                try {
                    TableProto.Row tableRow = stub.getRow(userTable,key);
                    if (tableRow.getSerializedSize() > 0) {
                        String money = String.valueOf(tableRow.getColumns(1).getInt32());
                        String volume = String.valueOf(tableRow.getColumns(2).getInt32());
                        return String.valueOf(fieldID) + " : " + money + " : " + volume;
                    }else
                    {
                        return "No record found !";
                    }
                } catch (Exception invalidProtocolBufferException) {
                    invalidProtocolBufferException.printStackTrace();
                }
            }
            case "get_order": {
                try {
                    TableProto.Row tableRow = stub.getRow(orderBook,key);
                    if (tableRow.getSerializedSize() > 0) {
                        int traderID = tableRow.getColumns(1).getInt32();
                        int price = tableRow.getColumns(2).getInt32();
                        int volume = tableRow.getColumns(3).getInt32();
                        return String.format("%1$d | %2$d | %3$d | %4$d", fieldID, traderID, price, volume);
                    }else
                    {
                        return "No record found !";
                    }
                } catch (Exception invalidProtocolBufferException) {
                    invalidProtocolBufferException.printStackTrace();
                }
            }
            case "order_book": {
                // query rows
                ArrayList<int[]> rows = queryTable(stub, orderBook);

                String result = "\norderID\t\ttraderID\t\tprice\t\tvolume\t\n";
                for (int[] row : rows) {
                    int size = row.length;
                    result += String.format("   %1$d\t\t   %2$d\t\t\t %3$d\t\t   %4$d\n", row[0], row[1], row[2], row[3]);
                }
                return result;
            }
            case "market_orders": {
                // query rows
                ArrayList<int[]> rows = queryTable(stub, marketOrders);

                String result = "\norderID\t\ttraderID\t\tvolume\t\n";
                for (int[] row : rows) {
                    int size = row.length;
                    result += String.format("   %1$d\t\t   %2$d\t\t\t %3$d\n", row[0], row[1], row[2]);
                }
                return result;
            }
            case "traders": {
                // query rows
                ArrayList<int[]> rows = queryTable(stub, userTable);

                String result = "\ntraderID\tmoney\t\t\tvolume\n";
                for (int[] row : rows) {
                    int size = row.length;
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