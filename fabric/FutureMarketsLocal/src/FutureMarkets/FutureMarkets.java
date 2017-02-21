package FutureMarkets;

import java.io.*;
import java.util.*;

public class FutureMarkets {
    // setting maximum price and volume
    private static HelperMethods helper = new HelperMethods(100, 15000);

    public static void main(String[] args) {
        FutureMarkets fm = new FutureMarkets();

        double[] txnRateArr = new double[100];

        for (int i = 0; i < 100; i++) {
            System.out.println(i);
            double start = System.nanoTime();

            fm.run("init", new String[]{"15000"});
            try (BufferedReader br = new BufferedReader(new FileReader("data.csv"))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] values = line.split(",");
                    String[] input = values;

                    if (Integer.parseInt(values[1]) == 0) {
                        input = new String[]{values[0], values[2]};
                    }

                    fm.run("post_order", input);
                }
            } catch (IOException ex) {
                System.out.println(ex.getMessage());
            }

            double end = System.nanoTime();

            txnRateArr[i] = (500 * Math.pow(10, 9)) / (end - start);

            System.out.println(String.valueOf(txnRateArr[i]));

            try {
                System.out.println("Deleting contents of files");
                PrintWriter pw1 = new PrintWriter(HelperMethods.orderBook);
                PrintWriter pw2 = new PrintWriter(HelperMethods.userTable);
                PrintWriter pw3 = new PrintWriter(HelperMethods.marketOrders);

                pw1.close();
                pw2.close();
                pw3.close();
            } catch (FileNotFoundException ex) {
                System.out.println(ex.getMessage());
            }
        }

        System.out.println(Arrays.toString(txnRateArr));
        double mean = helper.mean(txnRateArr);
        System.out.println(mean);
        System.out.println("done.");
    }

    private void run(String function, String[] args) {
        //System.out.println("In run, function:" + function);

        // convert all arguments from string to int
        int[] args_i = new int[args.length];
        for (int i = 0; i < args.length; i++) {
            try {
                args_i[i] = Integer.parseInt(args[i]);
                //log.info(String.format("%1$s -> %2$d", args[i], args_i[i]));
            } catch (NumberFormatException e){
                //System.out.println(e.getMessage());
                System.exit(-1);
            }
        }

        switch (function) {
            case "dummy":
                settleMargin();

                break;
            case "init":
                init(args_i[0]);
                break;
            case "post_order":
                // args = traderID, price, volume
                // or traderID, volume
                boolean isRoundValid = helper.validateData();

                if (!isRoundValid){
                    //System.out.println("Current round is invalid. Check the data");
                }

                postOrder(args_i, false, false);
                buildOrderBook();

                boolean success = settleMargin();

                if (!success) {
                    //System.out.println("Time for Mark To Market");
                    markToMarket();
                }

                break;
            case "update_order":
                // args = orderID, traderID, price, volume
                // or orderID, traderID, volume
                postOrder(args_i, true, false);
                buildOrderBook();
                break;
            case "cancel_order":
                // args = orderID
                cancelLimitOrder(args_i[0]);
                buildOrderBook();

                success = settleMargin();

                if (!success) {
                    //System.out.println("Time for Mark To Market");
                    markToMarket();
                }
                break;
            case "add_trader":
                deposit(args_i, false);
                break;
            // TODO: values provided must represent value to be deposited(existing + provided), not a redefinition of values
            case "deposit":
                deposit(args_i, true);
                break;
            case "delete":
                // args = id, option : {-1 = trader, 0 = order, 1 = market_order}
                delete(args_i);
                break;
            case "build_order_book":
                buildOrderBook();
                break;
            default:
                //System.out.println("No matching case for function:"+function);

        }
    }

    private void init(int money) {
        for (int i = 0; i < 5; i++)
            deposit(new int[]{money, 0}, false);
    }

    private void deposit(int[] args, boolean update) {
        int fieldID = 0;
        int money_amount = 0;
        int volume = 0;

        if (update) {
            fieldID = args[0];
            money_amount = args[1];
            volume = args[2];
        } else {
            fieldID = helper.getTableSize(HelperMethods.userTable) + 1;
            money_amount = args[0];
            volume = args[1];
        }

        int[] row = new int[]{fieldID, money_amount, volume};
        ArrayList<int[]> table = helper.queryTable(HelperMethods.userTable);

        if (update) {
            table.remove(fieldID - 1);
            table.add(fieldID - 1, row);
        } else {
            table.add(row);
        }

        try(PrintWriter out = new PrintWriter(HelperMethods.userTable)  ){
            for (int[] element : table) {
                String line = Arrays.toString(element).replace("[", "").replace("]", "").replace(" ", "");
                out.println(line);
            }
        } catch (FileNotFoundException ex) {
            //System.out.println(ex.getMessage());
        }


    }

    private String postOrder(int[] args, boolean update, boolean bypass) {
        String tableName = "";

        int orderID = 0;
        int traderID = 0;
        int price = 0;
        int volume = 0;

        boolean isMarket = false;

        ////System.out.println("Number of arguments = " + args.length);
        if ((args.length == 3 && update) || (args.length == 2 && !update)) {
            //System.out.println("Type of order = Market");
            isMarket = true;
        }

        if (isMarket) {
            tableName = HelperMethods.marketOrders;

            if (update) {
                orderID = args[0];
                traderID = args[1];
                volume = args[2];
            } else {
                orderID = helper.getTableSize(tableName) + 1;
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
                orderID = helper.getTableSize(tableName) + 1;
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
            boolean isOrderValid = helper.validateOrder(new_order);

            if (!isOrderValid) {
                //System.out.println(String.format("Order %1$s is invalid", Arrays.toString(new_order)));
                return null;
            }

            do {
                ////System.out.println("\nQuerying orders\n");
                ArrayList<int[]> orders = helper.queryTable(HelperMethods.orderBook);

                int i = 0;
                for (i = 0; i < orders.size(); i++) {
                    ////System.out.println(String.format("\ni = %1$d\n", i));
                    int[] order = orders.get(i);
                    new_order = new int[]{orderID, traderID, price, volume};

                    if (price == order[2] && volume * order[3] < 0) {
                        remainder = transaction(new_order, order);

                        volume = remainder;
                        break;
                    }
                }

                orderID = helper.getTableSize(tableName) + 1;

                if (i == orders.size())
                    break;
            } while (remainder != 0);

            if (volume == 0) {
                //System.out.println("Order was totally fulfilled, therefore not posted");
                return null;
            }
        } else if (isMarket && !update) {
            int[] new_order = new int[]{orderID, traderID, volume};

            boolean isBuy = true;
            if (volume > 0)
                isBuy = false;

            int bestPrice = helper.findBestPriceOrder(isBuy, traderID)[2];

            boolean isOrderValid = true;
            if (!bypass)
                isOrderValid = helper.validateOrder(new int[]{orderID, traderID, bestPrice, volume});

            if (!isOrderValid) {
                //System.out.println(String.format("Order %1$s is invalid", Arrays.toString(new_order)));
                return null;
            }

            int[] matchRes = matchMarketOrder(new_order);

            remainder = matchRes[0];
            int numOfTransactions = matchRes[1];

            if (remainder == 0 && numOfTransactions != 0) {
                //System.out.println("Order was totally fulfilled, therefore not posted");
                return null;
            } else if (remainder == 0 && numOfTransactions == 0)
                remainder = volume;
        }

        int[] row;
        if (!isMarket) {
            if (remainder != 0) {
                volume = remainder;
            }
            row = new int[]{orderID, traderID, price, volume};
        } else {
            if (remainder != 0) {
                volume = remainder;
            }
            row = new int[]{orderID, traderID, volume};
        }

        ArrayList<int[]> table = helper.queryTable(tableName);
        if (update) {
            table.remove(orderID - 1);
            table.add(orderID - 1, row);
        } else {
            table.add(row);
        }

        try(PrintWriter out = new PrintWriter(tableName)){
            for (int[] element : table) {
                String line = Arrays.toString(element).replace("[", "").replace("]", "").replace(" ", "");
                out.println(line);
            }
        } catch (FileNotFoundException ex) {
            //System.out.println(ex.getMessage());
        }

        return "";
    }

    /*
    options:
    -1 - trader
    0 - limit order
    1 - market order
     */
    private boolean delete(int[] args) {
        int fieldID = args[0];
        int option = args[1];

        String tableName = "";
        switch (option) {
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

        if (tableName.equals("")) {
            //System.out.println(String.format("Table with name %1$s does not exist", option));
            return false;
        }

        ArrayList<int[]> rows = helper.queryTable(tableName);

        if (rows.size() >= fieldID) {
            rows.remove(fieldID - 1);
        } else {
            //System.out.println(String.format("Field id %1$d does not exist in table %2$s", fieldID, tableName));
            return false;
        }

        //System.out.println(String.format("The row with id = %1$d successfully deleted from table %2$s", fieldID, tableName));

        if (rows.size() != 1 && fieldID != rows.size()) {
            // insert fieldID + 1
            for (int i = fieldID - 1; i < rows.size(); i++) {
                rows.get(i)[0]--;
            }
        }

        try(PrintWriter out = new PrintWriter(tableName)){
            for (int[] element : rows) {
                String line = Arrays.toString(element).replace("[", "").replace("]", "").replace(" ", "");
                out.println(line);
            }
        } catch (FileNotFoundException ex) {
            //System.out.println(ex.getMessage());
        }

        return true;
    }

    private boolean deleteTrader(int traderID) {
        if (helper.getTraderOrders(traderID).size() != 0){
            //System.out.println(String.format("Trader %1$d has orders in order book. Deletion not permitted.", traderID));
            return false;
        }

        int numOfTraders = helper.getTableSize(HelperMethods.userTable);
        List<Integer> tradersToUpdate = new ArrayList<>();

        //create list of traders to update
        for (int i = 1; i <= numOfTraders - traderID; i++) {
            tradersToUpdate.add(traderID + i);
        }

        ArrayList<int[]> ordersToUpdate = new ArrayList<>();
        ArrayList<int[]> orders = helper.queryTable(HelperMethods.orderBook);
        for (int[] order : orders) {
            if (tradersToUpdate.contains(order[1])) {
                order[1]--;
                ordersToUpdate.add(order);
            }
        }

        for (int [] order : ordersToUpdate) {
            postOrder(order, true, false);
        }

        delete(new int[]{traderID, -1});

        return true;
    }

    private boolean cancelLimitOrder(int orderID) {
        boolean isPermitted = helper.isCancelPermitted(orderID);

        if (!isPermitted)
            return false;

        return delete(new int[]{orderID, 0});
    }

    private int[] matchMarketOrder(int[] order) {
        int remainder = 0;
        int numOfTransactions = 0;
        int[] result = new int[]{remainder, numOfTransactions};

        boolean isBuy;

        if (order[2] > 0)
            isBuy = false;
        else
            isBuy = true;

        do {
            int[] bestPriceOrder = helper.findBestPriceOrder(isBuy, order[1]);

            if (bestPriceOrder[0] == -1) {
                result[0] = remainder;
                result[1] = numOfTransactions;
                return result;
            } else if (bestPriceOrder[0] != 0) {
                remainder = transaction(order, bestPriceOrder);
                order[2] = remainder;
                numOfTransactions++;

                result[0] = remainder;
                result[1] = numOfTransactions;
            } else {
                break;
            }
        } while (remainder != 0);

        return result;
    }

    private int transaction(int[] newOrder, int[] existingOrder) {
        //System.out.println(String.format("Matching two orders:\n%1$s\n%2$s", Arrays.toString(newOrder),
        //        Arrays.toString(existingOrder)));

        int traderID = newOrder[1];
        int matchedTraderID = existingOrder[1];

        int[] trader = helper.getTrader(newOrder[1]);
        int[] matchedTrader = helper.getTrader(existingOrder[1]);

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
            delete(new int[]{existingOrder[0], 0});
        }
        else {
            existingOrder[existingOrder.length - 1] = existingOrderVolume;
            postOrder(existingOrder, true, false);
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
            //System.out.println(String.format("Updating trader who posted order:\n%1$s", Arrays.toString(trader)));

            deposit(trader, true);

            matchedTrader[1] = matchedTraderMoney;
            matchedTrader[2] = matchedTraderVolume;
            //System.out.println(String.format("Updating trader who got matched:\n%1$s", Arrays.toString(matchedTrader)));

            deposit(matchedTrader, true);
        }

        return volume;
    }

    private void buildOrderBook() {
        ArrayList<int[]> rows = helper.queryTable(HelperMethods.orderBook);

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
            postOrder(row_buy, true, false);

            j++;
        }

        for (int[] row_sell : rows_sell) {
            row_sell[0] = j;
            postOrder(row_sell, true, false);

            j++;
        }
    }

    private boolean settleMargin() {
        ArrayList<int[]> traders = helper.queryTable(HelperMethods.userTable);
        ArrayList<int[]> brokeTraders = new ArrayList<>();

        for (int[] trader : traders) {
            int netValue = helper.netValue(trader[0]);

            if (netValue < 0)
                brokeTraders.add(trader);
        }

        if (brokeTraders.size() == 0) {
            return true;
        }

        for (int [] brokeTrader : brokeTraders) {
            ArrayList<int[]> traderOrders = helper.getTraderOrders(brokeTrader[0]);
            for (int j = traderOrders.size() - 1; j >= 0; j--) {
                delete(new int[]{traderOrders.get(j)[0], 0});
            }
        }

        int totalSellVolume = Math.abs(helper.getTotalSellVolume());
        int totalBuyVolume = helper.getTotalBuyVolume();
        int buyVolumeOfBrokeTraders = 0;
        int sellVolumeOfBrokeTraders = 0;
        for (int [] brokeTrader : brokeTraders) {
            if (brokeTrader[2] > 0)
                buyVolumeOfBrokeTraders += brokeTrader[2];
            else
                sellVolumeOfBrokeTraders += brokeTrader[2];
        }

        if (sellVolumeOfBrokeTraders > totalSellVolume || buyVolumeOfBrokeTraders > totalBuyVolume) {
            return false;
        }

        for (int i = brokeTraders.size() - 1; i >= 0; i--) {
            int traderVolume = brokeTraders.get(i)[2];

            if (traderVolume != 0)
                postOrder(new int[]{brokeTraders.get(i)[0], -1 * brokeTraders.get(i)[2]}, false, true);

            deleteTrader(brokeTraders.get(i)[0]);
        }

        return true;
    }

    private void markToMarket () {
        ArrayList<int[]> traders = helper.queryTable(HelperMethods.userTable);
        int midPrice = helper.findMidPrice();

        for (int[] trader: traders) {
            int traderVolume = trader[2];
            int traderMoney = trader[1];

            traderMoney += traderVolume * midPrice;
            deposit(new int[]{trader[0], traderMoney, 0}, true);
        }
    }
}
