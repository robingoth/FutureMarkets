package FutureMarkets;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hyperledger.java.shim.ChaincodeStub;
import org.hyperledger.protos.Chaincode;
import org.hyperledger.protos.TableProto;

import java.util.*;
import java.util.List;

public class HelperMethods {
    public static final String userTable = "UserTable";
    public static final String orderBook = "OrderBook";
    public static final String marketOrders = "MarketOrders";
    private static Log log = LogFactory.getLog(HelperMethods.class);

    public int maxPrice;
    public int maxVolume;

    public HelperMethods(int maxPrice, int maxVolume) {
        this.maxPrice = maxPrice;
        this.maxVolume = maxVolume;
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

    public int getTableSize(ChaincodeStub stub, String tableName) {
        //log.info(String.format("Attempting to get the size of table %1$s", tableName));

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
    is_best_buy : 1 - find best buying price
                  0 - find best selling price
     */
    public int findBestPrice(ChaincodeStub stub, boolean is_best_buy) {
        log.debug("entering findBestPriceOrder");

        ArrayList<int[]> rows = queryTable(stub, orderBook);
        int bestPrice = 0;

        if (rows.size() == 0) {
            if (is_best_buy)
                return  1;
            else
                return this.maxPrice;
        }

        if (is_best_buy)
        {
            // if 1st data entry is negative, no one is buying
            if (rows.get(0)[3] < 0)
            {
                log.debug("value of first entry is " + String.valueOf(rows.get(0)[3]));
                log.error("no traders are buying");
                return 1;
            }

            // if last entry is positive, no one is selling, so take the best price from the last row
            if (rows.get(rows.size() - 1)[3] > 0)
            {
                log.debug("size is" + String.valueOf(rows.size()));
                log.debug("value of last entry is " + String.valueOf(rows.get(rows.size() - 1)[3]));
                bestPrice = rows.get(rows.size() - 1)[2];
            }
            else
            {
                for (int i = 0; i < rows.size(); i++)
                {
                    int[] cur = rows.get(i);
                    int[] next = rows.get(i + 1);

                    if (cur[3] > 0 && next[3] < 0)
                    {
                        bestPrice = cur[2];
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
                return this.maxPrice;
            }

            // if fist entry is negative, no one is bying, so take the best price from the first row
            if (rows.get(0)[3] < 0)
            {
                bestPrice = rows.get(0)[2];
            }
            else
            {
                for (int i = rows.size() - 1; i >= 0; i--)
                {
                    int[] cur = rows.get(i);
                    int[] next = rows.get(i - 1);

                    if (cur[3] < 0 && next[3] > 0)
                    {
                        bestPrice = cur[2];
                        break;
                    }
                }
            }
        }
        return bestPrice;
    }

    public int findMidPrice (ChaincodeStub stub) {
        int result = 0;

        int bestBuyPrice = findBestPrice(stub, true);
        int bestSellPrice = findBestPrice(stub, false);

        result = (bestBuyPrice + bestSellPrice) / 2;

        return result;
    }

    //same as above, but does not consider orders with trader id specified
    public int[] findBestPriceOrder(ChaincodeStub stub, boolean is_best_buy, int traderID) {
        log.debug("entering findBestPriceOrder");

        ArrayList<int[]> rows = queryTable(stub, orderBook);
        int[] bestPrice = new int[4];

        if (rows.size() == 0) {
            return new int[]{0, 0, 0, 0};
        }

        if (is_best_buy)
        {
            // if 1st data entry is negative, no one is buying
            if (rows.get(0)[3] < 0)
            {
                log.debug("value of first entry is " + String.valueOf(rows.get(0)[3]));
                log.error("no traders are buying");
                return new int[]{-1, 0, 0, 0};
            }

            // if last entry is positive, no one is selling, so take the best price from the last row
            if (rows.get(rows.size() - 1)[3] > 0)
            {
                log.debug("size is" + String.valueOf(rows.size()));
                log.debug("value of last entry is " + String.valueOf(rows.get(rows.size() - 1)[3]));

                for (int i = rows.size() - 1; i >= 0; i--) {
                    int[] order = rows.get(i);
                    if (order[1] != traderID) {
                        bestPrice = rows.get(i);
                        break;
                    }
                }
            } else {
                for (int i = 0; i < rows.size(); i++) {
                    int[] cur = rows.get(i);
                    int[] next = rows.get(i + 1);

                    if (cur[3] > 0 && next[3] < 0) {
                        bestPrice = cur;
                        break;
                    }
                }
            }

        } else {
            // if last data entry is positive, no one is selling
            if (rows.get(rows.size() - 1)[3] > 0) {
                log.error("no traders are selling");
                return new int[]{-1, 0, 0, 0};
            }

            // if fist entry is negative, no one is buying, so take the best price from the first row
            if (rows.get(0)[3] < 0) {
                for (int i = 0; i < rows.size(); i++) {
                    int[] order = rows.get(i);
                    if (order[1] != traderID) {
                        bestPrice = rows.get(i);
                        break;
                    }
                }
            } else {
                for (int i = rows.size() - 1; i > 0; i--) {
                    int[] cur = rows.get(i);
                    int[] next = rows.get(i - 1);

                    if (cur[3] < 0 && next[3] > 0) {
                        bestPrice = cur;
                        break;
                    }
                }
            }
        }
        return bestPrice;
    }

    public boolean doBuyingOrdersExist(ChaincodeStub stub) {
        ArrayList<int[]> rows = queryTable(stub, orderBook);

        if (rows.size() == 0)
            return false;

        // if 1st data entry is negative, no one is bying
        if (rows.get(0)[3] < 0) {
            log.debug("value of first entry is " + String.valueOf(rows.get(0)[3]));
            log.error("no traders are bying");
            return false;
        } else
            return true;
    }

    public boolean doSellingOrdersExist(ChaincodeStub stub) {
        ArrayList<int[]> rows = queryTable(stub, orderBook);

        if (rows.size() == 0)
            return false;

        // if last data entry is positive, no one is selling
        if (rows.get(rows.size() - 1)[3] > 0) {
            log.error("no traders are selling");
            return false;
        } else
            return true;
    }

    public boolean validateOrder(ChaincodeStub stub, int[] order) {
        int traderID = order[1];
        int price = order[2];
        int volume = order[3];

        if (price < 1 || price > maxPrice) {
            log.error(String.format("Price of this order is not in allowed boundaries [1, %1$d]", maxPrice));
            return false;
        }

        // check if the price is valid
        if (volume > 0) {
            if (doSellingOrdersExist(stub)) {
                int bestSellingPrice = findBestPrice(stub, false);

                //log.info("bsp = " + String.valueOf(bestSellingPrice));
                if (price > bestSellingPrice) {
                    log.error(String.format("Order's price is higher than current best selling price.\n" +
                            "Best selling price = %1$d", bestSellingPrice));
                    return false;
                }
            } else {
                if (price > this.maxPrice) {
                    log.error(String.format("Order's price is higher than current best selling price.\n" +
                            "Best selling price = %1$d", this.maxPrice));
                    return false;
                }
            }
        } else {
            if (doBuyingOrdersExist(stub)) {
                int bestBuyingPrice = findBestPrice(stub, true);

                //log.info("bbp = " + String.valueOf(bestBuyingPrice));
                if (price < bestBuyingPrice) {
                    log.error(String.format("Order's price is lower than current best buying price.\n" +
                            "Best buying price = %1$d", bestBuyingPrice));
                    return false;
                }
            } else {
                if (price < 1) {
                    log.error(String.format("Order's price is lower than current best buying price.\n" +
                            "Best buying price = 1"));
                    return false;
                }
            }
        }

        // |volume + buy_volume + sell_volume| <= V_max
        int tradersVolume = getTrader(stub, traderID)[2];

        ArrayList<int[]> pricesVolumes = getPricesVolumes(stub, traderID);
        int traderBuyVolume = 0;
        int traderSellVolume = 0;
        for (int[] tuple : pricesVolumes) {
            int v = tuple[1];

            if (v > 0)
                traderBuyVolume += volume;
            else
                traderSellVolume += volume;
        }

        int sum = Math.abs(volume + tradersVolume + traderBuyVolume + traderSellVolume);

        if (sum > this.maxVolume) {
            log.error(String.format("Trader's volume speculation is not in the allowed boundary of [%1$d; %2$d]",
                    this.maxVolume * (-1), this.maxVolume));
            return false;
        }

        // net value speculation of trader is non-negative w.r.t. order book + new order
        pricesVolumes.add(new int[] {price, volume});
        int netValueSpeculation = netValueSpeculation(stub, traderID, pricesVolumes);

        //log.info("NVS = " + String.valueOf(netValueSpeculation));

        if (netValueSpeculation < 0) {
            log.error("Net value speculation of trader is negative");
            return false;
        }

        return true;
    }

    public boolean isCancelPermitted(ChaincodeStub stub, int orderID) {
        ArrayList<int[]> orders = queryTable(stub, HelperMethods.orderBook);

        int[] orderToDelete = orders.get(orderID - 1);
        int traderID = orderToDelete[1];
        int volume = orderToDelete[3];

        int[] trader = getTrader(stub, traderID);
        int traderVolume = trader[2];

        ArrayList<int[]> pricesVolumes = new ArrayList<>();

        int sum = 0;
        for (int[] order : orders) {
            if (order[1] == traderID && order[0] != orderID){
                sum += Math.abs(order[3]);
                pricesVolumes.add(new int[]{order[2], order[3]});
            }

        }

        sum += Math.abs(traderVolume);

        if (sum > this.maxVolume) {
            log.error(String.format("Volume speculation of trader is higher than V_max.\n%1$d > %2$d", sum, this.maxPrice));
            return false;
        }

        int netValueSpeculation = netValueSpeculation(stub, traderID, pricesVolumes);

        if (netValueSpeculation < 0) {
            log.error(String.format("Trader's net value speculation = %1$d and is negative.", netValueSpeculation));
            return false;
        }

        log.info(String.format("\n\nsum = %1$d\nnvs = %2$d\n", sum, netValueSpeculation));

        return true;
    }

    public int netValue(ChaincodeStub stub, int traderID) {
        int netValue = 0;
        // get trader's cash
        int cash = getTrader(stub, traderID)[1];
        int volume = getTrader(stub, traderID)[2];

        netValue = cash - coToLiq(stub, volume);
        return netValue;
    }

    public int netValueSpeculation(ChaincodeStub stub, int traderID) {
        int result = 0;

        int traderCash = getTrader(stub, traderID)[1];
        int traderVolume = getTrader(stub, traderID)[2];

        int traderSellVolume = 0;
        int traderBuyVolume = 0;

        ArrayList<int[]> traderOrders = getPricesVolumes(stub, traderID);

        // count sum of prices * volumes of the trader
        int sum = 0;
        for (int[] order : traderOrders) {
            int price = order[0];
            int volume = order[1];

            if (volume > 0)
                traderBuyVolume += volume;
            else
                traderSellVolume += volume;

            sum += price * volume;
        }

        result = traderCash - sum + coToLiq(stub, traderVolume + traderBuyVolume + traderSellVolume);

        //log.info(String.format("sum = %1$d\ncotoliq = %2$d", sum,
        //        coToLiq(stub, traderVolume + traderBuyVolume - traderSellVolume)));

        return result;
    }

    // traderOrders must be an ArrayList of int[]{price, volume}
    private int netValueSpeculation(ChaincodeStub stub, int traderID, ArrayList<int[]> traderOrders) {
        int result = 0;

        int[] trader = getTrader(stub, traderID);
        int traderCash = trader[1];
        int traderVolume = trader[2];

        int traderSellVolume = 0;
        int traderBuyVolume = 0;

        // count sum of prices * volumes of the trader
        int sum = 0;
        for (int[] order : traderOrders) {
            int price = order[0];
            int volume = order[1];

            if (volume > 0)
                traderBuyVolume += volume;
            else
                traderSellVolume += volume;

            sum += price * volume;
        }

        result = traderCash - sum + coToLiq(stub, traderVolume + traderBuyVolume + traderSellVolume);

        //log.info(String.format("sum = %1$d\ncotoliq = %2$d", sum,
        //        coToLiq(stub, traderVolume + traderBuyVolume - traderSellVolume)));

        return result;
    }

    private int coToLiq(ChaincodeStub stub, int volume) {
        int result = 0;

        ArrayList<int[]> pricesVolumes = new ArrayList<>();

        if (volume > 0) {
            pricesVolumes = getPricesVolumes(stub, true);

            int sumVolumes = 0;
            for (int[] priceVolumeTuple : pricesVolumes) {
                sumVolumes += priceVolumeTuple[1];
            }

            if (sumVolumes == 0) {
                result = -volume;
            } else if (Math.abs(sumVolumes) - volume >= 0) {
                for (int[] priceVolumeTuple : pricesVolumes) {
                    if (priceVolumeTuple[1] + volume <= 0) {
                        result -= priceVolumeTuple[0] * volume;
                        break;
                    } else {
                        result -= Math.abs(priceVolumeTuple[1]) * priceVolumeTuple[0];

                        volume += priceVolumeTuple[1];
                    }
                }
            } else {
                for (int[] priceVolumeTuple : pricesVolumes) {
                    result -= Math.abs(priceVolumeTuple[1]) * priceVolumeTuple[0];
                    volume += priceVolumeTuple[1];
                }

                result -= volume;
            }
        } else {
            pricesVolumes = getPricesVolumes(stub, false);

            int sumVolumes = 0;
            for (int[] priceVolumeTuple : pricesVolumes) {
                sumVolumes += priceVolumeTuple[1];
            }

            if (sumVolumes == 0) {
                result = Math.abs(volume) * this.maxPrice;
            } else if (sumVolumes - Math.abs(volume) >= 0) {
                for (int i = pricesVolumes.size() - 1; i >= 0; i--) {
                    int priceOB = pricesVolumes.get(i)[0];
                    int volumeOB = pricesVolumes.get(i)[1];
                    if (volumeOB + volume >= 0) {
                        result += priceOB * Math.abs(volume);
                        break;
                    } else {
                        result += volumeOB * priceOB;

                        volume += volumeOB;
                    }
                }
            } else {
                for (int i = pricesVolumes.size() - 1; i >= 0; i--) {
                    int priceOB = pricesVolumes.get(i)[0];
                    int volumeOB = pricesVolumes.get(i)[1];

                    result += volumeOB * priceOB;
                    volume += volumeOB;
                }

                result += Math.abs(volume) * this.maxPrice;
            }
        }

        return result;
    }

    /*
    isSale = true => get negative prices-volumes
    isSale = false => get positive prices-volumes
     */
    private ArrayList<int[]> getPricesVolumes(ChaincodeStub stub, boolean isSale) {
        ArrayList<int[]> result = new ArrayList<int[]>();

        ArrayList<int[]> orderBookRows = queryTable(stub, orderBook);

        for (int i = 0; i < orderBookRows.size(); i++) {
            int[] valueToAdd = new int[2];

            int price = orderBookRows.get(i)[2];
            int volume = orderBookRows.get(i)[3];

            if (isSale) {
                if (volume < 0) {
                    valueToAdd[0] = price;
                    valueToAdd[1] = volume;

                    result.add(valueToAdd);
                }
            } else {
                if (volume > 0) {
                    valueToAdd[0] = price;
                    valueToAdd[1] = volume;

                    result.add(valueToAdd);
                }
            }
        }

        return result;
    }

    public ArrayList<int[]> getPricesVolumes(ChaincodeStub stub, int traderID) {
        ArrayList<int[]> result = new ArrayList<int[]>();

        ArrayList<int[]> orderBookRows = queryTable(stub, orderBook);

        for (int[] order : orderBookRows) {
            int[] valueToAdd = new int[2];

            int id = order[1];
            int price = order[2];
            int volume = order[3];

            if (id == traderID) {
                valueToAdd[0] = price;
                valueToAdd[1] = volume;

                result.add(valueToAdd);
            }
        }

        return result;
    }

    public ArrayList<int[]> getTraderOrders (ChaincodeStub stub, int traderID) {
        ArrayList<int[]> orders = queryTable(stub, orderBook);
        ArrayList<int[]> result = new ArrayList<>();

        for (int[] order : orders) {
            if (order[1] == traderID)
                result.add(order);
        }

        return result;
    }

    public boolean validateData (ChaincodeStub stub) {
        ArrayList<int[]> userTableRows = queryTable(stub, userTable);

        // TODO total sum of cash over all traders remains constant

        // total sum of volume holding over all traders == 0
        // each trader has no more than V_max volume
        // all traders have non-negative instant net position and can afford all orders they've posted
        // sum of all orders' volumes is in boundary
        int sumVolume = 0;
        for (int[] row : userTableRows) {
            int volume = row[2];
            int id = row[0];

            if (Math.abs(volume) > maxVolume) {
                log.error(String.format("Trader %1$d has more volume than %2$d", row[1], this.maxVolume));
                return false;
            }

            int netValue = netValue(stub, id);
            if (netValue < 0) {
                log.error(String.format("Instant net value of trader %1$d is negative", id));
                return false;
            }

            int netValueSpeculation = netValueSpeculation(stub, id);
            if (netValueSpeculation < 0) {
                log.error(String.format("Trader %1$d can't afford all his limit orders", id));
                return false;
            }

            int sumVolumesTrader = 0;
            ArrayList<int[]> pricesVolumes = getPricesVolumes(stub, id);
            for (int[] tuple : pricesVolumes) {
                sumVolumesTrader += tuple[1];
            }

            if (Math.abs(sumVolumesTrader + volume) > this.maxVolume) {
                log.error("Summ of volumes is larger than V_max");
                return false;
            }

            sumVolume += volume;
        }

        if (sumVolume != 0) {
            log.error("Summary of volume is not equal to zero");
            return false;
        }

        // best buy price and best sell price condition
        int bestBuyPrice = findBestPrice(stub, true);
        int bestSellPrice = findBestPrice(stub, false);

        //log.info("bbp" + String.valueOf(bestBuyPrice));
        //log.info("bsp" + String.valueOf(bestSellPrice));

        if (!(1 <= bestBuyPrice && bestBuyPrice < bestSellPrice && bestSellPrice <= this.maxPrice)) {
            log.error("Best price condition not satisfied");
            return false;
        }

        return true;
    }

    public int getTotalSellVolume(ChaincodeStub stub) {
        ArrayList<int[]> orders = queryTable(stub, orderBook);
        int result = 0;

        for (int[] order : orders) {
            if (order[3] < 0)
                result += order[3];
        }

        return result;
    }

    public int getTotalBuyVolume(ChaincodeStub stub) {
        ArrayList<int[]> orders = queryTable(stub, orderBook);;
        int result = 0;

        for (int[] order : orders) {
            if (order[3] > 0)
                result += order[3];
        }

        return result;
    }
}
