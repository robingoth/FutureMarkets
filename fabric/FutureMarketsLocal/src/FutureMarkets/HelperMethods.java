package FutureMarkets;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.List;

public class HelperMethods {
    public static final String userTable = "UserTable.csv";
    public static final String orderBook = "OrderBook.csv";
    public static final String marketOrders = "MarketOrders.csv";

    public int maxPrice;
    public int maxVolume;

    public HelperMethods(int maxPrice, int maxVolume) {
        this.maxPrice = maxPrice;
        this.maxVolume = maxVolume;
    }

    public ArrayList<int[]> queryTable(String tableName) {
        // query rows
        ArrayList<int[]> rows = new ArrayList<int[]>();

        try (BufferedReader br = new BufferedReader(new FileReader(tableName))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                int[] row = new int[values.length];

                for (int i = 0; i < row.length; i++) {
                    try {
                        row[i] = Integer.parseInt(values[i]);
                    } catch (NumberFormatException e){
                        System.out.println(e.getMessage());
                    }
                }
                rows.add(row);
            }
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }

        return rows;
    }

    public int[] getTrader(int id) {
        return queryTable(userTable).get(id - 1);
    }

    public int getTableSize(String tableName) {
        return queryTable(tableName).size();
    }

    /*
    is_best_buy : 1 - find best buying price
                  0 - find best selling price
     */
    public int findBestPrice(boolean is_best_buy) {
        //System.out.println("entering findBestPriceOrder");

        ArrayList<int[]> rows = queryTable(orderBook);
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
                System.out.println("value of first entry is " + String.valueOf(rows.get(0)[3]));
                System.out.println("no traders are buying");
                return 1;
            }

            // if last entry is positive, no one is selling, so take the best price from the last row
            if (rows.get(rows.size() - 1)[3] > 0)
            {
                //System.out.println("size is" + String.valueOf(rows.size()));
                //System.out.println("value of last entry is " + String.valueOf(rows.get(rows.size() - 1)[3]));
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
                System.out.println("no traders are selling");
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

    public int findMidPrice() {
        int result = 0;

        int bestBuyPrice = findBestPrice(true);
        int bestSellPrice = findBestPrice(false);

        result = (bestBuyPrice + bestSellPrice) / 2;

        return result;
    }

    //same as above, but does not consider orders with trader id specified
    public int[] findBestPriceOrder(boolean is_best_buy, int traderID) {
        //System.out.println("entering findBestPriceOrder");

        ArrayList<int[]> rows = queryTable(orderBook);
        int[] bestPrice = new int[4];

        if (rows.size() == 0) {
            return new int[]{0, 0, 0, 0};
        }

        if (is_best_buy)
        {
            // if 1st data entry is negative, no one is buying
            if (rows.get(0)[3] < 0)
            {
                System.out.println("value of first entry is " + String.valueOf(rows.get(0)[3]));
                System.out.println("no traders are buying");
                return new int[]{-1, 0, 0, 0};
            }

            // if last entry is positive, no one is selling, so take the best price from the last row
            if (rows.get(rows.size() - 1)[3] > 0)
            {
                System.out.println("size is" + String.valueOf(rows.size()));
                System.out.println("value of last entry is " + String.valueOf(rows.get(rows.size() - 1)[3]));

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
                System.out.println("no traders are selling");
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

    public boolean doBuyingOrdersExist() {
        ArrayList<int[]> rows = queryTable(orderBook);

        if (rows.size() == 0)
            return false;

        // if 1st data entry is negative, no one is bying
        if (rows.get(0)[3] < 0) {
            System.out.println("value of first entry is " + String.valueOf(rows.get(0)[3]));
            System.out.println("no traders are bying");
            return false;
        } else
            return true;
    }

    public boolean doSellingOrdersExist() {
        ArrayList<int[]> rows = queryTable(orderBook);

        if (rows.size() == 0)
            return false;

        // if last data entry is positive, no one is selling
        if (rows.get(rows.size() - 1)[3] > 0) {
            System.out.println("no traders are selling");
            return false;
        } else
            return true;
    }

    public boolean validateOrder(int[] order) {
        int traderID = order[1];
        int price = order[2];
        int volume = order[3];

        if (price < 1 || price > maxPrice) {
            System.out.println(String.format("Price of this order is not in allowed boundaries [1, %1$d]", maxPrice));
            return false;
        }

        // check if the price is valid
        if (volume > 0) {
            if (doSellingOrdersExist()) {
                int bestSellingPrice = findBestPrice(false);

                //System.out.println("bsp = " + String.valueOf(bestSellingPrice));
                if (price > bestSellingPrice) {
                    System.out.println(String.format("Order's price is higher than current best selling price.\n" +
                            "Best selling price = %1$d", bestSellingPrice));
                    return false;
                }
            } else {
                if (price > this.maxPrice) {
                    System.out.println(String.format("Order's price is higher than current best selling price.\n" +
                            "Best selling price = %1$d", this.maxPrice));
                    return false;
                }
            }
        } else {
            if (doBuyingOrdersExist()) {
                int bestBuyingPrice = findBestPrice(true);

                //System.out.println("bbp = " + String.valueOf(bestBuyingPrice));
                if (price < bestBuyingPrice) {
                    System.out.println(String.format("Order's price is lower than current best buying price.\n" +
                            "Best buying price = %1$d", bestBuyingPrice));
                    return false;
                }
            } else {
                if (price < 1) {
                    System.out.println(String.format("Order's price is lower than current best buying price.\n" +
                            "Best buying price = 1"));
                    return false;
                }
            }
        }

        // |volume + buy_volume + sell_volume| <= V_max
        int tradersVolume = getTrader(traderID)[2];

        ArrayList<int[]> pricesVolumes = getPricesVolumes(traderID);
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
            System.out.println(String.format("Trader's volume speculation is not in the allowed boundary of [%1$d; %2$d]",
                    this.maxVolume * (-1), this.maxVolume));
            return false;
        }

        // net value speculation of trader is non-negative w.r.t. order book + new order
        pricesVolumes.add(new int[] {price, volume});
        int netValueSpeculation = netValueSpeculation(traderID, pricesVolumes);

        //System.out.println("NVS = " + String.valueOf(netValueSpeculation));

        if (netValueSpeculation < 0) {
            System.out.println("Net value speculation of trader is negative");
            return false;
        }

        return true;
    }

    public boolean isCancelPermitted(int orderID) {
        ArrayList<int[]> orders = queryTable(HelperMethods.orderBook);

        int[] orderToDelete = orders.get(orderID - 1);
        int traderID = orderToDelete[1];
        int volume = orderToDelete[3];

        int[] trader = getTrader(traderID);
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
            System.out.println(String.format("Volume speculation of trader is higher than V_max.\n%1$d > %2$d", sum, this.maxPrice));
            return false;
        }

        int netValueSpeculation = netValueSpeculation(traderID, pricesVolumes);

        if (netValueSpeculation < 0) {
            System.out.println(String.format("Trader's net value speculation = %1$d and is negative.", netValueSpeculation));
            return false;
        }

        System.out.println(String.format("\n\nsum = %1$d\nnvs = %2$d\n", sum, netValueSpeculation));

        return true;
    }

    public int netValue(int traderID) {
        int netValue = 0;
        // get trader's cash
        int cash = getTrader(traderID)[1];
        int volume = getTrader(traderID)[2];

        netValue = cash - coToLiq(volume);
        return netValue;
    }

    public int netValueSpeculation(int traderID) {
        int result = 0;

        int traderCash = getTrader(traderID)[1];
        int traderVolume = getTrader(traderID)[2];

        int traderSellVolume = 0;
        int traderBuyVolume = 0;

        ArrayList<int[]> traderOrders = getPricesVolumes(traderID);

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

        result = traderCash - sum + coToLiq(traderVolume + traderBuyVolume + traderSellVolume);

        //System.out.println(String.format("sum = %1$d\ncotoliq = %2$d", sum,
        //        coToLiq(traderVolume + traderBuyVolume - traderSellVolume)));

        return result;
    }

    // traderOrders must be an ArrayList of int[]{price, volume}
    private int netValueSpeculation(int traderID, ArrayList<int[]> traderOrders) {
        int result = 0;

        int[] trader = getTrader(traderID);
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

        result = traderCash - sum + coToLiq(traderVolume + traderBuyVolume + traderSellVolume);

        //System.out.println(String.format("sum = %1$d\ncotoliq = %2$d", sum,
        //        coToLiq(traderVolume + traderBuyVolume - traderSellVolume)));

        return result;
    }

    private int coToLiq(int volume) {
        int result = 0;

        ArrayList<int[]> pricesVolumes = new ArrayList<>();

        if (volume > 0) {
            pricesVolumes = getPricesVolumes(true);

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
            pricesVolumes = getPricesVolumes(false);

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
    private ArrayList<int[]> getPricesVolumes(boolean isSale) {
        ArrayList<int[]> result = new ArrayList<int[]>();

        ArrayList<int[]> orderBookRows = queryTable(orderBook);

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

    public ArrayList<int[]> getPricesVolumes(int traderID) {
        ArrayList<int[]> result = new ArrayList<int[]>();

        ArrayList<int[]> orderBookRows = queryTable(orderBook);

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

    public ArrayList<int[]> getTraderOrders (int traderID) {
        ArrayList<int[]> orders = queryTable(orderBook);
        ArrayList<int[]> result = new ArrayList<>();

        for (int[] order : orders) {
            if (order[1] == traderID)
                result.add(order);
        }

        return result;
    }

    public boolean validateData () {
        ArrayList<int[]> userTableRows = queryTable(userTable);

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
                System.out.println(String.format("Trader %1$d has more volume than %2$d", row[1], this.maxVolume));
                return false;
            }

            int netValue = netValue(id);
            if (netValue < 0) {
                System.out.println(String.format("Instant net value of trader %1$d is negative", id));
                return false;
            }

            int netValueSpeculation = netValueSpeculation(id);
            if (netValueSpeculation < 0) {
                System.out.println(String.format("Trader %1$d can't afford all his limit orders", id));
                return false;
            }

            int sumVolumesTrader = 0;
            ArrayList<int[]> pricesVolumes = getPricesVolumes(id);
            for (int[] tuple : pricesVolumes) {
                sumVolumesTrader += tuple[1];
            }

            if (Math.abs(sumVolumesTrader + volume) > this.maxVolume) {
                System.out.println("Summ of volumes is larger than V_max");
                return false;
            }

            sumVolume += volume;
        }

        if (sumVolume != 0) {
            System.out.println("Summary of volume is not equal to zero");
            return false;
        }

        // best buy price and best sell price condition
        int bestBuyPrice = findBestPrice(true);
        int bestSellPrice = findBestPrice(false);

        //System.out.println("bbp" + String.valueOf(bestBuyPrice));
        //System.out.println("bsp" + String.valueOf(bestSellPrice));

        if (!(1 <= bestBuyPrice && bestBuyPrice < bestSellPrice && bestSellPrice <= this.maxPrice)) {
            System.out.println("Best price condition not satisfied");
            return false;
        }

        return true;
    }

    public int getTotalSellVolume() {
        ArrayList<int[]> orders = queryTable(orderBook);
        int result = 0;

        for (int[] order : orders) {
            if (order[3] < 0)
                result += order[3];
        }

        return result;
    }

    public int getTotalBuyVolume() {
        ArrayList<int[]> orders = queryTable(orderBook);;
        int result = 0;

        for (int[] order : orders) {
            if (order[3] > 0)
                result += order[3];
        }

        return result;
    }
}
