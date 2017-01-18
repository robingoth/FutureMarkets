package FutureMarkets;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hyperledger.java.shim.ChaincodeBase;
import org.hyperledger.java.shim.ChaincodeStub;
import org.hyperledger.protos.Chaincode;
import org.hyperledger.protos.TableProto;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class HelperMethods {
    public static final String userTable = "UserTable";
    public static final String orderBook = "OrderBook";
    public static final String marketOrders = "MarketOrders";
    private static Log log = LogFactory.getLog(HelperMethods.class);

    private int maxPrice;
    private int maxVolume;

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

    public int netValue(ChaincodeStub stub, int traderID) {
        int netValue = 0;
        // get trader's cash
        int cash = getTrader(stub, traderID)[1];
        int volume = getTrader(stub, traderID)[2];

        netValue = cash - cotoliq(stub, volume);
        return netValue;
    }

    private int cotoliq(ChaincodeStub stub, int volume) {
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

}
