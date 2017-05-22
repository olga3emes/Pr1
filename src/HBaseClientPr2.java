import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class HBaseClientPr2 {

	/*
	 * We have to use DAO
	 */


    //The number of columns specified
    private final static int NUM_COLUMNS = 1000;

    //Random number of rows
    private final static int NUM_ROWS = 500;

    private static Configuration conf = null;

    /*
     * Initialization reference [7]
     */
    static {
        conf = HBaseConfiguration.create();
    }

    /*
     * Create a table reference [7]
     */
    public static void createTable(String tableName, String[] familys)
            throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (!admin.tableExists(tableName)) {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (int i = 0; i < familys.length; i++) {
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
            }
            admin.createTable(tableDesc);
        }
    }

    /*
     * Delete a table reference [7]
     */
    public static void deleteTable(String tableName) throws Exception {
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
    }

    /*
     * Put (or insert) a row reference [7]
     */
    public static void addRecord(HTable table, String rowKey,
                                 String family, String qualifier, String value) throws Exception {
        try {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
                    .toBytes(value));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
     * Delete a row reference [7]
     */
    public static void delRecord(HTable table, String rowKey)
            throws IOException {
        List<Delete> list = new ArrayList<Delete>();
        Delete del = new Delete(rowKey.getBytes());
        list.add(del);
        table.delete(list);
    }

    /*
     * Get a row reference [7]
     */
    public static void getOneRecord(HTable table, String rowKey) throws IOException {
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        for (KeyValue kv : rs.raw()) {
            System.out.print(new String(kv.getRow()) + " ");
            System.out.print(new String(kv.getFamily()) + ":");
            System.out.print(new String(kv.getQualifier()) + " ");
            System.out.print(kv.getTimestamp() + " ");
            System.out.println(new String(kv.getValue()));
        }
    }

    /*
     * Scan (or list) a table reference [7]
     */
    public static void getAllRecord(HTable table) {
        try {
            Scan s = new Scan();
            ResultScanner ss = table.getScanner(s);
            for (Result r : ss) {
                for (KeyValue kv : r.raw()) {
                    System.out.print(new String(kv.getRow()) + " ");
                    System.out.print(new String(kv.getFamily()) + ":");
                    System.out.print(new String(kv.getQualifier()) + " ");
                    System.out.print(kv.getTimestamp() + " ");
                    System.out.println(new String(kv.getValue()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
     * Average column
     */
    public static double getAverage(HTable table, String familyName, String columnName) {
        try {
            Scan s = new Scan();
            s.addColumn(Bytes.toBytes(familyName), Bytes
                    .toBytes(columnName));
            ResultScanner ss = table.getScanner(s);
            int i = 0;
            long sum = 0;
            for (Result r : ss) {
                for (KeyValue kv : r.raw()) {
                    int value = Integer.parseInt(new String(kv.getValue()));
                    sum += value;
                    i++;
                }
            }
            return sum / i;
        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        }
    }

    /*
     * Delete a row
     */
    public static void delOddRecords(HTable table)
            throws IOException {

        List<Delete> list = new ArrayList<Delete>();
        for (int i = 0; i < NUM_ROWS; i++) {
            if (i % 2 != 0) {
                String rowKey = "row" + i;
                Delete del = new Delete(rowKey.getBytes());
                list.add(del);
            }
        }
        table.delete(list);
    }

    public static void main(String[] agrs) {
        try {
            //Timers
            long startTime;
            long stopTime;

            String tablename = "project1";
            String[] family = {"family"};
            System.out.println("Number of rows: "+ NUM_ROWS);

            System.out.println("CREATE TABLE____________");
            startTime = System.currentTimeMillis();
            HBaseClientPr2.createTable(tablename, family);
            stopTime = System.currentTimeMillis();
            System.out.println("CREATE TABLE TIME_____________");
            System.out.println(( stopTime - startTime ) +" ms");
            System.out.println("__________________________________");



            System.out.println("INSERT RECORDS____________________");
            startTime = System.currentTimeMillis();
            HTable table = new HTable(conf, tablename);
            for (int i = 0; i < NUM_COLUMNS; i++) {
                for (int j = 0; j < NUM_ROWS; j++) {
                    HBaseClientPr2.addRecord(table, "row" + j, family[0], "col" + i, (i + j) + "");
                }
            }
            stopTime = System.currentTimeMillis();
            System.out.println("INSERT RECORDS TIME_____________________");
            System.out.println(( stopTime - startTime ) +" ms");
            System.out.println("__________________________________");
            


            System.out.println("SHOW ALL RECORDS__________________");
            startTime = System.currentTimeMillis();
            HBaseClientPr2.getAllRecord(table);
            stopTime = System.currentTimeMillis();
            System.out.println("SHOW ALL RECORDS TIME_____________");
            System.out.println(( stopTime - startTime ) +" ms");
            System.out.println("__________________________________/n");


            System.out.println("GET AVERAGE COLUMN 300_________________");
            startTime = System.currentTimeMillis();
            double average = HBaseClientPr2.getAverage(table, family[0], "col3");
            System.out.println("AVG: " + average + "_________________");
            stopTime = System.currentTimeMillis();
            System.out.println("GET AVERAGE TIME_____________");
            System.out.println(( stopTime - startTime ) +" ms");
            System.out.println("__________________________________");


            System.out.println("GET AVERAGE COLUMN 700_________________");
            startTime = System.currentTimeMillis();
            average = HBaseClientPr2.getAverage(table, family[0], "col7");
            System.out.println("AVG: " + average + "_________________");
            stopTime = System.currentTimeMillis();
            System.out.println("GET AVERAGE TIME_____________");
            System.out.println(( stopTime - startTime ) +" ms");
            System.out.println("__________________________________");


            System.out.println("DELETE ODD RECORDS_________________");
            startTime = System.currentTimeMillis();
            HBaseClientPr2.delOddRecords(table);
            stopTime = System.currentTimeMillis();
            System.out.println("DELETE ODD RECORDS TIME_______________");
            System.out.println(( stopTime - startTime ) +" ms");
            System.out.println("__________________________________");


            System.out.println("SHOW ALL RECORDS_________________");
            startTime = System.currentTimeMillis();
            HBaseClientPr2.getAllRecord(table);
            stopTime = System.currentTimeMillis();
            System.out.println("SHOW ALL RECORDS TIME_______________");
            System.out.println(( stopTime - startTime ) +" ms");
            System.out.println("__________________________________");



            System.out.println("DELETE TABLE___________");
            startTime = System.currentTimeMillis();
            HBaseClientPr2.deleteTable(tablename);
            stopTime = System.currentTimeMillis();
            System.out.println("DELETE TABLE TIME________________");
            System.out.println(( stopTime - startTime ) +" ms");
            System.out.println("__________________________________");



        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
