import java.sql.*;

/**
 * Created by olga on 21/5/17.
 */


public class MySQLClient {


    private final static int NUM_COLUMNS = 30;
    private final static int NUM_ROWS = 20;

    private final static String jdbcDriver = "com.mysql.jdbc.Driver";
    private final static String url = "jdbc:mysql://localhost:3306/bigdata";
    private final static String username = "root";
    private final static String password = "";




    public static void createTable(Statement statement,String tableName) throws SQLException{

        String sql = "CREATE TABLE " + tableName+ "(ID int NOT NULL AUTO_INCREMENT, PRIMARY KEY (ID))";

        statement.executeUpdate(sql);

    }

    public static void addRecord(Statement statement, String tableName) throws SQLException{

                for(int i = 0;i<= NUM_COLUMNS;i++) {
                String addcolum = "ALTER TABLE " + tableName + " ADD " + "col"+i+ " VARCHAR(30)";
                statement.executeUpdate(addcolum);
                for (int j = 0; j <= NUM_ROWS; j++) {
                int value=i+j;
                String insert = "INSERT INTO " +tableName+ "(ID, col"+i+") VALUES ("+j+","+ value+") ON DUPLICATE KEY UPDATE col"+i+"="+value;
                statement.executeUpdate(insert);
            }
        }
    }


    public static void getAllRecord(Statement statement, String tableName)throws  SQLException{

        String sql = "SELECT * from "+ tableName;
        ResultSet rs= statement.executeQuery(sql);
        while (rs.next()) {
            for (int i = 1; i <= NUM_COLUMNS; i++) {
                if (i > 1) System.out.print(",  ");
                int columnValue = rs.getInt(i);
                System.out.print(columnValue + " " + rs.getMetaData().getColumnName(i));
            }
            System.out.println("");
        }


    }

    public static double getAverage (Statement statement, String tableName, String columnname) throws  SQLException{

        String sql = "SELECT AVG("+columnname+") from "+ tableName;
        ResultSet rs= statement.executeQuery(sql);
        if(rs.next())
            return rs.getDouble(1);
        else
            return 0.0;
    }


    public static void delOddRecords(Statement statement, String tableName) throws SQLException{

        String delete = "DELETE FROM "+tableName+" WHERE ID % 2 = 1";
        statement.executeUpdate(delete);
    }

    public static void deleteTable(Statement statement,String tableName) throws SQLException{

        String delete = "DROP TABLE "+tableName;
        statement.executeUpdate(delete);
    }



    public static void main(String[] agrs) {
        //Timers
        long startTime;
        long stopTime;

        String tablename = "project1";
        Connection connection = null;

        try{
            Class.forName(jdbcDriver);
            connection = DriverManager.getConnection(url,username,password);

            if (connection!=null){
                System.out.println("MySQL CONNECTION OK __________");
            }
        }
        catch(SQLException e){
            System.out.println(e);
        }catch(ClassNotFoundException e){
            System.out.println(e);
        }catch(Exception e){
            System.out.println(e);
        }



        try {


            System.out.println("Number of rows: "+ NUM_ROWS);


            Statement statement = connection.createStatement();


            System.out.println("CREATE TABLE____________");
            startTime = System.currentTimeMillis();
            MySQLClient.createTable(statement,tablename);
            stopTime = System.currentTimeMillis();
            System.out.println("CREATE TABLE TIME_____________");
            System.out.println(( stopTime - startTime ) +" ms");
            System.out.println("__________________________________");



            System.out.println("INSERT RECORDS____________________");
            startTime = System.currentTimeMillis();
            MySQLClient.addRecord(statement, tablename);
            stopTime = System.currentTimeMillis();
            System.out.println("INSERT RECORDS TIME_____________________");
            System.out.println(( stopTime - startTime ) +" ms");
            System.out.println("__________________________________");



            System.out.println("SHOW ALL RECORDS__________________");
            startTime = System.currentTimeMillis();
            MySQLClient.getAllRecord(statement,tablename);
            stopTime = System.currentTimeMillis();
            System.out.println("SHOW ALL RECORDS TIME_____________");
            System.out.println(( stopTime - startTime ) +" ms");
            System.out.println("__________________________________/n");

/*
            System.out.println("GET AVERAGE COLUMN 300_________________");
            startTime = System.currentTimeMillis();
            double average=MySQLClient.getAverage(statement,tablename, "col300");
            System.out.println("AVG: " + average + "_________________");
            stopTime = System.currentTimeMillis();
            System.out.println("GET AVERAGE TIME_____________");
            System.out.println(( stopTime - startTime ) +" ms");
            System.out.println("__________________________________");


            System.out.println("GET AVERAGE COLUMN 700_________________");
            startTime = System.currentTimeMillis();
            average = MySQLClient.getAverage(statement,tablename, "col700");
            System.out.println("AVG: " + average + "_________________");
            stopTime = System.currentTimeMillis();
            System.out.println("GET AVERAGE TIME_____________");
            System.out.println(( stopTime - startTime ) +" ms");
            System.out.println("__________________________________");*/


            System.out.println("DELETE ODD RECORDS_________________");
            startTime = System.currentTimeMillis();
            MySQLClient.delOddRecords(statement,tablename);
            stopTime = System.currentTimeMillis();
            System.out.println("DELETE ODD RECORDS TIME_______________");
            System.out.println(( stopTime - startTime ) +" ms");
            System.out.println("__________________________________");


            System.out.println("SHOW ALL RECORDS_________________");
            startTime = System.currentTimeMillis();
            MySQLClient.getAllRecord(statement,tablename);
            stopTime = System.currentTimeMillis();
            System.out.println("SHOW ALL RECORDS TIME_______________");
            System.out.println(( stopTime - startTime ) +" ms");
            System.out.println("__________________________________");



           System.out.println("DELETE TABLE___________");
            startTime = System.currentTimeMillis();
            MySQLClient.deleteTable(statement,tablename);
            stopTime = System.currentTimeMillis();
            System.out.println("DELETE TABLE TIME________________");
            System.out.println(( stopTime - startTime ) +" ms");
            System.out.println("__________________________________");


        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}