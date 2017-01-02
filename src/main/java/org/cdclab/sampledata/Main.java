/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdclab.sampledata;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 * @author brandboat
 */
public class Main {

    private static String TABLENAME;
    private static final int SPLITNUM = 10;
    private static final int THREADNUM = 20;

    public static String createTableSQL(long colNum) {

        String sql = "", colNames = "";

        colNames += "test1 char(1) not null"
            + ", test2 bigint not null";

        for (long i = 3; i <= colNum; i++) {
            colNames += ", test" + i + " varchar";
        }
        sql = "CREATE TABLE " + TABLENAME + "("
            + colNames
            + " CONSTRAINT pk PRIMARY KEY(test1,test2))"
            + " COMPRESSION='GZ'"
            + " SPLIT ON ('b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j')";
//        System.out.println("create table sql: " + sql);

        return sql;
    }

    public static void main(String[] args) throws SQLException, IOException, InterruptedException {
        double startTime = 0, endTime = 0;
//        Properties propLog4j = new Properties();
//        propLog4j.load(new FileInputStream(args[0] + "log4j.properties"));
//        PropertyConfigurator.configure(propLog4j);

//        Properties prop = new Properties();
//        prop.load(new FileInputStream(args[0] + "sampledata.properties"));
//        String connURL = prop.getProperty("PHOENIX_URL");
//        TABLENAME = prop.getProperty("TABLENAME");
//        long maxRowNum = Long.parseLong(prop.getProperty("MAX_ROWNUM")),
//            maxColNum = Long.parseLong(prop.getProperty("MAX_COLNUM")),
//            rowsPerThread = (maxRowNum / THREADNUM);
        String connURL = "jdbc:phoenix:umc-03";
        TABLENAME = args[1];
        long maxRowNum = Long.parseLong(args[2]),
            maxColNum = Long.parseLong(args[3]),
            rowsPerThread = maxRowNum / THREADNUM;

        Connection connection = DriverManager.getConnection(connURL);
        Statement statement = connection.createStatement();

        if (statement.executeQuery("select table_name from system.catalog where table_name = "
            + "'" + TABLENAME + "'").next()) {
            statement.executeUpdate("drop table \"" + TABLENAME + "\"");
            connection.commit();
        }

        String sql = createTableSQL(maxColNum);
        statement.executeUpdate(sql);
        connection.commit();
        statement.close();
        connection.close();

        ExecutorService executor = Executors.newFixedThreadPool(THREADNUM);
        CountDownLatch latch = new CountDownLatch(THREADNUM);
        startTime = System.currentTimeMillis();
        for (int i = 0; i < THREADNUM; i++) {
            long startRow = i * rowsPerThread + 1,
                endRow = (i + 1) * rowsPerThread;
            SampleDataThread putThread = new SampleDataThread(latch, connURL, TABLENAME, SPLITNUM, maxColNum, startTime, startRow, endRow);
            executor.execute(putThread);
        }
        latch.await();
        executor.shutdown();
        endTime = System.currentTimeMillis();

//        logger.info(maxRowNum + " rows: " + (endTime - startTime));
    }
}
