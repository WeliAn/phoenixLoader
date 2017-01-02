/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdclab.sampledata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Logger;

/**
 *
 * @author brandboat
 */
public class SampleDataThread implements Runnable {

    private static final Logger logger = Logger.getLogger(SampleDataThread.class);
    private final String tableName;
    private Connection conn;
    private Statement stmt;
    private final int splitNum;
    private final long maxColNum;
    private final long startRow;
    private final long endRow;
    private final double startTime;
//    private static AtomicLong batchTimes = new AtomicLong(0);
    private CountDownLatch latch;

    SampleDataThread(CountDownLatch latch, String connURL, String tableName, int splitNum, long maxColNum, double startTime, long startRow, long endRow) throws SQLException {
        conn = DriverManager.getConnection(connURL);
        conn.setAutoCommit(false);
        this.stmt = conn.createStatement();
        this.tableName = tableName;
        this.splitNum = splitNum;
        this.maxColNum = maxColNum;
        this.startTime = startTime;
        this.startRow = startRow;
        this.endRow = endRow;
        this.latch = latch;
    }

    public String createUpsertSQL(long rowNum) {
        Random r = new Random();
        char prefix = (char) (r.nextInt(splitNum) + 'a');
        String sql = "upsert into \"" + tableName + "\" values( "
            + "'" + prefix + "'"
            + "," + rowNum;

        for (int i = 3; i <= maxColNum; i++) {
            sql += ", 'hello " + i + "'";
        }

        sql += ")";
        return sql;
    }

    public String createUpsertSQL() {
        Random r = new Random();
        char prefix = (char) (r.nextInt(splitNum) + 'a');
        String sql = "upsert into \"" + tableName + "\" values( "
            + "'" + prefix + "'"
            + "," + "NEXT VALUE FOR big_sequence";

        for (int i = 3; i <= maxColNum; i++) {
            sql += ", 'hello " + i + "'";
        }

        sql += ")";
        return sql;
    }

    @Override
    public void run() {
        String sql = "";
        for (long i = startRow; i <= endRow; i++) {
            try {
                sql = createUpsertSQL(i);
                stmt.addBatch(sql);
                if ((i > 100) && (i % 100 == 0)) {
                    stmt.executeBatch();
                    conn.commit();
                    stmt.clearBatch();
                }
            } catch (SQLException ex) {
                logger.error("ex", ex);
            }
        }
        try {
            stmt.executeBatch();
            conn.commit();
            stmt.clearBatch();
        } catch (SQLException ex) {
            logger.error("ex", ex);
        }
        try {
            stmt.close();
            conn.close();
        } catch (SQLException ex) {
            logger.error("ex", ex);
        }

//        long currentRows = batchTimes.addAndGet(endRow - startRow + 1);
//        double endTime = System.currentTimeMillis();
//        logger.info(currentRows + " rows: " + (endTime - startTime));
        latch.countDown();
    }
}
