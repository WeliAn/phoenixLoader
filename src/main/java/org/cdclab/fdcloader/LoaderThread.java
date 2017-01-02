/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author brandboat
 */
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdclab.fdcloader;

import au.com.bytecode.opencsv.CSVReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author brandboat
 */
public class LoaderThread implements Runnable {

  private Path FILE_PATH;
  private String TABLE_NAME;
  private Connection conn;
  private Statement stat;
  private CountDownLatch latch;
  private int putTimes;
  private int SPLIT_NUMBER;

  public LoaderThread(String connURL, Path filePath, String tableName,
      CountDownLatch latch, int splitNum) throws SQLException {
    conn = DriverManager.getConnection(connURL);
    this.stat = conn.createStatement();
    this.latch = latch;
    this.putTimes = 0;
    this.FILE_PATH = filePath;
    this.TABLE_NAME = tableName;
    this.SPLIT_NUMBER = splitNum;
  }

  public String getRunNumber(String fileName) {
    Pattern logEntry = Pattern.compile("\\_(.*?)\\.");
    Matcher matchPattern = logEntry.matcher(fileName);
    String rn = "";

    while (matchPattern.find()) {
      rn = matchPattern.group(1);
    }

    return rn;
  }

  public String createUpsertSQL(Path filePath)
      throws SQLException, FileNotFoundException, IOException, ParseException {
    FileReader fileReader = new FileReader(filePath.toFile());
    CSVReader csvReader = new CSVReader(fileReader);
    String[] words = csvReader.readNext();
//    byte[] rowKey = null;
    String rowKey = "";
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
    SimpleDateFormat outputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    String sql = "";

    // create upsert sql
    while ((words = csvReader.readNext()) != null) {
      // create row key : byte[25] = "prefix""step number""date time""run number";
      // prefix      
//      rowKey = new byte[25];
      Random r = new Random();
      char prefix = (char) (r.nextInt(SPLIT_NUMBER) + 'a');
//      rowKey[0] = (byte) prefix;
      // step number
//      byte[] stepNum = Bytes.toBytes(Long.parseLong(words[4]));
//      System.arraycopy(rowKey, 1, stepNum, 0, 8);
      // date time : transform the time format into jdbc time format
//      long dateTime = dateFormat.parse(words[0]).getTime();
//      byte[] time = Bytes.toBytes(dateTime);
//      System.arraycopy(rowKey, 9, time, 0, 8);
      // run number
//      long runNo = Long.parseLong(getRunNumber(FILE_PATH.getFileName().toString()));
//      byte[] runNumber = Bytes.toBytes(runNo);
//      System.arraycopy(rowKey, 17, runNumber, 0, 8);
      rowKey = "";
      rowKey += prefix;
      rowKey += getRunNumber(FILE_PATH.getFileName().toString());
      rowKey += words[4];
      rowKey += words[0];
      long runNo = Long.parseLong(getRunNumber(FILE_PATH.getFileName().toString()));
      long dateTime = dateFormat.parse(words[0]).getTime();
      sql = "upsert into \"" + TABLE_NAME + "\" values( " + "'" + rowKey + "'";
      sql += "," + runNo;
      for (int i = 0; i < words.length; i++) {
        switch (i) {
          case 0:
            Date tmp = new Date(dateTime);
            String s = outputDateFormat.format(tmp);
            sql += ", TO_DATE('" + s + "')";
            break;
          case 1:
            sql += ", " + words[i];
            break;
          case 2:
            sql += ", " + words[i];
            break;
          case 3:
            sql += ",'" + words[i] + "'";
            break;
          case 4:
            sql += ", " + words[i];
            break;
          case 5:
            sql += ", " + words[i];
            break;
          default:
            sql += ", " + words[i];
        }
      }
      sql += ")";
      // System.out.println(sql);
      stat.executeUpdate(sql);
      putTimes++;
    }
    csvReader.close();
    fileReader.close();
    return sql;
  }

  // create upsert sql for every line in a file, also testing the time period.
  @Override
  public void run() {
    double putStart = 0, putEnd = 0, readStart = 0, readEnd = 0, exeStart = 0, exeEnd = 0;
    try {
      //System.out.println("open thread");
      exeStart = System.currentTimeMillis();
      readStart = System.currentTimeMillis();

      readEnd = System.currentTimeMillis();
      putStart = System.currentTimeMillis();
      System.out.println(FILE_PATH);
      createUpsertSQL(FILE_PATH);
      conn.commit();
      putEnd = System.currentTimeMillis();
      exeEnd = System.currentTimeMillis();
    } catch (SQLException ex) {
      Logger.getLogger(LoaderThread.class.getName()).log(Level.SEVERE, null, ex);
    } catch (IOException ex) {
      Logger.getLogger(LoaderThread.class.getName()).log(Level.SEVERE, null, ex);
    } catch (ParseException ex) {
      Logger.getLogger(LoaderThread.class.getName()).log(Level.SEVERE, null, ex);
    } finally {
      latch.countDown();
//      System.out.println("read time: " + (readEnd - readStart) + " ms, "
//          + "put time: " + (putEnd - putStart) + " ms, "
//          + "exeuction time: " + (exeEnd - exeStart) + " ms,"
//          + " total puts: " + putTimes);
      try {
        stat.close();
        conn.close();
      } catch (SQLException ex) {
        Logger.getLogger(LoaderThread.class.getName()).log(Level.SEVERE, null, ex);
      }
    }
  }
}
