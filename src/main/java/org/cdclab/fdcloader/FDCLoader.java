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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import au.com.bytecode.opencsv.CSVReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.FilenameUtils;

/**
 *
 * @author brandboat
 */
public class FDCLoader {
  private static Path TESTING_DATA_PATH;
  private static String TABLE_NAME;
  private static final int CF_NUM = 7;
  private static final int TOTAL_THREADS = 10;
  private static Connection connection;
  private static Statement statement;
  private static final int SPLIT_NUMBER = 15;
  private static String connURL;
  
  private static ArrayList<Path> getFileList(Path path) {
    ArrayList<Path> filePaths = new ArrayList<Path>();
    File folder = path.toFile();
    File[] listOfFiles = folder.listFiles();

    if (folder != null) {
      for (int i = 0; i < listOfFiles.length; i++) {
        if (listOfFiles[i].isFile()
            && FilenameUtils.getExtension(listOfFiles[i].getName()).equals("csv")) {
          filePaths.add(listOfFiles[i].toPath());
        }
      }
    }

    return filePaths;
  }

  private static String getSchema(Path path) throws FileNotFoundException, IOException {
    String schemaSQL = "", schemaSQLPart = "";
    FileReader fr = new FileReader(path.toFile());
    CSVReader csvReader = new CSVReader(fr);
    String[] attributes = csvReader.readNext();
    
    for (int i = 1; i < attributes.length; i++) {
      if (i == 3) 
        schemaSQLPart += ",\"CF\".\"" + attributes[i] + "\" VARCHAR";
      else 
        schemaSQLPart += ",\"CF\".\"" + attributes[i] + "\" DOUBLE";
    }
    
    schemaSQL += "create table" + " \"" + TABLE_NAME + "\" "
        + "("
        + "pk VARCHAR PRIMARY KEY, "
        + "\"CF\".\"RunNo\" UNSIGNED_LONG, "
        + "\"CF\".\"DateTime\" TIME"
        + schemaSQLPart
        + ")"
        ;
    
//    schemaSQL += "create table " + "\"" + TABLE_NAME + "\""
//        + "("
//        + "pk BINARY(25) PRIMARY KEY, "
//        + "RunNo.RunNo UNSIGNED_LONG, "
//        + "DateTime.DateTime TIME, "
//        + "Time_Diff.Time_Diff DOUBLE, "
//        + "Time_Diff_Point.Time_Diff_Point DOUBLE, "
//        + "RECIPE_STEP.RECIPE_STEP VARCHAR, "
//        + "STEPNUMBER.STEPNUMBER INTEGER, "
//        + "STEP_TIME.STEP_TIME DOUBLE"
//        + schemaSQLPart 
//        + ")";
    
    System.out.println(schemaSQL);
    return schemaSQL;
  }
  
  private static boolean isTableExists(String tableName) 
      throws SQLException {
    if (statement.executeQuery("select table_name from system.catalog where table_name = " 
        + "'" + tableName.toUpperCase() + "'").next())
      return true;
    else
      return false;
  }
  
  private static void dropTable(String tableName)
      throws SQLException {
    statement.executeUpdate("drop table " + tableName.toUpperCase());
    connection.commit();
  }
  
  private static String createSplitKeys(int splitNum) {
    String sql = " split on (";

    sql += "'b'";
    for(int i = 2; i < splitNum; i++) {
      sql += ",'" + (char)('a' + i) + "'";
    }
    sql += ")";
    System.out.println(sql);
    return sql;
  }
  
  private static String enableCompression() {
    return " COMPRESSION='GZ'";
  }
  
  private static void createTable(String schema)
      throws FileNotFoundException, IOException, SQLException {
    String splitPolicy = createSplitKeys(SPLIT_NUMBER);
    String compression = enableCompression();
    statement.executeUpdate(schema + compression + splitPolicy);
    // statement.executeUpdate(schema + compression);
    connection.commit();
  }

  // args : testing_data_path , table_name
  public static void main(String[] args)
      throws SQLException, FileNotFoundException, 
      IOException, ParseException, InterruptedException {
    
    // input the jdbc url
    Scanner in = new Scanner(System.in);
    System.out.println("Please input the jdbc url(Ex.jdbc:phoenix:192.168.0.103:2181) :");
    connURL = in.nextLine();
    
    double startTime = 0, endTime = 0;
    TESTING_DATA_PATH = Paths.get(args[0]);
    TABLE_NAME = args[1];
    
    startTime = System.currentTimeMillis();
    ArrayList<Path> fileList = getFileList(TESTING_DATA_PATH);
    
    connection = DriverManager.getConnection(connURL);
    statement = connection.createStatement();
    
    System.out.println("Initialize table " + TABLE_NAME);
    if(isTableExists(TABLE_NAME)) {
      dropTable(TABLE_NAME);
    }
    createTable(getSchema(fileList.get(0)));
    statement.close();
    connection.close();
    
    BlockingQueue<Path> bq = new ArrayBlockingQueue<Path>(fileList.size());
    for (Path fileName : fileList) {
      bq.add(fileName);
    }

    final CountDownLatch latch = new CountDownLatch(fileList.size());
    ExecutorService executor = Executors.newFixedThreadPool(TOTAL_THREADS);
    Path filePath = null;
    while((filePath = bq.poll()) != null) {
      LoaderThread putThread = new LoaderThread(connURL, filePath, TABLE_NAME, latch, SPLIT_NUMBER);
      executor.execute(putThread);
    }
    
    // wait for latch countdown to zero
    latch.await();
    executor.shutdown();
    endTime = System.currentTimeMillis();
    System.out.println("Total Time: " + (endTime - startTime) + " ms");
    System.out.println("Total files: " + fileList.size());
  }
}
