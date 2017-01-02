/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdclab.fdcloader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Scanner;

/**
 *
 * @author brandboat
 */
public class GetResult {
  
  public static void main(String[] args) throws SQLException {
    double start = 0, end = 0;
    Scanner in = new Scanner(System.in);
    System.out.println("Please input the jdbc url(Ex.jdbc:phoenix:192.168.0.103:2181) :");
    String connURL = in.nextLine();
    
    Connection connection = DriverManager.getConnection(connURL);
    Statement statement = connection.createStatement();
    ResultSet rs = null;
    ResultSetMetaData rsmd;
    int colNum = 0;
    
    String sql = args[0];
    rs = statement.executeQuery(sql);
    rsmd = rs.getMetaData();
    colNum = rsmd.getColumnCount();
      
    start = System.currentTimeMillis();
    
    ArrayList<String> colName = new ArrayList<String>();
    for(int i = 1; i < colNum+1; i++) {
      String name = rsmd.getColumnName(i);
      colName.add(name);
    }
    int count = 0;
    while(rs.next()) {
      count++;
      for(int i = 0; i < colName.size(); i++) {
        String d = rs.getString(colName.get(i));
        // System.out.println(d);
      }
      // break;
    }
    end = System.currentTimeMillis();
    System.out.println(count);
    System.out.println("Total Time: " + (end - start));
    rs.close();
    statement.close();
    connection.close();
  }
}
