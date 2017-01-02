/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.brandboat.loader.lib;

import com.brandboat.loader.PhoenixDB;
import com.brandboat.loader.SQLUtil;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 *
 * @author brandboat
 */
public class FileServerInfo extends PhoenixDB {

  private final Statement stat;
  private final Connection conn;
  private final static Log LOG = LogFactory.getLog(FileServerInfo.class);
  private static final String TABLE_NAME = "FILE_SERVER_INFO";

  public FileServerInfo(CountDownLatch latch, String connURL)
    throws SQLException {
    super(latch, connURL);
    conn = DriverManager.getConnection(connURL);
    stat = conn.createStatement();
  }

  @Override
  public void createTableIfNotExist() {
    try {
      if (stat.executeQuery(SQLUtil.checkTableExistSQL(TABLE_NAME)).next()) {
        LOG.info("Table " + TABLE_NAME + " exists.");
      } else {
        try {
          stat.executeUpdate(getCreateTableSQL());
          conn.commit();
        } catch (Exception ex) {
          LOG.info("Table " + TABLE_NAME + " exists.");
        }

      }
    } catch (SQLException ex) {
      LOG.error(ex);
    }
  }

  @Override
  protected void insertOneRow() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  protected void insertMultiRow(int n) {

  }

  @Override
  protected void innerRun() {
    try {
      createTableIfNotExist();
      String s = createUpsertSQL1();
      stat.executeUpdate(s);
      s = createUpsertSQL2();
      stat.executeUpdate(s);
      s = createUpsertSQL3();
      stat.executeUpdate(s);
      s = createUpsertSQL4();
      stat.executeUpdate(s);
      s = createUpsertSQL5();
      stat.executeUpdate(s);
      s = createUpsertSQL6();
      stat.executeUpdate(s);
      conn.commit();
    } catch (SQLException ex) {
      LOG.error(ex);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      stat.close();
    } catch (SQLException ex) {
      LOG.error(ex);
    }
    try {
      conn.close();
    } catch (SQLException ex) {
      LOG.error(ex);
    }
  }

  private String getCreateTableSQL() {
    return "CREATE TABLE FDC.FILE_SERVER_INFO("
      + "SERVER_ID varchar,"
      + "SERVER_NAME varchar,"
      + "IS_PRIMARY boolean,"
      + "IS_SERVER_ALIVE boolean,"
      + "LOAD_DATE timestamp,"
      + "CONSTRAINT PK_SERVER_INFO PRIMARY KEY (SERVER_ID, SERVER_NAME)"
      + ")";
  }

  private String createUpsertSQL1() {
    DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss");
    DateTime startTime = dtf.parseDateTime("2015-09-01 01:00:00");
    final UpsertSQLBuilder sql = new UpsertSQLBuilder();
    return sql.setServerId("SET1")
      .setServerName("f12aofdcf01")
      .setIsPrimary(true)
      .setIsServerAlive(true)
      .setLoadDate(startTime)
      .build();
  }

  private String createUpsertSQL2() {
    DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss");
    DateTime startTime = dtf.parseDateTime("2015-09-01 01:00:00");
    final UpsertSQLBuilder sql = new UpsertSQLBuilder();
    return sql.setServerId("SET1")
      .setServerName("f12aofdcf02")
      .setIsPrimary(false)
      .setIsServerAlive(true)
      .setLoadDate(startTime)
      .build();
  }

  private String createUpsertSQL3() {
    DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss");
    DateTime startTime = dtf.parseDateTime("2015-09-01 01:00:00");
    final UpsertSQLBuilder sql = new UpsertSQLBuilder();
    return sql.setServerId("SET2")
      .setServerName("F12AOFDCF01")
      .setIsPrimary(false)
      .setIsServerAlive(true)
      .setLoadDate(startTime)
      .build();
  }

  private String createUpsertSQL4() {
    DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss");
    DateTime startTime = dtf.parseDateTime("2015-09-01 01:00:00");
    final UpsertSQLBuilder sql = new UpsertSQLBuilder();
    return sql.setServerId("SET2")
      .setServerName("F12AOFDCF02")
      .setIsPrimary(true)
      .setIsServerAlive(true)
      .setLoadDate(startTime)
      .build();
  }

  private String createUpsertSQL5() {
    DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss");
    DateTime startTime = dtf.parseDateTime("2015-09-01 01:00:00");
    final UpsertSQLBuilder sql = new UpsertSQLBuilder();
    return sql.setServerId("SET3")
      .setServerName("F12AOFDCF01")
      .setIsPrimary(true)
      .setIsServerAlive(true)
      .setLoadDate(startTime)
      .build();
  }

  private String createUpsertSQL6() {
    DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss");
    DateTime startTime = dtf.parseDateTime("2015-09-01 01:00:00");
    final UpsertSQLBuilder sql = new UpsertSQLBuilder();
    return sql.setServerId("SET3")
      .setServerName("F12AOFDCF02")
      .setIsPrimary(false)
      .setIsServerAlive(true)
      .setLoadDate(startTime)
      .build();
  }

  public class UpsertSQLBuilder {

    private final StringBuilder sb = new StringBuilder();
    private String serverId, serverName, isPrimary, isServerAlive, loadDate;

    public UpsertSQLBuilder() {

    }

    public UpsertSQLBuilder setLoadDate(DateTime dt) {
      this.loadDate = PhoenixDB.toTimeStamp(dt);
      return this;
    }

    public UpsertSQLBuilder setServerId(String s) {
      this.serverId = s;
      return this;
    }

    public UpsertSQLBuilder setServerName(String s) {
      this.serverName = s;
      return this;
    }

    public UpsertSQLBuilder setIsPrimary(boolean b) {
      this.isPrimary = String.valueOf(b);
      return this;
    }

    public UpsertSQLBuilder setIsServerAlive(boolean b) {
      this.isServerAlive = String.valueOf(b);
      return this;
    }

    public String build() {
      return sb.append("upsert into FDC.\"").append(TABLE_NAME).append("\" values(")
        .append("'").append(serverId).append("',")
        .append("'").append(serverName).append("',")
        .append(isPrimary).append(",")
        .append(isServerAlive).append(",")
        .append("'").append(loadDate).append("')")
        .toString();
    }
  }
}
