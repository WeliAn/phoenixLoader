/*
* To change this license header, choose License Headers in Project Properties.
* To change this template file, choose Tools | Templates
* and open the template in the editor.
 */
package com.brandboat.loader.lib;

import com.brandboat.loader.PhoenixDB;
import com.brandboat.loader.RandomDataUtil;
import com.brandboat.loader.SQLUtil;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
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
public class FdcRunData extends PhoenixDB {

    private static final String TABLE_NAME = "FDC_RUN_DATA";
    private static final String INDEX_TABLE_NAME = "FDC_LOCAL_INDEX";
    private final Connection conn;
    private final PreparedStatement stat;
    private DateTime startTime, endTime;
//  private static final Log LOG = LogFactory.getLog(FdcRunData.class);

    public FdcRunData(CountDownLatch latch, String connURL, String sql) throws SQLException {
        super(latch, connURL);
        this.conn = DriverManager.getConnection(connURL);
        this.stat = conn.prepareStatement(sql);
        this.startTime = new DateTime();
        this.endTime = new DateTime();
        DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss");
        startTime = dtf.parseDateTime("2015-09-01 01:00:00");
        endTime = dtf.parseDateTime("2015-11-01 01:00:00");
    }

    @Override
    protected void insertOneRow() {
        try {
            stat.executeUpdate(createUpsertSQL());
        } catch (SQLException ex) {
//      LOG.error(ex);
        }
    }

    @Override
    protected void insertMultiRow(int n) {
        try {
            for (int i = 0; i < n; i++) {
                //String s = createUpsertSQL();
                for (int j = 0; j < 100; j++) {
                    setPrepareStat();
                    stat.addBatch();
                }
                stat.executeBatch();
                //stat.executeUpdate(s);
                stat.clearBatch();
                conn.commit();
            }
        } catch (SQLException ex) {
//      LOG.error(ex);
        }
    }

    @Override
    public synchronized void createTableIfNotExist() {
        try {
            if (stat.executeQuery(SQLUtil.checkTableExistSQL(TABLE_NAME)).next()) {
//        LOG.info("Table " + TABLE_NAME + " exists.");
            } else {
                try {
                    stat.executeUpdate(getCreateTableSQL());
                    conn.commit();
                } catch (Exception ex) {
//          LOG.info("Table " + TABLE_NAME + " exists.");
                }

            }
        } catch (SQLException ex) {
//      LOG.error(ex);
        }
    }

    public synchronized void createLocalindex() {
        try {
            if (stat.executeQuery(SQLUtil.checkTableExistSQL(INDEX_TABLE_NAME)).next()) {
//        LOG.info("Table " + TABLE_NAME + " exists.");
            } else {
                try {
                    stat.executeQuery(getCreateLocalIndex());
                    conn.commit();
                } catch (Exception ex) {
                }
            }
        } catch (SQLException ex) {

        }
    }

    public String getCreateLocalIndex() {
        String sql;
        sql = "create local index FDC_LOCAL_INDEX on fdc.fdc_run_data (fab, module_id, run_no);";
        return sql;
    }

    // insert data
    @Override
    public void innerRun() {
        createTableIfNotExist();
        createLocalindex();
        for (int i = 0; i < 900; i++) {
            insertMultiRow(10000);
            System.out.println("ROW:" + (i + 1) * 100000);
//      LOG.info("Row " + i * 10000 + " has inserted.");
        }
        try {
            conn.commit();
        } catch (SQLException ex) {
//      LOG.error(ex);
        }
    }

    @Override
    public void close() {
        try {
            stat.close();
        } catch (SQLException ex) {
//      LOG.error(ex);
        }
        try {
            conn.close();
        } catch (SQLException ex) {
//      LOG.error(ex);
        }
    }

    private String getCreateTableSQL() {
        return "CREATE TABLE FDC.FDC_RUN_DATA("
                + "ENDTMST timestamp NOT NULL, "
                + "FAB varchar, "
                + "MODULE_ID integer NOT NULL, "
                + "RUN_NO integer NOT NULL, "
                + "SUBRUN_NO varchar, "
                + "PARAM_VER integer, "
                + "STARTTMST timestamp, "
                + "LOT varchar, "
                + "WAFER_NO varchar, "
                + "WAFER_ID varchar, "
                + "CASSETTE varchar, "
                + "PORTID varchar, "
                + "RECIPE varchar, "
                + "RECIPE2 varchar, "
                + "PRODUCT varchar, "
                + "PROCESS varchar, "
                + "ROUTE varchar, "
                + "STEP varchar, "
                + "LAYER varchar, "
                + "LOTTYPE varchar, "
                + "MES_RECIPE1 varchar, "
                + "MES_RECIPE2 varchar, "
                + "SERVER varchar, "
                + "PATH varchar, "
                + "TOOL_NAME varchar, "
                + "MODULE_NAME varchar, "
                + "CONSTRAINT PK_FDC_RUN_DATA PRIMARY KEY (ENDTMST, FAB, MODULE_ID, RUN_NO, SUBRUN_NO)"
                + ") SALT_BUCKETS = 15, COMPRESSION='SNAPPY'";
    }

    private void setPrepareStat() throws SQLException {
        stat.setTimestamp(1, new Timestamp(RandomDataUtil.randomLong()));
        stat.setString(2, RandomDataUtil.randomString(10));
        stat.setInt(3, RandomDataUtil.randomInt());
        stat.setInt(4, RandomDataUtil.randomInt());
        stat.setString(5, RandomDataUtil.randomString(10));
        stat.setInt(6, RandomDataUtil.randomInt());
        stat.setTimestamp(7, new Timestamp(RandomDataUtil.randomLong()));
        stat.setString(8, RandomDataUtil.randomString(10));
        stat.setString(9, RandomDataUtil.randomString(10));
        stat.setString(10, RandomDataUtil.randomString(10));
        stat.setString(11, RandomDataUtil.randomString(10));
        stat.setString(12, RandomDataUtil.randomString(10));
        stat.setString(13, RandomDataUtil.randomString(10));
        stat.setString(14, RandomDataUtil.randomString(10));
        stat.setString(15, RandomDataUtil.randomString(10));
        stat.setString(16, RandomDataUtil.randomString(10));
        stat.setString(17, RandomDataUtil.randomString(10));
        stat.setString(18, RandomDataUtil.randomString(10));
        stat.setString(19, RandomDataUtil.randomString(10));
        stat.setString(20, RandomDataUtil.randomString(10));
        stat.setString(21, RandomDataUtil.randomString(10));
        stat.setString(22, RandomDataUtil.randomString(10));
        stat.setString(23, RandomDataUtil.randomString(10));
        stat.setString(24, RandomDataUtil.randomString(10));
        stat.setString(25, RandomDataUtil.randomString(10));
        stat.setString(26, RandomDataUtil.randomString(10));
    }

    private String createUpsertSQL() {
        final UpsertSQLBuilder sql = new UpsertSQLBuilder();
        return sql.setEndTmst(RandomDataUtil.randomDateTime(startTime, endTime))
                .setFab(RandomDataUtil.randomString(10))
                .setModuleId(RandomDataUtil.randomInt())
                .setRunNo(RandomDataUtil.randomInt())
                .setSubRunNo(RandomDataUtil.randomString(10))
                .setParamVer(RandomDataUtil.randomInt())
                .setStartTmst(RandomDataUtil.randomDateTime(startTime, endTime))
                .setLot(RandomDataUtil.randomString(10))
                .setWaferNo(RandomDataUtil.randomString(10))
                .setWaferId(RandomDataUtil.randomString(10))
                .setCassette(RandomDataUtil.randomString(10))
                .setPortId(RandomDataUtil.randomString(10))
                .setRecipe(RandomDataUtil.randomString(10))
                .setRecipe2(RandomDataUtil.randomString(10))
                .setProduct(RandomDataUtil.randomString(10))
                .setProcess(RandomDataUtil.randomString(10))
                .setRoute(RandomDataUtil.randomString(10))
                .setStep(RandomDataUtil.randomString(10))
                .setLayer(RandomDataUtil.randomString(10))
                .setLottype(RandomDataUtil.randomString(10))
                .setMesRecipe1(RandomDataUtil.randomString(10))
                .setMesRecipe2(RandomDataUtil.randomString(10))
                .setServer(RandomDataUtil.randomString(10))
                .setPath(RandomDataUtil.randomString(10))
                .setToolName(RandomDataUtil.randomString(10))
                .setModuleName(RandomDataUtil.randomString(10))
                .build();
    }

    public class UpsertSQLBuilder {

        private final StringBuilder sb = new StringBuilder();
        private String endTmst, fab, moduleId, runNo, subRunNo, paramVer, startTmst,
                lot, waferNo, waferId, cassette, portId, recipe, recipe2, product,
                process, route, step, layer, lottype, mesRecipe1, mesRecipe2, server,
                path, toolName, moduleName;

        public UpsertSQLBuilder() {

        }

        public UpsertSQLBuilder setEndTmst(DateTime dt) {
            this.endTmst = PhoenixDB.toTimeStamp(dt);
            return this;
        }

        public UpsertSQLBuilder setFab(String s) {
            this.fab = s;
            return this;
        }

        public UpsertSQLBuilder setModuleId(int i) {
            this.moduleId = String.valueOf(i);
            return this;
        }

        public UpsertSQLBuilder setRunNo(int i) {
            this.runNo = String.valueOf(i);
            return this;
        }

        public UpsertSQLBuilder setSubRunNo(String s) {
            this.subRunNo = s;
            return this;
        }

        public UpsertSQLBuilder setParamVer(int i) {
            this.paramVer = String.valueOf(i);
            return this;
        }

        public UpsertSQLBuilder setStartTmst(DateTime dt) {
            this.startTmst = PhoenixDB.toTimeStamp(dt);
            return this;
        }

        public UpsertSQLBuilder setLot(String s) {
            this.lot = s;
            return this;
        }

        public UpsertSQLBuilder setWaferNo(String s) {
            this.waferNo = s;
            return this;
        }

        public UpsertSQLBuilder setWaferId(String s) {
            this.waferId = s;
            return this;
        }

        public UpsertSQLBuilder setCassette(String s) {
            this.cassette = s;
            return this;
        }

        public UpsertSQLBuilder setPortId(String s) {
            this.portId = s;
            return this;
        }

        public UpsertSQLBuilder setRecipe(String s) {
            this.recipe = s;
            return this;
        }

        public UpsertSQLBuilder setRecipe2(String s) {
            this.recipe2 = s;
            return this;
        }

        public UpsertSQLBuilder setProduct(String s) {
            this.product = s;
            return this;
        }

        public UpsertSQLBuilder setProcess(String s) {
            this.process = s;
            return this;
        }

        public UpsertSQLBuilder setRoute(String s) {
            this.route = s;
            return this;
        }

        public UpsertSQLBuilder setStep(String s) {
            this.step = s;
            return this;
        }

        public UpsertSQLBuilder setLayer(String layer) {
            this.layer = layer;
            return this;
        }

        public UpsertSQLBuilder setLottype(String lot) {
            this.lottype = lot;
            return this;
        }

        public UpsertSQLBuilder setMesRecipe1(String mes) {
            this.mesRecipe1 = mes;
            return this;
        }

        public UpsertSQLBuilder setMesRecipe2(String mes) {
            this.mesRecipe2 = mes;
            return this;
        }

        public UpsertSQLBuilder setServer(String server) {
            this.server = server;
            return this;
        }

        public UpsertSQLBuilder setPath(String path) {
            this.path = path;
            return this;
        }

        public UpsertSQLBuilder setToolName(String tool) {
            this.toolName = tool;
            return this;
        }

        public UpsertSQLBuilder setModuleName(String module) {
            this.moduleName = module;
            return this;
        }

        public String build() {
            return sb.append("upsert into FDC.\"").append(TABLE_NAME).append("\" values(")
                    .append("'").append(endTmst).append("',")
                    .append("'").append(fab).append("',")
                    .append(moduleId).append(",")
                    .append(runNo).append(",")
                    .append("'").append(subRunNo).append("',")
                    .append(paramVer).append(",")
                    .append("'").append(startTmst).append("',")
                    .append("'").append(lot).append("',")
                    .append("'").append(waferNo).append("',")
                    .append("'").append(waferId).append("',")
                    .append("'").append(cassette).append("',")
                    .append("'").append(portId).append("',")
                    .append("'").append(recipe).append("',")
                    .append("'").append(recipe2).append("',")
                    .append("'").append(product).append("',")
                    .append("'").append(process).append("',")
                    .append("'").append(route).append("',")
                    .append("'").append(step).append("',")
                    .append("'").append(layer).append("',")
                    .append("'").append(lottype).append("',")
                    .append("'").append(mesRecipe1).append("',")
                    .append("'").append(mesRecipe2).append("',")
                    .append("'").append(server).append("',")
                    .append("'").append(path).append("',")
                    .append("'").append(toolName).append("',")
                    .append("'").append(moduleName).append("')")
                    .toString();
        }
    }
}
