/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.brandboat.loader;

/**
 *
 * @author brandboat
 */
public final class SQLUtil {

  private SQLUtil() {}

  public static String checkTableExistSQL(String tableName) {
    return "select table_name from system.catalog where table_name = "
        + "'" + tableName + "'";
  }
}
