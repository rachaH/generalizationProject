/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sqlwrapper;

import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.nodes.TGroupBy;
import gudusoft.gsqlparser.nodes.TResultColumnList;
import gudusoft.gsqlparser.nodes.TWhenClauseItem;
import gudusoft.gsqlparser.nodes.TWhereClause;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;
import java.util.ArrayList;

/**
 *
 * @author Racha
 */
public class SqlQueryParser {

    /**
     *
     * @param query
     * @return ArrayList of attributes from the query
     */
    public static ArrayList<String> getAttributes(String query) {
        TGSqlParser sqlParser = new TGSqlParser(EDbVendor.dbvhive);
        sqlParser.sqltext = query;
        sqlParser.parse();
        TResultColumnList selectAttributes;
        ArrayList<String> attributes = new ArrayList<String>();
        try {
            selectAttributes = sqlParser.sqlstatements.get(0).getResultColumnList();
            for (int i = 0; i < selectAttributes.size(); i++) {
                attributes.add(selectAttributes.getResultColumn(i).toString());
            }
        } catch (Exception ex) {
        }
        return attributes;
    }

    /**
     *
     * @param query
     * @return ArrayList of tables from the query
     */
    public static ArrayList<String> getTables(String query) {
        TGSqlParser sqlParser = new TGSqlParser(EDbVendor.dbvhive);
        sqlParser.sqltext = query;
        sqlParser.parse();
        ArrayList<String> tables = new ArrayList<String>();
        try {
            TSelectSqlStatement select = (TSelectSqlStatement) sqlParser.sqlstatements.get(0);
            for (int i = 0; i < select.tables.size(); i++) {
                tables.add(select.tables.getTable(i).toString());
            }
        } catch (Exception ex) {
        }
        return tables;
    }

    /**
     *
     * @param query
     * @return where clause from the query
     */
    public static TWhereClause getWhereClause(String query) {
        TGSqlParser sqlParser = new TGSqlParser(EDbVendor.dbvhive);
        sqlParser.sqltext = query;
        sqlParser.parse();
        TSelectSqlStatement pstmt = null;
        try {
            pstmt = (TSelectSqlStatement) sqlParser.getSqlstatements().get(0);
        } catch (Exception ex) {
        }
        return pstmt.getWhereClause();
    }

    public static String addWhereClause(String query, String whereClause) {
        TGSqlParser sqlParser = new TGSqlParser(EDbVendor.dbvhive);
        sqlParser.sqltext = query;
        sqlParser.parse();
        TSelectSqlStatement pstmt = null;
        try {
            pstmt = (TSelectSqlStatement) sqlParser.getSqlstatements().get(0);
            pstmt.addWhereClause(whereClause);
        } catch (Exception ex) {
        }
        return pstmt.toString();
    }

    public static String setWhereClause(String query, String whereClause) {

        TGSqlParser sqlParser = new TGSqlParser(EDbVendor.dbvhive);
        sqlParser.sqltext = query;
        sqlParser.parse();

        TSelectSqlStatement pstmt = null;
        try {
            pstmt = (TSelectSqlStatement) sqlParser.getSqlstatements().get(0);
            TWhereClause w = pstmt.getWhereClause();
            w.setString(whereClause);
            pstmt.setWhereClause(w);
        } catch (Exception ex) {
        }
        return pstmt.toString();
    }

    public static TGroupBy getGroupBy(String query) {
        TGSqlParser sqlParser = new TGSqlParser(EDbVendor.dbvhive);
        sqlParser.sqltext = query;
        sqlParser.parse();
        TSelectSqlStatement pstmt = null;
        try {
            pstmt = (TSelectSqlStatement) sqlParser.getSqlstatements().get(0);
        } catch (Exception ex) {
        }
        return pstmt.getGroupByClause();
    }
}
