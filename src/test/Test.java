/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.EExpressionType;
import static gudusoft.gsqlparser.ESetStatementType.password;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.TSourceToken;
import gudusoft.gsqlparser.TSourceTokenList;
import gudusoft.gsqlparser.nodes.IExpressionVisitor;
import gudusoft.gsqlparser.nodes.TExpression;
import gudusoft.gsqlparser.nodes.TExpressionList;
import gudusoft.gsqlparser.nodes.TGroupBy;
import gudusoft.gsqlparser.nodes.TObjectNameList;
import gudusoft.gsqlparser.nodes.TParseTreeNode;
import gudusoft.gsqlparser.nodes.TParseTreeNodeList;
import gudusoft.gsqlparser.nodes.TResultColumnList;
import gudusoft.gsqlparser.nodes.TWhereClause;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.chrono.ThaiBuddhistEra;
import java.util.ArrayList;
import java.util.HashSet;
import static java.util.Objects.hash;
import java.util.Stack;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.jws.WebParam;
import org.gibello.zql.ZStatement;
import sqlwrapper.MySQLWrapper;
import sqlwrapper.QueryResult;
import sqlwrapper.SqlQueryParser;
import org.gibello.zql.*;

/**
 *
 * @author Racha
 */
public class Test {

    private String generalizedQuery = "";
    private String joinWhereConditions = "";
    private boolean continueToNextOperand = true;
    private MySQLWrapper mySqlWrapperServer;
    private MySQLWrapper mySqlWrapperLocal;
    private TWhereClause whereClause;
    private TGroupBy groupBy;
    private ArrayList<String> attributes;
    private ArrayList<String> tables;
    private String originalQuery;
    private Connection serverCon = null;
    private Connection localCon = null;

    public Test() {
        try {
            mySqlWrapperServer = new MySQLWrapper("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/", "serverdata");
            serverCon = mySqlWrapperServer.getConnection();

            mySqlWrapperLocal = new MySQLWrapper("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/", "localdata");
            localCon = mySqlWrapperLocal.getConnection();
        } catch (SQLException ex) {
            Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public String generalizeQuery(String query, int utility, int privacy) {

        try {
            System.out.println("Original query : " + query);
            this.originalQuery = query;
            this.whereClause = SqlQueryParser.getWhereClause(query);
            this.attributes = SqlQueryParser.getAttributes(query);
            this.tables = SqlQueryParser.getTables(query);
            this.groupBy = SqlQueryParser.getGroupBy(query);
            String whereConditions = whereClause.getCondition().toString();

            long start = System.currentTimeMillis();
            String pre_query = buildPreQuery(query);
            long fin = System.currentTimeMillis();
            System.out.println("Pre-Query Building time : " + (fin - start) + " ms");

            //Remove join conditions from the where clause to generalize the values without join conditions
            if (!joinWhereConditions.equals("")) {
                Pattern p = Pattern.compile("(and |or )+" + joinWhereConditions + "( )?");
                Matcher m = p.matcher(whereConditions);

                if (m.find()) {
                    whereConditions = whereConditions.replace(m.group(), "");
                } else {
                    Pattern p2 = Pattern.compile(joinWhereConditions + "( and | or )+");
                    Matcher m2 = p2.matcher(whereConditions);
                    if (m2.find()) {
                        whereConditions = whereConditions.replace(m2.group(), "");
                    }
                }
            }

            int hash = Math.abs(pre_query.hashCode());
            System.out.println("Pre-Query : " + pre_query);
            if (!checkTableExists("TBL_" + hash)) {
                QueryResult result = mySqlWrapperServer.executeQuery(pre_query);
                while (result.hasNext()) {
                    ResultSet rs = (ResultSet) result.next();
                    if (rs != null) {
                        start = System.currentTimeMillis();
                        cacheResult(rs, hash, true);
                        fin = System.currentTimeMillis();
                        System.out.println("Caching time of the Pre-query result : " + (fin - start) + " ms");
                    } else {
                        break;
                    }

                }
            }
            start = System.currentTimeMillis();
            readingThroughWhereTree(hash, utility, privacy);
            fin = System.currentTimeMillis();
            System.out.println("Generalization time : " + (fin - start) + " ms");

            generalizedQuery = SqlQueryParser.setWhereClause(query, generalizedQuery);
            if (groupBy != null) {
                groupByProcess();
            } else {
                System.out.println("Generalization query : " + generalizedQuery);
                //send the generalized query to the server
                int generalizedQuery_hash = Math.abs(generalizedQuery.hashCode());
                if (!checkTableExists("tbl_" + generalizedQuery_hash)) {
                    QueryResult result = mySqlWrapperServer.executeQuery(generalizedQuery);
                    while (result.hasNext()) {
                        ResultSet rs = (ResultSet) result.next();
                        if (rs != null) {
                            start = System.currentTimeMillis();
                            cacheResult(rs, generalizedQuery_hash, false);
                            fin = System.currentTimeMillis();
                            System.out.println("Caching Time of the generalization result : " + (fin - start) + " ms");
                        } else {
                            break;
                        }
                    }
                }
                //execute the original query on the generalized result
                String selectList = getSelectList(attributes);
                String newQuery = "select " + selectList + " from tbl_" + generalizedQuery_hash + " where " + whereConditions;
                System.out.println("Executing final query :" + newQuery);
                QueryResult resultOriginal = mySqlWrapperLocal.executeQuery(newQuery);
                while (resultOriginal.hasNext()) {

                    ResultSet rs = (ResultSet) resultOriginal.next();
                    //send it to the client
                    if (rs != null) {
                        System.out.println("Done");
                    } else {
                        break;
                    }
                }

            }

        } catch (Exception ex) {
            Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
        }
        return generalizedQuery;
    }

    private void readingThroughWhereTree(int hash, int utility, int privacy) {
        generalizedQuery = "where ";
        String whereConditions = whereClause.getCondition().toString();
        Stack<String> stack = new Stack<String>();
        StringBuffer postfix = new StringBuffer(whereConditions.length());
        String c;
        Pattern p = Pattern.compile("( and | or )+");
        Matcher m = p.matcher(whereConditions.toLowerCase());
        for (int i = 0; i <= m.groupCount(); i++) {
            if (m.find()) {
                if (m.group(i).contains("and")) {
                    whereConditions = whereConditions.replace(m.group(i), "   and   ");
                } else {
                    whereConditions = whereConditions.replace(m.group(i), "   or   ");
                }

            }
        }
        String[] infixList = whereConditions.split("\\s\\s\\s");
        for (int i = 0; i < infixList.length; i++) {
            if (!continueToNextOperand) {
                break;
            } else {
                c = infixList[i];
                if (i == infixList.length - 1) {
                    stack.push(c);
                    while (!stack.isEmpty() && isLowerPrecedence(c, stack.peek(), hash, utility, privacy)) {
                        String pop = stack.pop();
                        if (!c.equals("(") || !c.equals(")")) {
                            postfix.append(pop);
                        } else {
                        }
                    }
                    stack.push(c);
                }
                if (!isOperator(c)) {
                    stack.push(c);
                } else {
                    while (!stack.isEmpty() && isLowerPrecedence(c, stack.peek(), hash, utility, privacy)) {
                        String pop = stack.pop();
                        if (!c.equals("(") || !c.equals(")")) {
                            postfix.append(pop);
                        } else {
                        }
                    }
                    stack.push(c);
                }
            }
        }
        while (!stack.isEmpty()) {
            postfix.append(stack.pop());
        }

    }

    private String generalizeValueAndCheckPrivacy(String condition, int hash, int utility, int privacy) {

        String generalizedValues = "";
        try {
            String[] t = condition.split("<=|>=|<|=|>");
            String attribute = t[0].toString();
            String valueRequested = t[1];//'value'

            boolean satisfait = false;
            int offset = 0;
            int nbOfTuples = 0;
            if (!isInteger(valueRequested)) {
                String commonString = null;

                while (!satisfait) {
                    ArrayList<String> values = new ArrayList<>();
                    values.add(valueRequested.substring(1, valueRequested.length() - 1));
//                    QueryResult resultCount = mySqlWrapperLocal.executeQuery("select count(*) from tbl_" + hash);
//                    int count = 0;
//                    while (resultCount.hasNext()) {
//                        ResultSet rs = (ResultSet) resultCount.next();
//                        while (rs.next()) {
//                            count = rs.getInt(1);
//                        }
//                    }
//
//                    if (offset >= count) {
//                        generalizedValues = "";
//                        break;
//                    }
                    Statement st = localCon.createStatement();
                    ResultSet res = st.executeQuery("select " + attribute + " from tbl_" + hash + " ORDER BY rowCount limit " + privacy + " Offset " + offset);
                    while (res.next()) {
                        if (!res.getString(1).equals(values.get(0))) {
                            values.add(res.getString(1));
                        }
                    }

                    if (values.size() >= privacy) {
                        commonString = this.identifyCommonSubStrOfNStr(values);
                        if (commonString != "") {
                            Statement stp = localCon.createStatement();
                            ResultSet rs = stp.executeQuery("select sum(rowCount) from TBL_" + hash + " where " + attribute + " LIKE '%" + commonString + "%'");
                            while (rs.next()) {
                                nbOfTuples = rs.getInt(1);
                            }

//                            QueryResult resultUtility = mySqlWrapperLocal.executeQuery("select sum(rowCount) from TBL_" + hash + " where " + attribute + " LIKE '%" + commonString + "%'");
//                            while (resultUtility.hasNext()) {
//                                ResultSet rs = (ResultSet) resultUtility.next();
//                                while (rs.next()) {
//                                    nbOfTuples = rs.getInt(1);
//                                }
//                            }
                            if (nbOfTuples <= utility) {
                                satisfait = true;
                                generalizedValues += attribute + " LIKE '%" + commonString + "%'";
                                break;
                            }
                        }
                    }
                    offset += privacy;
                }
                if (generalizedValues.equals("")) {
                    return "";
                }
            } else if (condition.contains("<=") || condition.contains("<") || condition.contains("=")) {
                int alpha = 5;
                while (!satisfait) {
                    nbOfTuples = 0;
                    int value = Integer.parseInt(valueRequested) + alpha;

                    QueryResult result = mySqlWrapperLocal.executeQuery("select TBL_" + hash + ".rowcount from TBL_" + hash + " where " + attribute + " < " + value);
                    int diverseRows = 0;
                    while (result.hasNext()) {
                        ResultSet rs = (ResultSet) result.next();
                        if (rs != null) {
                            while (rs.next()) {
                                nbOfTuples += rs.getInt(1);
                            }
                            rs.last();
                            diverseRows = rs.getRow();
                            break;
                        }
                    }

                    if (nbOfTuples <= utility && diverseRows >= privacy) {
                        satisfait = true;
                        generalizedValues += attribute + " < " + value;
                        break;
                    } else {
                        alpha += alpha;
                    }
                }
            } else if (condition.contains(">=") || condition.contains(">")) {
                int alpha = 5;
                while (!satisfait) {
                    nbOfTuples = 0;
                    int value = Integer.parseInt(valueRequested) - alpha;
                    while (value > 0) {
                        QueryResult result = mySqlWrapperLocal.executeQuery("select rowcount from TBL_" + hash + " where " + attribute + " > " + value);
                        int diverseRows = 0;
                        while (result.hasNext()) {
                            ResultSet rs = (ResultSet) result.next();
                            if (rs != null) {
                                while (rs.next()) {
                                    nbOfTuples += rs.getInt(1);
                                }
                                rs.last();
                                diverseRows = rs.getRow();
                                break;
                            }
                        }
//                        QueryResult resultUtility = mySqlWrapperLocal.executeQuery("select sum(rowcount) from TBL_" + hash + " where " + attribute + " > " + value);
//                        while (result.hasNext()) {
//                            ResultSet rs = (ResultSet) result.next();
//                            while (rs.next()) {
//                                nbOfTuples = rs.getInt(1);
//                            }
//                        }
                        if (nbOfTuples <= utility && diverseRows >= privacy) {
                            satisfait = true;
                            generalizedValues += attribute + " > " + value;
                            break;
                        } else {
                            alpha += alpha;
                        }
                    }
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
        }

        return generalizedValues;
    }

    private void groupByProcess() {
        //send the generalized query to the server and cache the result
        String query = removeGroupBy(generalizedQuery, groupBy.toString());
        query = SqlQueryParser.removeAggregateFunctions(query);
        System.out.println("Generalization Query : " + query);
        int generalizedQuery_hash = Math.abs(query.hashCode());
        if (!checkTableExists("tbl_" + generalizedQuery_hash)) {
            QueryResult result = mySqlWrapperServer.executeQuery(query);
            while (result.hasNext()) {
                ResultSet rs = (ResultSet) result.next();
                if (rs != null) {
                    long start = System.currentTimeMillis();
                    cacheResult(rs, generalizedQuery_hash, false);
                    long fin = System.currentTimeMillis();
                    System.out.println("Caching Time of the generalization result : " + (fin - start) + " ms");
                } else {
                    break;
                }
            }
        }
        String selectlist = getSelectList(SqlQueryParser.getAttributesWithoutAggregate(query));

        //execute the original query without the GROUP BY clause on the result of the generalized query
        String originalQueryWithoutGroupBy = "select " + selectlist + " from tbl_" + generalizedQuery_hash + " where " + whereClause.getCondition().toString();
        int originalQueryWithoutGroupBy_hash = Math.abs(originalQueryWithoutGroupBy.hashCode());
        QueryResult resultWithoutGroupBy = mySqlWrapperLocal.executeQuery(originalQueryWithoutGroupBy);
        while (resultWithoutGroupBy.hasNext()) {
            ResultSet rs = (ResultSet) resultWithoutGroupBy.next();
            cacheResult(rs, originalQueryWithoutGroupBy_hash, false);
        }
        if (checkTableExists("tbl_" + originalQueryWithoutGroupBy_hash)) {
            // execute all of the original query on the previous result and send it back to the user
            String newOriginalQuery = "select " + getSelectList(attributes) + " from tbl_" + originalQueryWithoutGroupBy_hash + " where " + whereClause.getCondition().toString() + " " + groupBy;
            QueryResult resultOriginal = mySqlWrapperLocal.executeQuery(newOriginalQuery);
            while (resultOriginal.hasNext()) {
                ResultSet rs = (ResultSet) resultOriginal.next();
            }
        }
    }

    public String buildPreQuery(String query) {
        String[] conditions = whereClause.getCondition().toString().toLowerCase().split(" or | and ");
        //String limit = query.substring(query.indexOf(" limit"), query.length());
        ArrayList<String> whereAttributes = new ArrayList<String>();
        for (String condition : conditions) {
            String[] c = condition.split("<=|>=|<|=|>");
            whereAttributes.add(c[0]);
        }
        String pre_query = "SELECT ";
        String groupBy = " GROUP By ";

        for (int i = 0; i < whereAttributes.size(); i++) {
            if (i == whereAttributes.size() - 1) {
                pre_query += whereAttributes.get(i) + ",count(*) as rowCount from ";
                groupBy += whereAttributes.get(i);
            } else {
                pre_query += whereAttributes.get(i) + ",";
                groupBy += whereAttributes.get(i) + ",";
            }
        }
        for (int i = 0; i < tables.size(); i++) {
            if (i == tables.size() - 1) {
                pre_query += tables.get(i);
            } else {
                pre_query += tables.get(i) + ",";
            }
        }
        if (tables.size() > 1) {
            TExpressionList el = new TExpressionList();
            getJoinWhereClauses(whereClause.getCondition(), el);

            for (int i = 0; i < el.size(); i++) {
                if (i == el.size() - 1) {
                    joinWhereConditions += el.getExpression(i).toString();
                } else {
                    joinWhereConditions += el.getExpression(i).toString() + " and ";
                }
            }
            pre_query = SqlQueryParser.addWhereClause(pre_query, joinWhereConditions);
        }
        pre_query += groupBy;
        return pre_query;
    }

    private void cacheResult(ResultSet rs, int hashcode, boolean prequeryCashing) {
        try {
            StringBuilder sbCreateTable = new StringBuilder(1024);

            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();

            sbCreateTable.append("CREATE table TBL_" + hashcode + " ( ");

            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) {
                    sbCreateTable.append(", ");
                }
                String columnName = rsmd.getColumnLabel(i);
                String columnType = rsmd.getColumnTypeName(i);

                sbCreateTable.append(columnName).append(" ").append(columnType);

                int precision = rsmd.getPrecision(i);
                if (precision != 0) {
                    sbCreateTable.append("( ").append(precision).append(" )");
                }
            }
            sbCreateTable.append(" ) ");
            mySqlWrapperLocal.execute(sbCreateTable.toString());

            while (rs.next()) {
                String insertData = "INSERT INTO TBL_" + hashcode + " (";

                for (int i = 1; i <= columnCount; i++) {
                    if (prequeryCashing) {
                        if (i == columnCount) {
                            insertData += "rowCount) VALUES(";
                        } else {
                            insertData += rsmd.getColumnLabel(i) + ",";
                        }
                    } else if (i == columnCount) {
                        insertData += rsmd.getColumnLabel(i) + ") VALUES(";
                    } else {
                        insertData += rsmd.getColumnLabel(i) + ",";
                    }
                }
                for (int i = 1; i <= columnCount; i++) {
                    Object attribute;
                    if (rs.getObject(i) instanceof String) {
                        attribute = "'" + rs.getString(i).replace("'", "''") + "'";
                    } else {
                        attribute = rs.getInt(i);
                    }

                    if (i == columnCount) {
                        insertData += attribute + ")";
                    } else {
                        insertData += attribute + ",";
                    }
                }
                Statement st = localCon.createStatement();
                st.executeUpdate(insertData);

            }

        } catch (SQLException ex) {
            Logger.getLogger(Test.class
                    .getName()).log(Level.SEVERE, null, ex);

        } finally {
            try {
                localCon.commit();
//            try {
//                rs.close();
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Test.class
//                        .getName()).log(Level.SEVERE, null, ex);
//            }
            } catch (SQLException ex) {
                Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

    }

    public void getJoinWhereClauses(TExpression e, TExpressionList el) {

        if (e.getLeftOperand() != null && e.getRightOperand() != null) {
            if (e.getLeftOperand().getRightOperand() == null || e.getRightOperand().getRightOperand() == null) {
                if (e.getRightOperand().getExpressionType().equals(EExpressionType.simple_object_name_t)) {
                    el.addExpression(e);
                }
            }
            getJoinWhereClauses(e.getLeftOperand(), el);
            getJoinWhereClauses(e.getRightOperand(), el);
        }
    }

    public String identifyCommonSubStrOfNStr(ArrayList<String> strArr) {
        String commonStr = "";
        String smallStr = "";
        //identify smallest String      
        for (String s : strArr) {
            if (smallStr.length() < s.length()) {
                smallStr = s;
            }
        }
        String tempCom = "";
        char[] smallStrChars = smallStr.toCharArray();
        for (char c : smallStrChars) {
            tempCom += c;

            for (String s : strArr) {
                if (!s.contains(tempCom)) {
                    tempCom += c;
                    for (String j : strArr) {
                        if (!j.contains(tempCom)) {
                            tempCom = "";
                            break;
                        }
                    }
                    break;
                }
            }
            if (tempCom != "" && tempCom.length() > commonStr.length()) {
                commonStr = tempCom;
            }
        }
        return commonStr;
    }

    private boolean checkTableExists(String tableName) {
        try {
            DatabaseMetaData metaData = localCon.getMetaData();
            String SCHEMA_NAME = "${YOUR_SCHEMA_NAME}";
            String tableType[] = {"TABLE"};
            ArrayList<String> queries = new ArrayList<>();
            ResultSet tablesResultSet = metaData.getTables(null, SCHEMA_NAME, null, tableType);

            while (tablesResultSet.next()) {
                if (tableName.equalsIgnoreCase(tablesResultSet.getString(3))) {
                    return true;
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(Test.class
                    .getName()).log(Level.SEVERE, null, ex);

        }
        return false;
    }

    private boolean isLowerPrecedence(String operator, String operand, int hash, int utility, int privacy) {
        String generalizedvalue = generalizeValueAndCheckPrivacy(operand, hash, utility, privacy);
        switch (operator.toLowerCase()) {
            case "and":
                if (generalizedvalue.equals("")) {
                    generalizedQuery += operand + " " + operator + " ";
                    continueToNextOperand = true;
                } else {
                    generalizedQuery += generalizedvalue;
                    continueToNextOperand = false;
                }
                return (operand == "AND");

            case "or":
                if (generalizedvalue.equals("")) {
                    generalizedQuery += operand + operator;
                    continueToNextOperand = true;
                } else {
                    generalizedQuery += generalizedvalue + " or ";
                    continueToNextOperand = true;
                }
                return (operand == "OR");

            default:
                //last value with no next operator
                generalizedQuery += generalizedvalue;
                return false;
        }
    }

    private static boolean isOperator(String c) {
        return c.equalsIgnoreCase("AND") || c.equalsIgnoreCase("OR") || c.equals("(") || c.equals(")");

    }

    public static boolean isInteger(String str) {
        if (str == null) {
            return false;
        }
        int length = str.length();
        if (length == 0) {
            return false;
        }
        int i = 0;
        if (str.charAt(0) == '-') {
            if (length == 1) {
                return false;
            }
            i = 1;
        }
        for (; i < length; i++) {
            char c = str.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
        }
        return true;
    }

    public String getSelectList(ArrayList<String> attributes) {
        String selectlist = "";
        for (int i = 0; i < attributes.size(); i++) {
            if (i == attributes.size() - 1) {
                selectlist += attributes.get(i);
            } else {
                selectlist += attributes.get(i) + ",";
            }
        }
        return selectlist;
    }

    public String removeGroupBy(String query, String groupBy) {
        Pattern p = Pattern.compile(groupBy);
        Matcher m = p.matcher(query.toLowerCase());
        if (m.find()) {
            query = query.replace(m.group(), "");
        }
        return query;
    }

    public static void main(String[] args) {
        Test x = new Test();
        long s=System.currentTimeMillis();
        x.generalizeQuery("SELECT * FROM aoldata WHERE Query='www.prescriptionfortime.com' or anonId<1000", 1000, 5);
        long f =System.currentTimeMillis();
          System.out.println("Total Time: "+(f-s)+" ms");
    }

}
