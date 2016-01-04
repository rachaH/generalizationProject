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

    private final String driver = "com.mysql.jdbc.Driver";
    String user = "root";
    String pass = "";
    String url = "jdbc:mysql://localhost:3306/";
    String dbName = "test";
    private String generalizedQuery = "";
    private Connection conn = null;
    private String joinWhereConditions = "";
    private boolean continueToNextOperand = true;

    public Connection loadDriver() throws SQLException, InstantiationException, IllegalAccessException {

        Connection cn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            cn = DriverManager.getConnection(url + dbName, user, pass);

        } catch (ClassNotFoundException ex) {
            System.err.println("\nUnable to load the JDBC driver ");
            System.err.println("Please check your CLASSPATH.");
            ex.printStackTrace(System.err);
        }
        return cn;

    }

    public String generalizeQuery(String query, int utility, int privacy) {

        try {
            TWhereClause where = SqlQueryParser.getWhereClause(query);
            TGroupBy groupBy = SqlQueryParser.getGroupBy(query);

            String whereConditions = where.getCondition().toString();
            String pre_query = buildPreQuery(query);

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
                System.out.println(whereConditions);
            }
            int hash = Math.abs(pre_query.hashCode());

            Statement st = loadDriver().createStatement();

            if (!checkTableExists("TBL_" + hash)) {
                cacheResult(st.executeQuery(pre_query), hash);
            }

            Stack<String> stack = new Stack<String>();
            StringBuffer postfix = new StringBuffer(whereConditions.length());
            String c;
            String[] infixList = whereConditions.split(" ");
            for (int i = 0; i < infixList.length; i++) {
                if (!continueToNextOperand) {
                    // generalizedQuery += infixList[i] + " ";
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
            generalizedQuery = SqlQueryParser.setWhereClause(query, generalizedQuery);
            System.out.println(generalizedQuery);

            if (groupBy!=null) {
               int groupByHash= Math.abs(generalizedQuery.hashCode());
               
               // send the generalized query to the server and cache the result
                cacheResult(st.executeQuery(generalizedQuery),groupByHash);
                ResultSet rs = st.executeQuery("select * from tbl_"+groupByHash+" "+ where.getCondition().toString());
               // st.executeQuery();
            }

        } catch (SQLException ex) {
            Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
        }
        return "";
    }

    private String generalizeValueAndCheckPrivacy(String condition, String nextOperator, int hash, int utility, int privacy) {

        String generalizedValues = "";
        try {
            Statement st = loadDriver().createStatement();

            String[] t = condition.split("[<=>]");
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
                    ResultSet rsCount = st.executeQuery("select count(*) from tbl_" + hash);
                    int count = 0;
                    while (rsCount.next()) {
                        count = rsCount.getInt(1);
                    }
                    if (offset >= count) {
                        generalizedValues = "";
                        break;
                    }
                    ResultSet rs = st.executeQuery("select " + attribute + " from tbl_" + hash + " ORDER BY rowCount limit " + privacy + " Offset " + offset);

                    while (rs.next()) {
                        if (!rs.getString(1).equals(values.get(0))) {
                            values.add(rs.getString(1));
                        }
                    }
                    if (values.size() >= privacy) {
                        commonString = this.identifyCommonSubStrOfNStr(values);
                        if (commonString != "") {
                            ResultSet rsReturnedTuples = st.executeQuery("select sum(rowCount) from TBL_" + hash + " where " + attribute + " LIKE '%" + commonString + "%'");
                            while (rsReturnedTuples.next()) {
                                nbOfTuples = rsReturnedTuples.getInt(1);
                            }

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
            } else if (condition.contains("<")) {
                int alpha = 5;
                while (!satisfait) {
                    nbOfTuples = 0;
                    int value = Integer.parseInt(valueRequested) + alpha;
                    ResultSet rs = st.executeQuery("select TBL_" + hash + ".rowcount from TBL_" + hash + " where " + attribute + " < " + value);
                    while (rs.next()) {
                        nbOfTuples += rs.getInt(1);
                    }
                    rs.last();
                    int diverseRows = rs.getRow();

                    if (nbOfTuples <= utility && diverseRows >= privacy) {
                        satisfait = true;
                        generalizedValues += attribute + " < " + value;
                        break;
                    } else {
                        alpha += alpha;
                    }
                }
            } else if (condition.contains(">")) {
                int alpha = 5;
                while (!satisfait) {
                    nbOfTuples = 0;
                    int value = Integer.parseInt(valueRequested) - alpha;
                    while (value > 0) {
                        ResultSet rs = st.executeQuery("select rowcount from TBL_" + hash + " where " + attribute + " > " + value);
                        rs.last();
                        int diverseRows = rs.getRow();
                        ResultSet rsCount = st.executeQuery("select sum(rowcount) from TBL_" + hash + " where " + attribute + " > " + value);
                        nbOfTuples = rsCount.getInt(1);
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
        } catch (InstantiationException ex) {
            Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
        }

        return generalizedValues;
    }

    private boolean isLowerPrecedence(String operator, String operand, int hash, int utility, int privacy) {
        String generalizedvalue = generalizeValueAndCheckPrivacy(operand, operator, hash, utility, privacy);
        switch (operator) {

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
                generalizedQuery += generalizeValueAndCheckPrivacy(operand, "", hash, utility, privacy);
                return false;
        }
    }

    private static boolean isOperator(String c) {
        return c.equalsIgnoreCase("AND") || c.equalsIgnoreCase("OR") || c.equals("(") || c.equals(")");

    }

    public String buildPreQuery(String query) {
        TWhereClause whereClause = SqlQueryParser.getWhereClause(query);
        String[] conditions = whereClause.getCondition().toString().split(" or | and ");
        ArrayList<String> tables = SqlQueryParser.getTables(query);
        ArrayList<String> attributes = new ArrayList<>();
        for (String condition : conditions) {
            String[] c = condition.split("[=<>]");
            attributes.add(c[0]);
        }
        String pre_query = "SELECT ";
        String groupBy = " GROUP By ";

        for (int i = 0; i < attributes.size(); i++) {
            if (i == attributes.size() - 1) {
                pre_query += attributes.get(i) + ",count(*) as rowCount from ";
                groupBy += attributes.get(i);
            } else {
                pre_query += attributes.get(i) + ",";
                groupBy += attributes.get(i) + ",";
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
            Connection con = loadDriver();
            DatabaseMetaData metaData = con.getMetaData();
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

        } catch (InstantiationException ex) {
            Logger.getLogger(Test.class
                    .getName()).log(Level.SEVERE, null, ex);

        } catch (IllegalAccessException ex) {
            Logger.getLogger(Test.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }

    private void cacheResult(ResultSet rs, int hashcode) {
        try {
            Connection con = loadDriver();
            Statement stp = con.createStatement();
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
            stp.execute(sbCreateTable.toString());

            while (rs.next()) {
                String insertData = "INSERT INTO TBL_" + hashcode + " (";

                for (int i = 1; i <= columnCount; i++) {
                    if (i == columnCount) {
                        insertData += "rowCount) VALUES(";
                    } else {
                        insertData += rsmd.getColumnLabel(i) + ",";
                    }
                }
                for (int i = 1; i <= columnCount; i++) {
                    Object attribute;
                    if (rs.getObject(i) instanceof String) {
                        attribute = "'" + rs.getString(i) + "'";
                    } else {
                        attribute = rs.getInt(i);
                    }

                    if (i == columnCount) {
                        insertData += attribute + ")";
                    } else {
                        insertData += attribute + ",";
                    }
                }
                stp.executeUpdate(insertData);

            }

        } catch (SQLException ex) {
            Logger.getLogger(Test.class
                    .getName()).log(Level.SEVERE, null, ex);

        } catch (InstantiationException ex) {
            Logger.getLogger(Test.class
                    .getName()).log(Level.SEVERE, null, ex);

        } catch (IllegalAccessException ex) {
            Logger.getLogger(Test.class
                    .getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                rs.close();

            } catch (SQLException ex) {
                Logger.getLogger(Test.class
                        .getName()).log(Level.SEVERE, null, ex);
            }
        }

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

    public static void main(String[] args) {
        Test x = new Test();
        x.generalizeQuery("select * from persons where name='Bechara' or age<20 and id>2", 100, 4);
    }

}
