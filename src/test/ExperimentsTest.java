/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

import gudusoft.gsqlparser.nodes.TExpressionList;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import sqlwrapper.MySQLWrapper;
import sqlwrapper.SqlQueryParser;

/**
 *
 * @author Racha
 */
public class ExperimentsTest {

    Connection localCon = null;
    private MySQLWrapper mySqlWrapperLocal;
    public ArrayList<String> attributes= new ArrayList<>();

    public ExperimentsTest() {
        try {
            mySqlWrapperLocal = new MySQLWrapper("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/", "serverdata");
            localCon = mySqlWrapperLocal.getConnection();
            getColumns();
            String[] atts = new String[attributes.size()];
            for (int i = 0; i < attributes.size(); i++) {
                atts[i] = attributes.get(i);
            }
            DatabaseProperties dbp = new DatabaseProperties(atts);
            RelativeErrorCalculator re = new RelativeErrorCalculator(dbp, mySqlWrapperLocal);
            System.out.println("Average relative error : " +re.getAverageRelativeError(100, 2, 0.02));
            
        } catch (SQLException ex) {
            Logger.getLogger(ExperimentsTest.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    private boolean getColumns() {
        try {
            DatabaseMetaData metaData = localCon.getMetaData();
            String SCHEMA_NAME = "${YOUR_SCHEMA_NAME}";
            String tableType = "COLUMN";
            ArrayList<String> queries = new ArrayList<>();
            ResultSet resultSet = metaData.getColumns(null, null, "aoldata", null);
            while (resultSet.next()) {
                if(!resultSet.getString("COLUMN_NAME").toLowerCase().equals("querytime") && !resultSet.getString("COLUMN_NAME").toLowerCase().equals("itemrank")){
                attributes.add(resultSet.getString("COLUMN_NAME"));
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(Test.class
                    .getName()).log(Level.SEVERE, null, ex);

        }
        return false;
    }
    public boolean checkAttributes(String c){
        return false;
    }
 
    public static void main(String[] args) {
        ExperimentsTest x = new ExperimentsTest();
      }
}
