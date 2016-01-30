/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

import java.security.SecureRandom;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import sqlwrapper.MySQLWrapper;
import sqlwrapper.QueryResult;
import sqlwrapper.SqLiteSQLWrapper;

/**
 *
 * @author test
 */
public class RelativeErrorCalculator {

    private DatabaseProperties conf;
    MySQLWrapper mySqlwrapper;
    private HashMap<Integer, ArrayList> qiValuesMap = new HashMap();
    ArrayList<String> Values = new ArrayList();
    ArrayList<String> qiValues = new ArrayList();
    ArrayList<String> sensValues = new ArrayList();
    private Random qiDimenRandomizer;
    private Random qiValuesRandomizer;
    HashMap<String, Integer> originalSensCountList = new HashMap();

    public RelativeErrorCalculator(DatabaseProperties conf, MySQLWrapper mysqlWrapper) throws SQLException {
        this.conf = conf;
        mySqlwrapper = mysqlWrapper;
        String sensValuesQuery = "";

        //get distinct sensitive values 
        sensValuesQuery = "SELECT " + conf.atts[0] + ", count(*)  FROM aoldata GROUP BY " + conf.atts[0];
        QueryResult result = mySqlwrapper.executeQuery(sensValuesQuery);
        while (result.hasNext()) {
            ResultSet res = (ResultSet) result.next();
            Values.add(res.getString(1));
            originalSensCountList.put(res.getString(1), res.getInt(2));
        }
        //get distinct qi values
        String valuesQuery = "";
        for (int i = 1; i < conf.atts.length; i++) {
            valuesQuery = "SELECT DISTINCT " + conf.atts[i] + " FROM aoldata";
            //   System.out.println(conf.qidAtts[i].index + " index ");
            result = mySqlwrapper.executeQuery(valuesQuery);
            while (result.hasNext()) {
                ResultSet res = (ResultSet) result.next();
                qiValues.add(res.getString(1));
            }

            qiValuesMap.put(i, qiValues);
            qiValues = new ArrayList<String>();
        }
        qiDimenRandomizer = new Random();
        qiValuesRandomizer = new Random();
        //System.out.println(qiValuesMap);
    }

    public String createPredicate(int queryDimension, double selectivity) throws SQLException {
        String predicate = "";

        // int previousQIIndex = 0;
        int qiIndex = 0;
        //get randomly attributes from the set of attributes 
        for (int i = 1; i <= queryDimension; i++) {

            qiIndex = qiDimenRandomizer.nextInt(conf.atts.length - 1) + 1;

            //     System.out.println("Random QI Index " + qiIndex);
            ArrayList currValues = qiValuesMap.get(qiIndex);
            //System.out.println("Random Values " + currValues);
            int sizeSelectivity = Math.max((int) (currValues.size() * selectivity), 1);
            //      System.out.println("Size " + qiSizeSelectivity);
            predicate = predicate;
            for (int j = 0; j < sizeSelectivity; j++) {
                predicate = predicate + " (" + conf.atts[qiIndex] + "='" + currValues.get(qiValuesRandomizer.nextInt(sizeSelectivity)).toString().replace("'", "''") + "') OR";
            }
            predicate = predicate.substring(0, predicate.lastIndexOf("OR"));
            predicate = predicate + "AND";
        }
        int sizeSelectivity = (int) (this.Values.size() * selectivity);
        predicate = predicate;
        for (int j = 0; j < sizeSelectivity; j++) {
            predicate = predicate + " (" + conf.atts[0] + "='" + Values.get(qiValuesRandomizer.nextInt(Values.size())).replace("'", "''") + "') OR";
        }
        predicate = predicate.substring(0, predicate.lastIndexOf("OR"));

        return predicate;
    }
//    public int getEstimationValue(String predicate) throws SQLException {
//        //String predicate = createQIPredicate(2, 0.1);
//        // System.out.println(predicate);
//        String attributes = "";
//        for (int i = 1; i < conf.qidAtts.length; i++) {
//            attributes = attributes + "ATT_" + this.conf.qidAtts[i].index + ", ";
//        }
//        attributes = attributes.substring(0, attributes.lastIndexOf(", "));
//        String query = "SELECT COUNT(*) FROM (SELECT EID, " + attributes
//                + " FROM " + this.anonAfterAnonymization.getName() + ") as a, (SELECT EID, ATT_" + this.conf.sensitiveAtts[0].index
//                + " FROM " + this.anonAfterAnonymization.getName() + ") as b WHERE a.EID = b.EID AND " + predicate;
//      //  System.out.println("Estimation Query " + query);
//
//        QueryResult result = mySqlwrapper.executeQuery(query);
//
//        ResultSet res = (ResultSet) result.next();
//        int value = res.getInt(1);
//        //res.close();
//      //  System.out.println("Count Estimation " + res.getString(1));
//        return value;
//
//    }

    public int getOriginalValue(String predicate) throws SQLException {
        String queryOriginal = "SELECT COUNT(*) FROM aoldata WHERE" + predicate;
        System.out.println("Original Query " + queryOriginal);
        QueryResult result = mySqlwrapper.executeQuery(queryOriginal);
        ResultSet res = (ResultSet) result.next();
        int value = res.getInt(1);
        //res.close();
        System.out.println("Count Original " + res.getString(1));
        return value;
    }

    public int getGeneralizedValue(String query) throws SQLException {
        String queryGeneralized = "SELECT COUNT(*) FROM (" + query + ") as T";
        System.out.println("Generalized Query " + queryGeneralized);
        QueryResult result = mySqlwrapper.executeQuery(queryGeneralized);
        ResultSet res = (ResultSet) result.next();
        int value = res.getInt(1);
        //res.close();
        System.out.println("Count Generalized " + res.getString(1));
        return value;
    }

    public double getAverageRelativeError(int numberOfQueries, int qDimension, double selectivity) throws SQLException {
        String predicate = "";
        int actual = 0;
        int estimation = 0;
        long actualTime = 0;
        long estimationTime = 0;
        double error = 0;
        long timeError = 0;
        double totErrors = 0;
        double totTimeError = 0;
        int countValidQueries = 0;
        
        for (int i = 0; i < numberOfQueries; i++) {
            predicate = createPredicate(qDimension, selectivity);
            long s1 = System.currentTimeMillis();
            actual = getOriginalValue(predicate);
            long f1 = System.currentTimeMillis();
            actualTime=f1-s1;
           // System.out.println("Actual Time: "+actualTime);
            Test x = new Test();
            long s = System.currentTimeMillis();
            String generalizedquery = x.generalizeQuery("SELECT * FROM aoldata WHERE" + predicate, 1800, 5);
            long f = System.currentTimeMillis();
            estimationTime = f - s;
            //System.out.println("Estimation Time: "+estimationTime);
            estimation = getGeneralizedValue(generalizedquery);
            if (actual != 0) {
                error = (double) Math.abs(actual - estimation) / (double) actual;
                timeError = timeError + actualTime;
                totTimeError = totTimeError + estimationTime;
                System.out.println(error);
                totErrors = totErrors + error;
                countValidQueries++;
            }
        }
        System.out.println("Average actual time consuming :" + timeError / (double) countValidQueries);
        System.out.println("Average estimation time consuming :" + totTimeError / (double) countValidQueries);
        return totErrors / (double) countValidQueries;
    }


}
