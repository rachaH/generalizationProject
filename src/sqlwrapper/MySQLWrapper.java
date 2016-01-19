package sqlwrapper;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Implementation of the SQLWrapper interface for SQLite embedded database.
 *
 */
public class MySQLWrapper implements SQLWrapper {

    private String driver = "";
    private String protocol = "";
    private String dbName = "";
    private String dbPath = null;

    private Connection conn = null;

    /**
     * Single instance created upon class loading.
     */
    public MySQLWrapper(String driver, String protocol, String dbName) {
        this.driver = driver;
        this.protocol = protocol;
        this.dbName = dbName;
    }

    public MySQLWrapper() {
    }

    /**
     * Returns the singleton sqLite instance
     *
     * @return mysqlInstance
     */
    public static MySQLWrapper getInstance() {

        MySQLWrapper mysqlInstance = new MySQLWrapper();
        try {
            if (mysqlInstance.conn == null || mysqlInstance.conn.isClosed()) {
                mysqlInstance.conn = mysqlInstance.getConnection();
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }

        return mysqlInstance;
    }

    /**
     * Execute SQL statement for data definition and manipulation
     *
     * @param sql Sql operation
     * @return success of executed operation
     */
    public boolean execute(String sql) {
        Statement st = null;
        try {
            st = this.conn.createStatement();
            boolean successful = st.execute(sql);
            st.close();
            return successful;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                st.close();
            } catch (Exception e) {
            }
        }
    }

    /**
     * Execute SQL statement for data query
     *
     * @param sql Sql operation
     * @return queryResult
     */
    public QueryResult executeQuery(String sql) {

        QueryResult result = null;

        try {
            Statement st = this.conn.createStatement();
            result = new QueryResult(st, sql);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

    /**
     * Commit transaction
     */
    public void commit() {
        try {
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Commit transaction, delete temporary data
     *
     * @return success of the operation
     */
    public boolean flush() {
        try {
            conn.commit();
            conn.close();

            String dbPath = this.dbPath + "/" + this.dbName;
            File file = new File(dbPath);
            boolean succ = file.delete();

            if (succ) {
                File dir = new File(this.dbPath);
                dir.delete();
            }

        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public Connection getConnection() throws SQLException {

        loadDriver();

	    if(this.dbPath == null){
        this.dbPath = System.getProperty("user.home") + "/.Toolbox";

        if (!(new File(this.dbPath)).exists()) {
            boolean success = (new File(this.dbPath)).mkdir();
            if (!success) {
                return null;
            }
        }

	    }
        conn = DriverManager.getConnection(this.protocol + this.dbName, "root", "");
        conn.setAutoCommit(false);

        return conn;
    }

    private void loadDriver() throws SQLException {
        try {
            //System.loadLibrary("sqlite_jni");
            Class.forName(this.driver);

        } catch (ClassNotFoundException cnfe) {
            System.err.println("\nUnable to load the JDBC driver ");
            System.err.println("Please check your CLASSPATH.");
            cnfe.printStackTrace(System.err);
        }
    }
}
