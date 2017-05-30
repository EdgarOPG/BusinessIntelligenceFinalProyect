package SQL;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


/**
 *
 * @author eopg9
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
public class OracleConnection {
    
    private final static String USUARIO = "oe";
    private final static String PASSWORD = "1234";
    private final static String CONEXION = "jdbc:oracle:thin:@localhost:1521:XE";
    private static OracleConnection INSTANCE;
    private Connection con;

    OracleConnection() {
        this.initConection();
    }

    private void initConection() {
        try {
            Class.forName("oracle.jdbc.OracleDriver");
            con = DriverManager.getConnection(CONEXION, USUARIO, PASSWORD);
//            System.out.println("Connected");
        } catch (ClassNotFoundException | SQLException ex) {
            Logger.getLogger(OracleConnection.class.getName())
                  .log(Level.SEVERE, null, ex);
        }
    }

    public static OracleConnection getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new OracleConnection();
        }
        return INSTANCE;
    }

    public Connection getCon() {
        return con;
    }

}
