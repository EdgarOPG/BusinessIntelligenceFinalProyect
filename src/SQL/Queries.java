package SQL;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;


/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Edgar
 */
public class Queries {

    Connection con = OracleConnection.getInstance().getCon();

    public Queries() {

    }

    public List<Object[]> getList(String query) throws SQLException {
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        ResultSetMetaData rsmd = rs.getMetaData();
        int rowIndex = 0;
        List<Object[]> itemList = new ArrayList<>();
        while (rs.next()) {
            Object[] itemArray = new Object[rsmd.getColumnCount()];
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                itemArray[i - 1] = rs.getObject(i);
            }
            itemList.add(rowIndex, itemArray);
            rowIndex++;
        }
        rs.close();
        return itemList;
    }

    public List<Object> getOrderDetails(Integer id) throws SQLException {
        Statement stmt = con.createStatement();
        String query = "SELECT tablaorders.*\n"
                + "  FROM xmlorders x,\n"
                + "  XMLTABLE ('/Orders/Order[@id = " + id.toString() + "]' \n"
                + "  PASSING x.orders \n"
                + "  COLUMNS order_id NUMBER PATH '@id',\n"
                + "  order_date VARCHAR2(20) PATH 'Date',\n"
                + "  order_mode VARCHAR2(20) PATH 'Mode',\n"
                + "  customer_id NUMBER PATH 'Customer/@id',\n"
                + "  customer_name VARCHAR2(30) PATH 'Customer',\n"
                + "  total NUMBER PATH 'Total',\n"
                + "  sales_rep_id NUMBER PATH 'Sales-rep/@id',\n"
                + "  sales_rep VARCHAR(30) PATH 'Sales-rep') tablaorders";
        ResultSet rs = stmt.executeQuery(query);
        ResultSetMetaData rsmd = rs.getMetaData();
        List<Object> detailsList = new ArrayList<>();
        Object item;
        if (rs.next()) {
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                item = rs.getObject(i);
                detailsList.add(item);
            }
        }
        rs.close();
        return detailsList;
    }

    public Integer getLastIndex() throws SQLException {
        Statement stmt = null;
        String query = "  SELECT tablaorders.*\n"
                + "  FROM xmlorders x,\n"
                + "    XMLTABLE ('/Orders/Order[not (@id < preceding-sibling::Order/@id)"
                + "    and not (@id < following-sibling::Order/@id)]' \n"
                + "    PASSING x.orders \n"
                + "    COLUMNS \n"
                + "    max_id NUMBER PATH '@id') tablaorders";
        stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        rs.next();
        return rs.getInt(1) + 1;
    }

    public static void main(String[] args) throws SQLException {

    }
}
