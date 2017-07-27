package com.spark;

import org.apache.log4j.DailyRollingFileAppender;

import java.sql.*;

/**
 * Created by Vivek on 7/26/2017.
 */
public class DBCon {

    private String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private String conString = "jdbc:sqlserver://localhost:1433;DatabaseName=test;integratedSecurity=true";


    private Connection con = null;
    private PreparedStatement ps = null;


    public void createCon() {
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(conString);
        } catch (ClassNotFoundException cnfEx) {
            cnfEx.printStackTrace();
        } catch (SQLException sqlEx) {
            sqlEx.printStackTrace();
        }
    }

    public void save(String data) {
        // Data delimiter is :
        try {
            String[] vals = data.split(":");
            ps = con.prepareStatement("exec sp_save ?, ?, ?");
            ps.setEscapeProcessing(true);
            ps.setQueryTimeout(90);

            // Set the params to stored procedure call
            ps.setString(1, vals[0]);
            ps.setString(2, vals[1]);
            ps.setString(3, vals[2]);

            ps.execute();

        } catch (SQLException sqlEx) {
            sqlEx.printStackTrace();
        }
    }

    public void close() {
        try {
            ps.close();
            con.close();
        } catch (SQLException sqlEx) {
            sqlEx.printStackTrace();
        }
    }
}
