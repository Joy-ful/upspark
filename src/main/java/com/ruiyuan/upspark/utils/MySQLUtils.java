package com.ruiyuan.upspark.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class MySQLUtils {

    //jdbc:mysql://localhost:3306/demo
    private static final String URL = "jdbc:mysql://localhost:3306/demo";
    private static final String USER = "root";
    private static final String PASSWORD = "root";
    private static Connection connection = null;

    static {
        //1、加载驱动程序（反射的方法）
        try {
            //  com.mysql.jdbc.Driver
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        //2、连接数据库
        try {
            connection = (Connection) DriverManager.
                    //地址，用户名，密码
                            getConnection(URL, USER, PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection() {
        return connection;
    }


    public void addNewsPaper(String taskid, int mcount) {
        connection = MySQLUtils.getConnection();
        String sql = "insert into uavvideo (taskid,mcount) values(?, ?)";

        java.sql.PreparedStatement ptmt = null;
        try {
            ptmt = connection.prepareStatement(sql);
        } catch (SQLException e1) {
            e1.printStackTrace();
        }

        try {
            ptmt.setString(1, taskid);
            ptmt.setInt(2, mcount);
            ptmt.execute();//执行给定的SQL语句，该语句可能返回多个结果

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}