package com.util;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.*;
import java.util.Stack;


public class TransactionStats {

    static Connection conn = null;
    static PrintStream out = System.out;

    static String[] updateCode = {"", "U", "D", "I"};


    public static void main (String[] args) throws Exception {

        Class.forName ("org.postgresql.Driver");
        conn = openConnection();

        File output = new File ("/Users/dave/temp/DIT" + ".csv");
        output.createNewFile ();

        out = new PrintStream (new BufferedOutputStream (new FileOutputStream(output,false)), true);

        getTransactionStats();

        conn.close();

    }


    private static void getTransactionStats () throws Exception {

        try {

            ResultSetMetaData meta = null;

            String sql = "SELECT tt.transaction_id, tt.seq, tt.update_type, tt.class " +
                         "FROM transaction_trace tt " +
                         "JOIN (SELECT transaction_id, count(*) " +
                         "FROM transaction_trace " +
                         "GROUP BY transaction_id " +
                         "HAVING count(*) > 1) AS ttcount " +
                         "ON ttcount.transaction_id = tt.transaction_id " +
                         "ORDER BY transaction_id, seq";


            PreparedStatement preparedStatement = conn.prepareStatement(sql);

            ResultSet rs = preparedStatement.executeQuery();

            meta = rs.getMetaData();

            Stack<String> transaction = new Stack<String>();
            String lasttxid = "";
            String lastclass = "";
            String lastupdate = "";

            while (rs.next()) {

                String txid = "";
                String update = "";
                String classname = "";
                for (int i = 1; i <= meta.getColumnCount(); i++) {

                    String value = rs.getString(i);
                    String name = meta.getColumnName(i);

                    if (name.equalsIgnoreCase ("transaction_id")) {
                        txid = value;
                        if (!txid.equalsIgnoreCase(lasttxid)) {

                            // we have a new transaction - print the last one out...
                            if (!transaction.empty()) {
                                out.println(transaction);       // deal with first transaction id
                                transaction = new Stack<String>();
                            }
                            lasttxid = txid;                    // update the last seen transaction id
                        }
                    }
                    else if (name.equalsIgnoreCase ("update_type")) {
                        update = updateCode[new Integer(value)];
                    }
                    else if (name.equalsIgnoreCase("class")) {
                        classname = value;

                        if (classname.equalsIgnoreCase(lastclass) && update.equalsIgnoreCase(lastupdate)) {
                            // don't record repeating transactions
                        }
                        else
                            transaction.push (update + ":" + classname);

                        lastupdate = update;
                        lastclass = classname;

                    }
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static Connection openConnection () throws SQLException {

        String jdbcurl = "" + "jdbc:postgresql://localhost:5432/tariff_development1";
        return DriverManager.getConnection (jdbcurl, "postgres", "dave");

    }


}
