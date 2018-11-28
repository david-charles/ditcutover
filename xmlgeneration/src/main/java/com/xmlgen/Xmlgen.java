package com.xmlgen;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;


public class Xmlgen {


    static String transactionId = "";
    static int messageId = 0;
    static int recordSequence = 0;
    static String recordOperation = "";

    static Map<String, String> recordCode = new HashMap<String,String>();
    static Map<String, String> subRecordCode = new HashMap<String,String>();

    static Connection conn = null;
    static PrintStream out = System.out;


    public static void main (String[] args) throws Exception {

        initRecordCodes();

        String fromDate = "";
        String sequence = "";

        if (args.length != 2) {
            System.out.println ("Usage: xmlgen <fromdate> <sequence>");
        } else {
            fromDate = args[0];
            sequence = args[1];
        }

        Class.forName ("org.postgresql.Driver");
        conn = openConnection();

        out = new PrintStream (new BufferedOutputStream(new FileOutputStream ("/tmp/DIT" + sequence + ".xml")), true);

        out.print (envelopeHeader (sequence));


        // TODO - should make this sequenced by transaction id ?

        // NB to obtain certain files, comment out relevant lines...

        // First file - regulations
        getRegulations (fromDate, "U");

        getRegulations (fromDate, "C");


        // Second file - end date measures
        getMeasures (fromDate, "U");

        // Third file - create new measures
        getMeasures (fromDate, "C");


        out.print (envelopeTrailer());

        conn.close();

    }

    // TODO - REFACTOR - to split into separate classes - per element type

    // TODO - OPTIMISE USE OF SQL - pass in as parameter to avoid repeating code

    private static void getRegulations (String fromDate, String operation) throws Exception {

        try {

            ResultSetMetaData meta = null;

            String sql = "SELECT " +
                    "base_regulation_role," +
                    "base_regulation_id," +
                    "to_char(published_date,'YYYY-MM-DD') AS published_date," +
                    "officialjournal_number," +
                    "officialjournal_page," +
                    "to_char(validity_start_date,'YYYY-MM-DD') AS validity_start_date," +
                    "to_char(validity_end_date,'YYYY-MM-DD') AS validity_end_date," +
                    "to_char(effective_end_date,'YYYY-MM-DD') AS effective_end_date," +
                    "community_code," +
                    "regulation_group_id," +
                    "antidumping_regulation_role," +
                    "related_antidumping_regulation_id," +
                    "complete_abrogation_regulation_role," +
                    "complete_abrogation_regulation_id," +
                    "explicit_abrogation_regulation_role," +
                    "explicit_abrogation_regulation_id," +
                    "replacement_indicator," +
                    "CASE stopped_flag WHEN 'f' THEN '0' ELSE '1' END AS stopped_flag," +
                    "information_text," +
                    "CASE approved_flag WHEN 'f' THEN '0' ELSE '1' END AS approved_flag," +
                    "CASE operation WHEN 'U' THEN '1' WHEN 'C' THEN '3' WHEN 'D' THEN '2' ELSE '?' END AS operation," +
                    "CASE operation WHEN 'U' THEN '2' WHEN 'C' THEN '3' WHEN 'D' THEN '1' ELSE '9' END AS _sortorder" +
                    " FROM base_regulations_oplog " +
                    " WHERE operation_date >= to_date(?,'DD.MM.YYYY') " +
                    " AND operation = ? " +
                    " ORDER BY _sortorder, oid";


            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.setString (1, fromDate);
            preparedStatement.setString (2, operation);

            ResultSet rs = preparedStatement.executeQuery();

            meta = rs.getMetaData();

            while (rs.next()) {
                String recordXml = "";
                for (int i = 1; i <= meta.getColumnCount(); i++) {

                    String value = rs.getString(i);
                    String name = meta.getColumnName(i);

                    if (name.equals ("operation"))
                        recordOperation = value;
                    else {
                        if (value != null) {
                            if (!name.startsWith("_")) {
                                value = toXml (name, value);
                                recordXml = recordXml + value;
                            }
                        }
                    }
                }

                out.print (newTransaction());

                out.print (message (record (recordXml, "base.regulation")));

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void getMeasures (String fromDate, String operation) throws Exception {

        try {

            ResultSetMetaData meta = null;

            String sql = "SELECT " +
                    "measure_sid," +
                    "measure_type_id as measure_type," +
                    "geographical_area_id as geographical_area," +
                    "goods_nomenclature_item_id," +
                    "additional_code_type_id AS additional_code_type," +
                    "additional_code_id AS additional_code," +
                    "ordernumber," +
                    "reduction_indicator," +
                    "to_char(validity_start_date,'YYYY-MM-DD') as validity_start_date," +
                    "measure_generating_regulation_role," +
                    "measure_generating_regulation_id," +
                    "to_char(validity_end_date,'YYYY-MM-DD') as validity_end_date," +
                    "justification_regulation_role," +
                    "justification_regulation_id," +
                    "CASE stopped_flag WHEN 'f' THEN '0' ELSE '1' END AS stopped_flag," +
                    "geographical_area_sid," +
                    "goods_nomenclature_sid," +
                    "additional_code_sid," +
                    "CASE operation WHEN 'U' THEN '1' WHEN 'C' THEN '3' WHEN 'D' THEN '2' ELSE '?' END AS operation," +
                    "CASE operation WHEN 'U' THEN '2' WHEN 'C' THEN '3' WHEN 'D' THEN '1' ELSE '9' END AS _sortorder" +
                    " FROM measures_oplog " +
                    " WHERE operation_date >= to_date(?,'DD.MM.YYYY') " +
                    " AND operation = ? " +
                    " ORDER BY _sortorder, oid";


            PreparedStatement preparedStatement = conn.prepareStatement (sql);
            preparedStatement.setString (1, fromDate);
            preparedStatement.setString (2, operation);

            ResultSet rs = preparedStatement.executeQuery();

            meta = rs.getMetaData();

            String measureSid = "";

            while (rs.next()) {
                String recordXml = "";
                for (int i = 1; i <= meta.getColumnCount(); i++) {

                    String value = rs.getString(i);
                    String name = meta.getColumnName(i);

                    if (name.equals ("operation"))
                        recordOperation = value;
                    else {
                        if (value != null) {
                            if (name.equals ("measure_sid"))
                                measureSid = value;
                            if (!name.startsWith("_")) {
                                value = toXml (name, value);
                                recordXml = recordXml + value;
                            }
                        }
                    }
                }

                out.print (newTransaction());

                out.print (message (record (recordXml, "measure")));

                getMeasureComponents (fromDate, measureSid);
                getMeasureConditions (fromDate, measureSid);
                getFootnoteAssociationMeasures (fromDate, measureSid);
                getMeasureExcludedGeographicalAreas (fromDate, measureSid);

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void getMeasureComponents (String fromDate, String measure_sid) throws Exception {

        try {

            ResultSetMetaData meta = null;

            String sql = "SELECT " +
                    "measure_sid," +
                    "LEFT (duty_expression_id, 2) AS duty_expression_id," +
                    "duty_amount::numeric AS duty_amount," +
                    "monetary_unit_code," +
                    "measurement_unit_code," +
                    "measurement_unit_qualifier_code," +
                    "CASE operation WHEN 'U' THEN '1' WHEN 'C' THEN '3' WHEN 'D' THEN '2' ELSE '?' END AS operation" +
                    " FROM measure_components_oplog " +
                    " WHERE operation_date >= to_date(?,'DD.MM.YYYY')" +
                    " AND measure_sid = ?" +
                    " ORDER BY oid";

            PreparedStatement preparedStatement = conn.prepareStatement (sql);
            preparedStatement.setString (1, fromDate);
            preparedStatement.setInt (2, (int) Integer.parseInt (measure_sid));

            ResultSet rs = preparedStatement.executeQuery();

            meta = rs.getMetaData();

            while (rs.next()) {
                String recordXml = "";
                for (int i = 1; i <= meta.getColumnCount(); i++) {

                    String value = rs.getString(i);
                    String name = meta.getColumnName(i);

                    if (name.equals ("operation"))
                        recordOperation = value;
                    else {
                        if (value != null && !value.equalsIgnoreCase ("")) {
                            value = toXml (name, value);
                            recordXml = recordXml + value;
                        }
                    }
                }

                out.print (message (record (recordXml, "measure.component")));

            }

        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }


    private static void getMeasureConditions (String fromDate, String measure_sid) throws Exception {

        try {

            ResultSetMetaData meta = null;

            String sql = "SELECT " +
                    "measure_condition_sid," +
                    "measure_sid," +
                    "condition_code," +
                    "component_sequence_number," +
                    "action_code," +
                    "certificate_type_code," +
                    "certificate_code," +
                    "CASE operation WHEN 'U' THEN '1' WHEN 'C' THEN '3' WHEN 'D' THEN '2' ELSE '?' END AS operation" +
                    " FROM measure_conditions_oplog " +
                    " WHERE operation_date >= to_date(?,'DD.MM.YYYY')" +
                    " AND measure_sid = ?" +
                    " ORDER BY oid";

            PreparedStatement preparedStatement = conn.prepareStatement (sql);
            preparedStatement.setString (1, fromDate);
            preparedStatement.setInt (2, (int) Integer.parseInt (measure_sid));

            ResultSet rs = preparedStatement.executeQuery();

            meta = rs.getMetaData();

            String measureConditionSid = "";

            while (rs.next()) {
                String recordXml = "";
                for (int i = 1; i <= meta.getColumnCount(); i++) {

                    String value = rs.getString(i);
                    String name = meta.getColumnName(i);

                    if (name.equals("operation"))
                        recordOperation = value;
                    else {
                        if (name.equals ("measure_condition_sid"))
                            measureConditionSid = value;
                        if (value != null && !value.equalsIgnoreCase("")) {
                            value = toXml (name, value);
                            recordXml = recordXml + value;
                        }
                    }
                }

                out.print (message (record (recordXml, "measure.condition")));

                getMeasureConditionComponents (fromDate, measureConditionSid);

            }

        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }


    private static void getMeasureConditionComponents (String fromDate, String measure_condition_sid) throws Exception {

        try {

            ResultSetMetaData meta = null;

            Statement stmt = conn.createStatement();
            String sql = "SELECT " +
                    "measure_condition_sid," +
                    "LEFT (duty_expression_id, 2) AS duty_expression_id," +
                    "duty_amount::numeric AS duty_amount," +
                    "monetary_unit_code," +
                    "measurement_unit_code," +
                    "measurement_unit_qualifier_code," +
                    "CASE operation WHEN 'U' THEN '1' WHEN 'C' THEN '3' WHEN 'D' THEN '2' ELSE '?' END AS operation" +
                    " FROM measure_condition_components_oplog " +
                    " WHERE operation_date >= to_date(?,'DD.MM.YYYY')" +
                    " AND measure_condition_sid = ?" +
                    " ORDER BY oid";


            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.setString (1, fromDate);
            preparedStatement.setInt (2, (int) Integer.parseInt (measure_condition_sid));

            ResultSet rs = preparedStatement.executeQuery();

            meta = rs.getMetaData();

            while (rs.next()) {
                String recordXml = "";
                for (int i = 1; i <= meta.getColumnCount(); i++) {

                    String value = rs.getString(i);
                    String name = meta.getColumnName(i);

                    if (name.equals ("operation"))
                        recordOperation = value;
                    else {
                        if (value != null && !value.equalsIgnoreCase("")) {
                            value = toXml (name, value);
                            recordXml = recordXml + value;
                        }
                    }
                }

                out.print (message (record (recordXml, "measure.condition.component")));

            }

        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }


    private static void getMeasureExcludedGeographicalAreas (String fromDate, String measure_sid) throws Exception {

        try {

            ResultSetMetaData meta = null;

            Statement stmt = conn.createStatement();
            String sql = "SELECT " +
                    "measure_sid," +
                    "excluded_geographical_area," +
                    "geographical_area_sid," +
                    "CASE operation WHEN 'U' THEN '1' WHEN 'C' THEN '3' WHEN 'D' THEN '2' ELSE '?' END AS operation" +
                    " FROM measure_excluded_geographical_areas_oplog " +
                    " WHERE operation_date >= to_date(?,'DD.MM.YYYY')" +
                    " AND measure_sid = ?" +
                    " ORDER BY oid";


            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.setString (1, fromDate);
            preparedStatement.setInt (2, (int) Integer.parseInt (measure_sid));

            ResultSet rs = preparedStatement.executeQuery();

            meta = rs.getMetaData();

            while (rs.next()) {
                String recordXml = "";
                for (int i = 1; i <= meta.getColumnCount(); i++) {

                    String value = rs.getString(i);
                    String name = meta.getColumnName(i);

                    if (name.equals ("operation"))
                        recordOperation = value;
                    else {
                        if (value != null && !value.equalsIgnoreCase("")) {
                            value = toXml (name, value);
                            recordXml = recordXml + value;
                        }
                    }
                }

                out.print (message (record (recordXml, "measure.excluded.geographical.area")));

            }

        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }


    private static void getFootnoteAssociationMeasures (String fromDate, String measure_sid) throws Exception {

        try {

            ResultSetMetaData meta = null;

            Statement stmt = conn.createStatement();
            String sql = "SELECT " +
                    "measure_sid," +
                    "footnote_type_id," +
                    "footnote_id," +
                    "CASE operation WHEN 'U' THEN '1' WHEN 'C' THEN '3' WHEN 'D' THEN '2' ELSE '?' END AS operation" +
                    " FROM footnote_association_measures_oplog " +
                    " WHERE operation_date >= to_date(?,'DD.MM.YYYY')" +
                    " AND measure_sid = ?" +
                    " ORDER BY oid";


            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.setString (1, fromDate);
            preparedStatement.setInt (2, (int) Integer.parseInt (measure_sid));

            ResultSet rs = preparedStatement.executeQuery();

            meta = rs.getMetaData();

            while (rs.next()) {
                String recordXml = "";
                for (int i = 1; i <= meta.getColumnCount(); i++) {

                    String value = rs.getString(i);
                    String name = meta.getColumnName(i);

                    if (name.equals ("operation"))
                        recordOperation = value;
                    else {
                        if (value != null && !value.equalsIgnoreCase ("")) {
                            value = toXml(name, value);
                            recordXml = recordXml + value;
                        }
                    }
                }

                out.print (message (record (recordXml, "footnote.association.measure")));

            }

        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }


    private static String getTransactionSequence () {

        // If error occurs, txid will be blank and thus cause the resulting XML to fail validation...

        String txid = "";

        try {

            ResultSetMetaData meta = null;

            Statement stmt = conn.createStatement();
            String sql = "SELECT nextval('transactionid')";

            PreparedStatement preparedStatement = conn.prepareStatement(sql);

            ResultSet rs = preparedStatement.executeQuery();

            meta = rs.getMetaData();

            while (rs.next()) {
                for (int i = 1; i <= meta.getColumnCount(); i++) {

                    String value = rs.getString(i);
                    String name = meta.getColumnName(i);

                    if (name.equals ("nextval"))
                        txid = value;

                }

            }

        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return txid;

    }


    private static Connection openConnection () throws SQLException {

        // TODO - use properties file

        String jdbcurl = "" + "jdbc:postgresql://localhost:5432/tariff_development1";
        return DriverManager.getConnection (jdbcurl, "postgres", "dave");

    }


    // TODO - use XML formatting class to ensure readable indents etc

    private static String toXml (String name, String value) {

        return "                    <oub:" + name.replace("_",".") + ">" +
                value +
                "</oub:" + name.replace("_",".") + ">\n";

    }


    private static String envelopeHeader (String sequence) {

        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<env:envelope xmlns=\"urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0\" " +
                "xmlns:env=\"urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0\" " +
                "id=\"" + sequence + "\">\n";
    }


    private static String envelopeTrailer () {

        return "    </env:transaction>\n" +
               "</env:envelope>\n";

    }


    private static String newTransaction () {

        String ret = "";

        if (!transactionId.equalsIgnoreCase(""))

            // only close transaction element if one started
            // first in file doesn't have an earlier one...hence blank id
            ret = "   </env:transaction>\n";

        // get next transaction id value from database sequence
        transactionId = getTransactionSequence();

        ret = ret + "    <env:transaction id=\"" + transactionId + "\">\n";

        return ret;

    }


    private static String message (String recordXml) {

        messageId++;

        return  "       <env:app.message id=\"" + Integer.toString(messageId) + "\">\n" +
                "           <oub:transmission xmlns:oub=\"urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0\" xmlns:env=\"urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0\">\n" +

                recordXml +

                "           </oub:transmission>\n" +
                "       </env:app.message>\n";
    }


    private static String record (String data, String dataType) {

        recordSequence++;

        return "            <oub:record>\n" +
                "               <oub:transaction.id>" + transactionId + "</oub:transaction.id>\n" +
                "               <oub:record.code>" + getRecordCode (dataType) + "</oub:record.code>\n" +
                "               <oub:subrecord.code>" + getSubRecordCode (dataType) + "</oub:subrecord.code>\n" +
                "               <oub:record.sequence.number>" + Integer.toString(recordSequence) + "</oub:record.sequence.number>\n" +
                "               <oub:update.type>" + recordOperation + "</oub:update.type>\n" +
                "               <oub:" + dataType + ">\n" +
                data +
                "               </oub:" + dataType + ">\n" +
                "           </oub:record>\n";

    }


    private static String getRecordCode (String dataType) {

        return recordCode.get (dataType);

    }


    private static String getSubRecordCode (String dataType) {

        return subRecordCode.get (dataType);

    }

    private static void initRecordCodes () {

        recordCode.put ("base.regulation", "285");
        subRecordCode.put ("base.regulation","00");


        recordCode.put ("measure", "430");
        subRecordCode.put ("measure","00");

        recordCode.put ("measure.component", "430");
        subRecordCode.put ("measure.component","05");

        recordCode.put ("measure.condition", "430");
        subRecordCode.put ("measure.condition","10");

        recordCode.put ("measure.condition.component", "430");
        subRecordCode.put ("measure.condition.component","11");

        recordCode.put ("measure.excluded.geographical.area", "430");
        subRecordCode.put ("measure.excluded.geographical.area","15");

        recordCode.put ("footnote.association.measure", "430");
        subRecordCode.put ("footnote.association.measure","20");

    }

}
