package database;

import document.Section;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parser.Member;

public class Postgres implements Database {

    private Connection conn;

    private final static Logger LOGGER = LoggerFactory.getLogger(Postgres.class);

    private final static String SCHEMA_NAME = "lncs_front_matter";

    public Postgres(String server, String username, String password, String database) {
        LOGGER.trace("Initializing Postgres connection");
        String url = "jdbc:postgresql://" + server + "/" + database;
        Properties props = new Properties();
        props.setProperty("user",username);
        props.setProperty("password",password);
        try {
            this.conn = DriverManager.getConnection(url, props);
            createSchema();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        init();
    }

    private void init() {
        createSchema();
        createTableFile();
        createTableSection();
        createTableMember();
    }

    private void createSchema() {
        String query = "CREATE SCHEMA IF NOT EXISTS " + SCHEMA_NAME;
        executeQueryNonResult(query);
    }

    public void createTableFile() {
        ArrayList<String> columns = new ArrayList<String>() {
            {
                add("id SERIAL PRIMARY KEY");
                add("run_id BIGINT");
                add("filename TEXT");
                add("status TEXT");
                add("message TEXT");
            }
        };
        createTable("file", columns);
    }

    public void createTableSection() {
        ArrayList<String> columns = new ArrayList<String>() {
            {
                add("id SERIAL PRIMARY KEY");
                add("run_id BIGINT");
                add("file_id BIGINT");
                add("title TEXT");
                add("num_parts INT");
                add("num_section_lines INT");
                add("num_section_lines_non_empty INT");
                add("num_merged_lines INT");
                add("num_grid_rows INT");
                add("num_grid_columns INT");
                add("all_values_contain_commas BOOLEAN");
                add("comma_ratios TEXT");
                add("affiliation_ratios TEXT");
                add("parser TEXT");
            }
        };
        createTable("section", columns);
    }

    public void createTableMember() {
        ArrayList<String> columns = new ArrayList<String>() {
            {
                add("id SERIAL PRIMARY KEY");
                add("run_id BIGINT");
                add("section_id BIGINT");
                add("role TEXT");
                add("name TEXT");
                add("firstname TEXT");
                add("lastname TEXT");
                add("affiliation TEXT");
            }
        };
        createTable("member", columns);
    }

    private void createTable(String name, ArrayList<String> columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS " + SCHEMA_NAME + "." + name + " (" + System.lineSeparator() );
        sb.append(String.join(", " + System.lineSeparator(), columns));
        sb.append(");");
        executeQueryNonResult(sb.toString());
    }

    private void executeQueryNonResult(String query){
        LOGGER.trace("Executing statement: " + System.lineSeparator() + query);
        Statement st = null;
        try {
            st = conn.createStatement();
            st.execute(query);
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (st != null) {
                    st.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    /*
     * WARNING: Sensitive for SQL Injection.
     */
    @Override
    public int saveSection(long runId, Section section, String parser, int fileId, int mergedLines) {
        return executeInsertStatement("section", 
            arrayOf(
            "run_id",
                "file_id",
                "title",
                "num_parts",
                "num_section_lines",
                "num_section_lines_non_empty",
                "num_merged_lines",
                "num_grid_rows",
                "num_grid_columns",
                "all_values_contain_commas",
                "comma_ratios",
                "affiliation_ratios",
                "parser"
            ), arrayOf(
                runId,
                fileId,
                section.getTitle(),
                section.getSectionInfo().getNumberOfTextParts(),
                section.getContentSize(),
                section.getNonEmptyContentSize(),
                mergedLines,
                section.getSectionInfo().getNumberOfRows(),
                section.getSectionInfo().getNumberOfColumns(),
                section.getSectionInfo().isAllValuesContainsCommas(),
                section.getSectionInfo().stringRepCommaRatios(),
                section.getSectionInfo().stringRepAffiliationRatios(),
                parser
            ));
    }

    private int executeInsertStatement(String tableName, String[] fields, Object[] params) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ");
        sb.append(SCHEMA_NAME);
        sb.append(".");
        sb.append(tableName);
        sb.append(" (");
        sb.append(String.join(", ", fields));
        sb.append(") " + System.lineSeparator());
        sb.append("VALUES (" + String.join(", ", Collections.nCopies(fields.length, "?")) + ") RETURNING id");
        sb.append(System.lineSeparator());
        return executeQueryIntResult(sb.toString(), params);
    }

    private int executeQueryIntResult(String query, Object[] params) {
        LOGGER.trace("Executing prepared statement: " + System.lineSeparator() + query);
        PreparedStatement st = null;
        int result = -1;
        try{
            st = conn.prepareStatement(query);
            for(int i = 0; i < params.length; i++) {
                st.setObject(i + 1, params[i]);
            }
            ResultSet rs = st.executeQuery();
            while (rs.next())
            {
                result = rs.getInt(1);
            }
            rs.close();
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (st != null) {
                    st.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    /*
     * WARNING: Sensitive for SQL Injection.
     */
    @Override
    public int startProcessingFile(long runId, File file) {
        return executeInsertStatement("file", arrayOf("run_id", "filename"), arrayOf(runId, file.getName()));
    }

    @Override
    public void endProcessingFile(int fileId, String representation, String status, String message) {
        String query = "UPDATE " + SCHEMA_NAME + ".file SET status = ?, message = ? WHERE id = ?";
        try (PreparedStatement st = conn.prepareStatement(query)) {
            st.setString(1, status);
            st.setString(2, message);
            st.setInt(3, fileId);
            st.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /*
     * WARNING: Sensitive for SQL Injection.
     */
    @Override
    public void saveMember(long runId, int sectionId, Member m) {
        executeInsertStatement("member", 
            arrayOf(
                "run_id",
                "section_id",
                "role",
                "name",
                "firstname",
                "lastname",
                "affiliation"
            ), arrayOf(
                runId,
                sectionId,
                m.getRole(),
                m.getName(),
                m.getFirstname(),
                m.getLastname(),
                m.getAffiliation()
            ));
    }

    private <E> E[] arrayOf(E... elements) {
        return elements;
    }
}
