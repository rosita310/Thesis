import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import database.Database;
import database.DatabaseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Program {

    private final static Logger LOGGER = LoggerFactory.getLogger(Program.class);
    private final static String PROPERTIES_FILE = "../.env";

    private final Properties properties;
    private final Database database;
    private final long runId;

    private Program(Properties properties, Database database) {
        this.properties = properties;
        this.database = database;
        this.runId = System.currentTimeMillis();
        LOGGER.info("Run id: " + runId);
    }

    /**
     * Iterate through all files and delegates the processing to the FileProcessor.
     * @throws Exception
     */
    public void execute() throws Exception {
        LOGGER.info("Start executing");
        String directoryPath = properties.getProperty("RAW_DATA") + properties.getProperty("LNCS_FRONT_MATTER_SUBDIR");
        File directory = new File(directoryPath);
        String[] filepaths = directory.list();
        int numberOfFiles = filepaths.length;
        LOGGER.info("Number of files to process: {}", numberOfFiles);
        FileProcessor fp = null;
        int numberOfFilesProcessed = 0;
        for (String file : filepaths) {
            String filepath = directoryPath + file;
            fp = new FileProcessor(filepath, database, runId);
            fp.execute();
            numberOfFilesProcessed++;
            double percentage = ((double)numberOfFilesProcessed / (double)numberOfFiles) * 100;
            LOGGER.info("Processed {} of {} ({}%)", numberOfFilesProcessed, numberOfFiles, percentage);
        }
    }
    // End class

    /**
     * Main entry of the application.
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream(PROPERTIES_FILE)) {
            properties.load(fis);
        } catch (FileNotFoundException ex) {
            LOGGER.warn("Properties file not found.");
        } catch (IOException ex) {
            LOGGER.warn("Unable to read properties file.");
        }
        Database database = DatabaseFactory.getDatabase(
                properties.getProperty("POSTGRES_SERVER"),
                properties.getProperty("POSTGRES_USER"),
                properties.getProperty("POSTGRES_PASSWORD"),
                properties.getProperty("POSTGRES_DB")
        );
        Program p = new Program(properties, database);
        p.execute();
    }
}
