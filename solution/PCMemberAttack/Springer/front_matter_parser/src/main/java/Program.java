import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import database.Database;
import database.DatabaseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Program {

    private final static Logger LOGGER = LoggerFactory.getLogger(Program.class);
    private final static String PROPERTIES_FILE = "solution/.env";
    public final static boolean UPDATE_DATABASE = true;

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
        File directory = new File(properties.getProperty("RAW_DATA"), properties.getProperty("LNCS_FRONT_MATTER_SUBDIR"));
        String[] filepaths = directory.list((dir, name) -> name.toLowerCase().endsWith(".pdf"));
        if (filepaths == null) {
            throw new FileNotFoundException("Directory not found: " + directory);
        }
        Arrays.sort(filepaths);
        int numberOfFiles = filepaths.length;
        LOGGER.info("Number of files to process: {}", numberOfFiles);
        FileProcessor fp = null;
        int numberOfFilesProcessed = 0;
        for (String file : filepaths) {
            String filepath = new File(directory, file).getPath();
            fp = new FileProcessor(filepath, database, runId);
            fp.execute();
            numberOfFilesProcessed++;
            double percentage = ((double)numberOfFilesProcessed / (double)numberOfFiles) * 100;
            LOGGER.info("Processed {} of {} ({}%)", numberOfFilesProcessed, numberOfFiles, percentage);
        }
    }
    // End class

    /**
     * Looks for solution/.env from the working directory upwards, so the program can be started
     * from this project folder as well as from the repository root.
     */
    private static File findPropertiesFile() {
        for (File dir = new File("").getAbsoluteFile(); dir != null; dir = dir.getParentFile()) {
            File candidate = dir.getName().equals("solution") ? new File(dir, ".env") : new File(dir, PROPERTIES_FILE);
            if (candidate.isFile()) {
                return candidate;
            }
        }
        return new File(PROPERTIES_FILE);
    }

    /**
     * Main entry of the application.
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        File propertiesFile = args.length > 0 ? new File(args[0]) : findPropertiesFile();
        LOGGER.info("Reading properties from {}", propertiesFile);
        try (FileInputStream fis = new FileInputStream(propertiesFile)) {
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
