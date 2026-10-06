package database;

public class DatabaseFactory {

    /*
     * Currently only implementation for Postgres is available.
     */
    public static Database getDatabase(String server, String username, String password, String database) {
        return new Postgres(server, username, password, database);
    }

}
