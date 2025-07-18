
import java.io.*;
import java.sql.*;
import java.util.Properties;

/**
 * @author Ramashankar
 */
public class AmazonRedshift {

    private Connection conn;
    private String jdbcUrl;
    private String user;
    private String password;

    public static void main(String[] args) throws Exception {
        AmazonRedshift redshift = new AmazonRedshift();
        redshift.run();
    }

    public void run() throws Exception {
        loadConfig();
        connect();
        drop();
        create();
        insert();
        query1();
        query2();
        query3();
        close();
    }

    public void loadConfig() throws IOException {
        Properties props = new Properties();
        try (InputStream input = new FileInputStream("config.properties")) {
            props.load(input);
        }
        jdbcUrl = props.getProperty("jdbc.url");
        user = props.getProperty("jdbc.user");
        password = props.getProperty("jdbc.password");
        System.out.println("Loaded config from config.properties");
    }

    public void connect() throws SQLException {
        conn = DriverManager.getConnection(jdbcUrl, user, password);
        System.out.println("Connected to Amazon Redshift");
    }

    public void close() throws SQLException {
        if (conn != null) {
            conn.close();
            System.out.println("Connection closed");
        }
    }

    public void drop() throws SQLException {
        System.out.println("🧹 Dropping existing tables if any...");
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("DROP TABLE IF EXISTS customer CASCADE;");
            stmt.executeUpdate("DROP TABLE IF EXISTS nation CASCADE;");
            stmt.executeUpdate("DROP TABLE IF EXISTS part CASCADE;");
            stmt.executeUpdate("DROP TABLE IF EXISTS region CASCADE;");
            stmt.executeUpdate("DROP TABLE IF EXISTS supplier CASCADE;");
        }
        System.out.println("Drop complete");
    }

    public void create() throws Exception {
        System.out.println("Creating tables from ddl_data/tpch_create.sql...");
        executeSqlFile("ddl_data/tpch_create.sql");
        System.out.println("Tables created");
    }

    public void insert() throws Exception {
        System.out.println("Inserting data from ddl_data/*.sql...");

        System.out.println("inserting region.sql");
        executeSqlFile("ddl_data/region.sql");
        
        System.out.println("inserting nation.sql");
        executeSqlFile("ddl_data/nation.sql");
        
        System.out.println("inserting part.sql");
        executeSqlFile("ddl_data/part.sql");
        
        System.out.println("inserting supplier.sql");
        executeSqlFile("ddl_data/supplier.sql");
        
        System.out.println("inserting customer.sql");
        executeSqlFile("ddl_data/customer.sql");

        System.out.println("Data inserted");
    }

    private void executeSqlFile(String filePath) throws Exception {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
                if (line.trim().endsWith(";")) {
                    String sql = sb.toString();
                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute(sql);
                    }
                    sb.setLength(0);
                }
            }
        }
    }

    public void query1() throws SQLException {
        System.out.println("\nQuery 1: Count of customers from INDIA");
        String sql = """
            SELECT COUNT(*) FROM customer c
            JOIN nation n ON c.c_nationkey = n.n_nationkey
            WHERE n.n_name = 'INDIA';
        """;
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                System.out.println("Customers from India: " + rs.getInt(1));
            }
        }
    }

    public void query2() throws SQLException {
        System.out.println("\nQuery 2: Number of suppliers per region");
        String sql = """
            SELECT r.r_name AS region_name, COUNT(s.s_suppkey) AS supplier_count
            FROM region r
            JOIN nation n ON r.r_regionkey = n.n_regionkey
            JOIN supplier s ON n.n_nationkey = s.s_nationkey
            GROUP BY r.r_name
            ORDER BY region_name;
        """;
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                System.out.println(rs.getString("region_name")
                        + ": " + rs.getInt("supplier_count") + " suppliers");
            }
        }
    }

    public void query3() throws SQLException {
        System.out.println("\nQuery 3: Top 3 nations by number of customers");
        String sql = """
            SELECT n.n_name AS nation_name, COUNT(c.c_custkey) AS customer_count
            FROM nation n
            JOIN customer c ON n.n_nationkey = c.c_nationkey
            GROUP BY n.n_name
            ORDER BY customer_count DESC
            LIMIT 3;
        """;
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            int rank = 1;
            while (rs.next()) {
                System.out.println("#" + rank++ + " " + rs.getString("nation_name")
                        + ": " + rs.getInt("customer_count") + " customers");
            }
        }
    }
}
