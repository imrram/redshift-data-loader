# redshift-data-loader

This repo connects to a Amazon Redshift cluster, loads data from `ddl_files` folder (`customer.sql`, `lineitem.sql`, `nation.sql`, `orders.sql`, `part.sql`, `partsupp.sql`, `region.sql`, `suplier.sql`, `tpch_create.sql`), and supports querying via multiple methods using the Redshift Java Driver.

---

## Directory Structure

```
.
├── AmazonRedshift.java
├── ddl_data/
│   ├── customer.sql
│   ├── orders.sql
│   └── lineitem.sql
│   └── nation.sql
│   └── part.sql
│   └── partsupp.sql
│   └── suplier.sql
│   └── region.sql
│   └── tpch_create.sql
├── run.sh
└── README.md
```

---

## Requirements

- Java 8+
- Amazon Redshift Cluster
- Redshift Java Driver 2.x
- `.sql` files inside `ddl_data/` folder

---

## Dependencies

Driver which are required to execute the code are under jar_files folder:

```
redshift-jdbc42
```

If compiling manually, download the `.jar` and run with:

```bash
javac -cp .:drivers/redshift-jdbc42-2.1.0.9.jar AmazonRedshift.java
java -cp .:drivers/redshift-jdbc42-2.1.0.9.jar AmazonRedshift
```

---

## How to Use

### 1. Set Up AmazonRedshift Connection

Update the `AmazonRedshift.java` with your AmazonRedshift URI (local or Atlas):

```
Load the config property file which contains the Redshift url, username, and password
```

### 2. Load Data

In `main()` of `AmazonRedshift.java`, uncomment these lines to load and nest data:

```java
redshift.run();
```

Then run:

```bash
./run.sh
```

### 3. Run Queries

Uncomment any of these to test:

```java
loadConfig();
connect();
drop();
create();
insert();
query1();
query2();
query3();
close();
```

---

## Queries Explained

| Method             | Description                                      |
|--------------------|--------------------------------------------------|
| `query1`           | Count of customers from INDIA                    |
| `query2`           | Number of suppliers per region                   |
| `query3()`         | Top 3 nations by number of customers             |

---

## Notes

- Files must be in `.sql` formated

---

## License

For academic and learning purposes only. Not intended for production use.

---
