# Data engineering with AWS

Nanodegree data engineering lessons from Udacity

## Topics

- [Data Modeling](#01---data-modeling)
  - [Introduction to Data Modeling](#introduction-to-data-modeling)
    - [What is a Data Model?](#what-is-a-data-model)
      - [What is Data Modeling?](#what-is-data-modeling)
      - [Why Data Modeling is important?](#why-data-modeling-is-important)
      - [Who does Data Modeling?](#who-does-data-modeling)
    - [Introduction to Relational Databases](#introduction-to-relational-databases)
      - [What is a Relational Model?](#what-is-a-relational-model)
      - [What is a Relational Database?](#what-is-a-relational-database)
      - [Advantages of Relational Databases](#advantages-of-relational-databases)
      - [Disadvantages of Relational Databases](#disadvantages-of-relational-databases)
      - [Introduction to PostgreSQL](#introduction-to-postgresql)
    - [Introduction to NoSQL Databases](#introduction-to-nosql-databases)
      - [What is not relational databases?](#what-is-not-relational-databases)
      - [Advantages of NoSQL Databases](#advantages-of-nosql-databases)
      - [Disadvantages of NoSQL Databases](#disadvantages-of-nosql-databases)
      - [Introduction to Apache Cassandra](#introduction-to-apache-cassandra)
    - [Remember SQL vs NoSQL](#remember-sql-vs-nosql)

## 01 - Data Modeling

### Introduction to Data Modeling

#### What is a Data Model?

- Is a abstraction that organizes elements of data and how they will relate to each other.

##### What is Data Modeling?

- Is the process of creating data models for a information system to support business and user applications.
- Data modeling can easily translate to database modeling.
- Steps to create a data model:
  - Gather requirements
  - Conceptual data modeling (Entity mapping)
  - Logical data modeling (Tables, columns, relationships)
  - Physical data modeling (The creation of DDL'S - Data Definition Language)

##### Why Data Modeling is important?

- Data organization is critical
- Use cases are easier to implement
- Organized data determines later data use
- Starting early
- Iterative process, having flexibility will help as new information becomes available

##### Who does Data Modeling?

- Data scientists
- Data engineers
- Software engineers
- Product owners
- Business analysts

#### Introduction to Relational Databases

##### What is a Relational Model?

- Model that organizes dta into one or more tables (or "relations") of columns and rows, with a unique key identifying each row.

##### What is a Relational Database?

- Is a digital Database base on the relational model of data.
- The RDBMS (Relational Database Management System) is the software that manages the relational database.
- SQL (Structured Query Language) is the language used across almost all relational database system for querying and maintaining the database.
- Common Relational Databases:
  - PostgreSQL
  - MySQL
  - SQLite
  - Oracle
  - SQL Server
- The structures:
  - Database/Schema: A collection of tables (Database)
  - Table: A collection of rows and columns (Entity)
  - Columns/Attributes: A column in a table (Single Field)
  - Rows/Tuples: A row in a table (Single Item)

##### Advantages of Relational Databases

- Easy of use
- Ability to do JOINS
- Ability to do Aggregations
- Smaller data volumes
- Easier to change business requirements
- Flexibility for queries
- Modeling the data not modeling the queries
- Secondary indexes
- ACID transactions (Atomicity, Consistency, Isolation, Durability):
  - Atomicity: All or nothing
  - Consistency: The data should be correct across all rows and tables.
  - Isolation: Operations do not interfere with each other
  - Durability: Once a transaction is committed, it will remain so

##### Disadvantages of Relational Databases

- Large amounts of data
- Need to be able to store different data types
- Need to high throughput --fast reads
- Need a flexible schema
- Need high availability: 24/7
- Need horizontal scaling: ability to add more servers

##### Introduction to PostgreSQL

Is an open-source object-relational database system that uses and extends the SQL language.

#### Introduction to NoSQL Databases

##### What is not relational databases?

- Databases which simpler design and horizontal scaling.
- Data structures are different than relational databases and are more flexible and faster.
- Common types of NoSQL databases:
  - Document databases: Store data in documents (MongoDB)
  - Partition row store: Wide-column stores (Cassandra)
  - Wide-column stores: Store data in tables, rows, and dynamic columns (Apache HBase)
  - Key-value stores: Data is stored in a schema-less way, and there is no need for a fixed schema (DynamoDB)
  - Graph databases: Data is saved in graph structures with nodes, edges, and properties (Neo4j)
- Common Non Relational Databases:
  - MongoDB
  - Cassandra
  - HBase
  - DynamoDB
  - Neo4j

##### Advantages of NoSQL Databases

- Need to be able to store different data types
- Large amounts of data
- Need horizontal scaling
- Need high throughput --fast reads
- Need high availability: 24/7
- Users are distributed geographically: Multi-region, low latency

##### Disadvantages of NoSQL Databases

- Need ACID transactions
- Need to ability to do JOINS
- Ability to do aggregations and analytics
- Have changing business requirements
- Need to be able to do complex queries
- Have a smaller data volume

##### Introduction to Apache Cassandra

Is a free and open-source, distributed, wide column store, NoSQL database management system designed to handle large amounts of data across many commodity servers, providing high availability with no single point of failure.

- Own query language: CQL (Cassandra Query Language)
- The structures:
  - Keyspace: The outermost container for data in Cassandra (Collection of tables)
  - Table: A collection of rows that contain a sorted map of columns
  - Row: A collection of columns
  - Column: A key-value pair
  - Primary Key: A unique identifier for a row
  - Partition Key: The first part of a primary key
  - Clustering Column: The second part of a primary key
  - Data Column: The actual data

#### Remember SQL vs NoSQL

NoSQL databases and Relational Databases do not replace each other for all tasks. Both do different tasks extremely well, and should be used for the use cases they fit best.
