"""
SQLite mock database for local testing without Snowflake.
Tables mirror a typical Snowflake analytics schema.
"""

import sqlite3

_conn: sqlite3.Connection | None = None


def get_connection() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(":memory:")
        _conn.row_factory = sqlite3.Row
        _seed(_conn)
    return _conn


def _seed(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE customers (
            customer_id   INTEGER PRIMARY KEY,
            name          TEXT NOT NULL,
            email         TEXT UNIQUE NOT NULL,
            region        TEXT,
            created_at    TEXT
        );

        CREATE TABLE products (
            product_id    INTEGER PRIMARY KEY,
            name          TEXT NOT NULL,
            category      TEXT,
            unit_price    REAL NOT NULL
        );

        CREATE TABLE orders (
            order_id      INTEGER PRIMARY KEY,
            customer_id   INTEGER REFERENCES customers(customer_id),
            order_date    TEXT,
            status        TEXT
        );

        CREATE TABLE order_items (
            item_id       INTEGER PRIMARY KEY,
            order_id      INTEGER REFERENCES orders(order_id),
            product_id    INTEGER REFERENCES products(product_id),
            quantity      INTEGER NOT NULL,
            unit_price    REAL NOT NULL
        );

        INSERT INTO customers VALUES
            (1,  'Acme Corp',       'acme@example.com',    'North',  '2023-01-15'),
            (2,  'Globex LLC',      'globex@example.com',  'South',  '2023-02-20'),
            (3,  'Initech',         'init@example.com',    'East',   '2023-03-05'),
            (4,  'Umbrella Co',     'umbrella@example.com','West',   '2023-04-12'),
            (5,  'Soylent Corp',    'soylent@example.com', 'North',  '2023-05-01'),
            (6,  'Vandelay Ind',    'van@example.com',     'East',   '2023-06-18'),
            (7,  'Bluth Company',   'bluth@example.com',   'West',   '2023-07-22'),
            (8,  'Sterling Cooper', 'sc@example.com',      'South',  '2023-08-09'),
            (9,  'Dunder Mifflin',  'dm@example.com',      'North',  '2023-09-14'),
            (10, 'Pied Piper',      'pp@example.com',      'West',   '2023-10-30');

        INSERT INTO products VALUES
            (1, 'Widget A',      'Widgets',    9.99),
            (2, 'Widget B',      'Widgets',   14.99),
            (3, 'Gadget Pro',    'Gadgets',   49.99),
            (4, 'Gadget Lite',   'Gadgets',   24.99),
            (5, 'Doohickey X',   'Parts',      4.99),
            (6, 'Thingamajig',   'Parts',      7.49),
            (7, 'Premium Suite', 'Software', 299.99),
            (8, 'Basic Plan',    'Software',  99.99);

        INSERT INTO orders VALUES
            (1,  1, '2024-01-10', 'completed'),
            (2,  2, '2024-01-15', 'completed'),
            (3,  3, '2024-02-01', 'completed'),
            (4,  1, '2024-02-14', 'completed'),
            (5,  4, '2024-02-20', 'completed'),
            (6,  5, '2024-03-05', 'completed'),
            (7,  6, '2024-03-12', 'completed'),
            (8,  2, '2024-03-18', 'completed'),
            (9,  7, '2024-04-01', 'completed'),
            (10, 8, '2024-04-10', 'completed'),
            (11, 9, '2024-04-22', 'completed'),
            (12, 10,'2024-05-01', 'completed'),
            (13, 1, '2024-05-15', 'completed'),
            (14, 3, '2024-05-20', 'completed'),
            (15, 5, '2024-06-01', 'pending');

        INSERT INTO order_items VALUES
            (1,  1,  7, 1, 299.99),
            (2,  1,  1, 5,   9.99),
            (3,  2,  3, 2,  49.99),
            (4,  2,  5,10,   4.99),
            (5,  3,  8, 1,  99.99),
            (6,  3,  2, 3,  14.99),
            (7,  4,  7, 1, 299.99),
            (8,  4,  6, 4,   7.49),
            (9,  5,  3, 1,  49.99),
            (10, 5,  4, 2,  24.99),
            (11, 6,  1,20,   9.99),
            (12, 6,  5,15,   4.99),
            (13, 7,  7, 2, 299.99),
            (14, 8,  3, 3,  49.99),
            (15, 9,  8, 1,  99.99),
            (16,10,  2, 5,  14.99),
            (17,11,  7, 1, 299.99),
            (18,12,  4, 4,  24.99),
            (19,13,  7, 1, 299.99),
            (20,14,  1,10,   9.99),
            (21,15,  3, 2,  49.99);
    """)
    conn.commit()


def run_query(sql: str) -> list[dict]:
    """Execute a SELECT query and return rows as a list of dicts."""
    conn = get_connection()
    cursor = conn.execute(sql)
    rows = cursor.fetchall()
    return [dict(row) for row in rows]
