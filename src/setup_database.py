import duckdb

def setup_database():
    
    # Kết nối database
    con = duckdb.connect('metrics.duckdb')

    try:
        con.execute("CREATE OR REPLACE TABLE customers AS SELECT * FROM read_csv_auto('data/customers.csv')")
        con.execute("CREATE OR REPLACE TABLE orders AS SELECT * FROM read_csv_auto('data/orders.csv')")
        con.execute("CREATE OR REPLACE TABLE order_items AS SELECT * FROM read_csv_auto('data/order_items.csv')")
        
        # Check data counts
        print("Data counts:")
        print("- Customers:", con.execute("SELECT COUNT(*) FROM customers").fetchone()[0])
        print("- Orders:", con.execute("SELECT COUNT(*) FROM orders").fetchone()[0])
        print("- Order items:", con.execute("SELECT COUNT(*) FROM order_items").fetchone()[0])
        
    except Exception as e:
        print(f"Error loading data: {e}")
        return False
    
    con.close()
    print("Database setup completed")
    return True

if __name__ == "__main__":
    setup_database()