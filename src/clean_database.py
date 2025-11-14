import duckdb
import os

def cleanup_database():
    """Clean up DuckDB database - remove all metric tables"""
    
    # Connect to database
    con = duckdb.connect('metrics.duckdb')
    print("Connected to DuckDB")
    
    try:
        # Get all tables except the original source tables
        tables = con.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'main' 
            AND table_name NOT IN ('customers', 'orders', 'order_items')
        """).fetchall()
        
        if tables:
            print("Found metric tables to remove:")
            for table in tables:
                table_name = table[0]
                con.execute(f"DROP TABLE IF EXISTS {table_name}")
                print(f"- Dropped table: {table_name}")
        else:
            print("No metric tables found to remove")
            
        # Verify cleanup
        remaining_tables = con.execute("SHOW TABLES").fetchall()
        print("\nRemaining tables:")
        for table in remaining_tables:
            print(f"- {table[0]}")
            
    except Exception as e:
        print(f"Error during cleanup: {e}")
        return False
    
    con.close()
    print("Database cleanup completed")
    return True

def reset_database():
    """Completely reset database - remove all tables including source data"""
    
    confirm = input("This will remove ALL tables including source data. Continue? (y/n): ")
    if confirm.lower() != 'y':
        print("Cleanup cancelled")
        return False
    
    con = duckdb.connect('metrics.duckdb')
    print("Connected to DuckDB")
    
    try:
        # Get all tables
        tables = con.execute("SHOW TABLES").fetchall()
        
        if tables:
            print("Dropping all tables:")
            for table in tables:
                table_name = table[0]
                con.execute(f"DROP TABLE IF EXISTS {table_name}")
                print(f"- Dropped table: {table_name}")
        else:
            print("No tables found")
            
    except Exception as e:
        print(f"Error during reset: {e}")
        return False
    
    con.close()
    print("Database reset completed")
    return True

if __name__ == "__main__":
    print("Choose cleanup option:")
    print("1 - Remove only metric tables (keep source data)")
    print("2 - Remove all tables (full reset)")
    
    choice = input("Enter choice (1 or 2): ")
    
    if choice == "1":
        cleanup_database()
    elif choice == "2":
        reset_database()
    else:
        print("Invalid choice")