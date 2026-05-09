import os
import sys
from sqlalchemy import create_engine, text
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

def local_socket_param(host):
    socket_path = os.getenv("PATENTS_DB_SOCKET", "/run/mysqld/mysqld.sock")
    if host in ("localhost", "127.0.0.1") and Path(socket_path).exists():
        return f"?unix_socket={socket_path}"
    return ""


def main():
    host = os.getenv("PATENTS_DB_HOST", "localhost")
    user = os.getenv("PATENTS_DB_USER", "root")
    password = os.getenv("PATENTS_DB_PASSWORD", "")
    db_name = os.getenv("PATENTS_DB_NAME", "patents_db")
    
    print(f"Connecting to MySQL server at {host}...")
    try:
        # Connect without DB to create it
        socket_param = local_socket_param(host)
        engine_no_db = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{socket_param}")
        with engine_no_db.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name}"))
        
        print(f"Database '{db_name}' ready.")
        
        # Connect to the DB to run schema.sql
        engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{db_name}{socket_param}")
        
        schema_path = BASE_DIR / "schema.sql"
        if schema_path.exists():
            with open(schema_path, "r", encoding="utf-8") as f:
                sql_content = f.read()
            
            # Split by semicolon, but carefully handle comments
            sql_statements = []
            current_stmt = ""
            for line in sql_content.split('\n'):
                stripped = line.strip()
                # Skip empty lines and comments
                if not stripped or stripped.startswith('--'):
                    continue
                current_stmt += " " + line
                if ';' in line:
                    # Found end of statement
                    stmt = current_stmt.strip()
                    if stmt:
                        sql_statements.append(stmt)
                    current_stmt = ""
            
            # Execute all statements in a single connection
            with engine.begin() as conn:
                for i, statement in enumerate(sql_statements):
                    if statement.strip():
                        try:
                            conn.execute(text(statement.strip()))
                            print(f"  [{i+1}/{len(sql_statements)}] Executed statement")
                        except Exception as stmt_error:
                            print(f"  Warning: Statement {i+1} failed: {stmt_error}")
                            # Continue with next statement rather than aborting
                            # This allows SET statements to fail gracefully if not supported
            
            print("Schema loaded successfully via SQLAlchemy.")
        else:
            print("schema.sql not found.")
    except Exception as e:
        print(f"Failed to create database or tables: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()