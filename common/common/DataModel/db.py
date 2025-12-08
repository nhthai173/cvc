from typing import Any, Optional, Protocol, Sequence

import os
import re
import sqlite3
import logging

import psycopg2
from psycopg2 import pool, sql
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
import threading

from ..config.logger import get_db_logger
logger = get_db_logger()


class DatabaseClient(Protocol):
    """Protocol describing the database client interface used in the project."""

    debug: bool
    connection: Any
    cursor: Any

    def connect(self) -> None: ...

    def execute_query(self, query: str, params: Optional[Sequence[Any]] = None) -> list[dict]: ...

    def execute_non_query(self, query: str, params: Optional[Sequence[Any]] = None) -> None: ...

    def execute_non_query_returning(self, query: str, params: Optional[Sequence[Any]] = None) -> Any: ...

    def close(self) -> None: ...

class PostgresDB:
    _pools = {}  # Dict of pools keyed by connection string
    _pool_lock = threading.Lock()
    _instances = {}  # Dict of instances keyed by connection string
    _instance_lock = threading.Lock()
    
    @classmethod
    def _make_connection_key(cls, host: str, database: str, user: str, port: int) -> str:
        """Create a unique key for connection parameters."""
        return f"{host}:{port}/{database}@{user}"
    
    def __new__(cls, host: str = None, database: str = None, user: str = None, password: str = None, port: int = 5432, debug: bool = False, logger_instance: logging.Logger = None, minconn: int = None, maxconn: int = None, force_new: bool = False):
        """
        Singleton pattern per connection parameters.
        Returns the same instance for same connection parameters unless force_new=True.
        
        Args:
            force_new: If True, always create a new instance (useful for multi-processing)
        """
        # Load default config if needed to generate connection key
        _host = host
        _database = database
        _user = user
        _port = port
        
        if not _host or not _database or not _user:
            try:
                from ..config import settings as cfg
                _host = _host or cfg.db.host
                _database = _database or cfg.db.name
                _user = _user or cfg.db.user
                _port = _port or cfg.db.port
            except ImportError:
                _host = _host or os.getenv('DB_HOST', 'localhost')
                _database = _database or os.getenv('DB_NAME', 'cipdb')
                _user = _user or os.getenv('DB_USER', 'cipuser')
                _port = _port or int(os.getenv('DB_PORT', '5432'))
        
        # Create connection key
        conn_key = cls._make_connection_key(_host, _database, _user, _port)
        
        # Force new instance
        if force_new:
            instance = super().__new__(cls)
            instance._initialized = False
            instance._connection_key = conn_key
            return instance
        
        # Return existing instance or create new one
        if conn_key not in cls._instances:
            with cls._instance_lock:
                if conn_key not in cls._instances:
                    instance = super().__new__(cls)
                    instance._initialized = False
                    instance._connection_key = conn_key
                    cls._instances[conn_key] = instance
        
        return cls._instances[conn_key]
    
    def __init__(self, host: str = None, database: str = None, user: str = None, password: str = None, port: int = 5432, debug: bool = False, logger_instance: logging.Logger = None, minconn: int = None, maxconn: int = None, force_new: bool = False):
        """
        Initialize PostgresDB instance.
        
        Args:
            host: Database host
            database: Database name
            user: Database user
            password: Database password
            port: Database port
            debug: Enable debug logging
            logger_instance: Custom logger instance
            minconn: Minimum connections in pool
            maxconn: Maximum connections in pool
            force_new: Force create new instance (useful for multi-processing)
        """
        # Skip re-initialization if already initialized (singleton pattern)
        if self._initialized:
            return
        
        # Load from config if parameters not provided
        try:
            from ..config import settings as cfg
            if not host and not database and not user and not password:
                host = cfg.db.host
                database = cfg.db.name
                user = cfg.db.user
                password = cfg.db.password
                port = cfg.db.port
            if minconn is None:
                minconn = cfg.db.pool_min
            if maxconn is None:
                maxconn = cfg.db.pool_max
        except ImportError:
            # Fallback to environment variables
            if not host and not database and not user and not password:
                host = os.getenv('DB_HOST', 'localhost')
                database = os.getenv('DB_NAME', 'cipdb')
                user = os.getenv('DB_USER', 'cipuser')
                password = os.getenv('DB_PASSWORD', '')
                port = int(os.getenv('DB_PORT', '5432'))
            if minconn is None:
                minconn = int(os.getenv('DB_POOL_MIN', '1'))
            if maxconn is None:
                maxconn = int(os.getenv('DB_POOL_MAX', '10'))
        self.debug = debug
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.minconn = minconn
        self.maxconn = maxconn
        self.connection = None
        self.cursor = None
        self.logger = logger_instance if logger_instance else logger
        self._local_connection = False  # Track if connection is from pool
        self._initialized = True

    def _log(self, msg: str, level: str = 'info'):
        if self.debug:
            if level == 'info':
                self.logger.info(msg)
            elif level == 'warning':
                self.logger.warning(msg)
            elif level == 'error':
                self.logger.error(msg)
            elif level == 'debug':
                self.logger.debug(msg)

    def _get_pool(self):
        """Get or create connection pool for this connection (thread-safe per connection key)."""
        if self._connection_key not in PostgresDB._pools:
            with PostgresDB._pool_lock:
                if self._connection_key not in PostgresDB._pools:
                    try:
                        PostgresDB._pools[self._connection_key] = pool.ThreadedConnectionPool(
                            self.minconn,
                            self.maxconn,
                            host=self.host,
                            database=self.database,
                            user=self.user,
                            password=self.password,
                            port=self.port
                        )
                        self._log("\n" + "="*60 + "\n🏊 PostgreSQL Connection Pool Created\n" + f"   Connection: {self._connection_key}\n" + f"   Min connections: {self.minconn}\n" + f"   Max connections: {self.maxconn}\n" + "="*60)
                    except Exception as e:
                        raise Exception(f"Error creating connection pool: {e}")
        return PostgresDB._pools[self._connection_key]
    
    def connect(self) -> None:
        """Get a connection from the pool."""
        try:
            pool_instance = self._get_pool()
            self.connection = pool_instance.getconn()
            self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            self._local_connection = True
            self._log("\n" + "="*60 + "\n🔗 Connection acquired from pool\n" + "="*60)
        except Exception as e:
            raise Exception(f"Error getting connection from pool: {e}")
    
    @contextmanager
    def get_connection(self):
        """Context manager for automatic connection management."""
        conn = None
        cursor = None
        try:
            pool_instance = self._get_pool()
            conn = pool_instance.getconn()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            self._log("🔗 Connection acquired from pool (context manager)")
            yield conn, cursor
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if cursor:
                cursor.close()
            if conn:
                pool_instance.putconn(conn)
                self._log("🔄 Connection returned to pool")
    
    def _execute_with_connection(self, query: str, params: Optional[Sequence[Any]], 
                                  auto_connection: bool, operation_type: str, 
                                  fetch_result: bool = False, fetch_returning: bool = False):
        """Internal method to handle query execution with auto/manual connection.
        
        Args:
            query: SQL query string
            params: Query parameters
            auto_connection: If True, use auto connection from pool
            operation_type: Type of operation for logging (e.g., 'Query', 'Non-Query')
            fetch_result: If True, fetch all results
            fetch_returning: If True, fetch one row (for RETURNING clause)
        
        Returns:
            Query results, returning ID, or None depending on operation type
        """
        icon_map = {
            'Query': '📋',
            'Non-Query': '✏️',
            'Non-Query (Returning)': '🔄'
        }
        icon = icon_map.get(operation_type, '📋')
        
        # Auto connection mode
        if auto_connection and not self.connection:
            with self.get_connection() as (conn, cursor):
                self._log(f"\n{icon} Executing {operation_type} (auto):\n   SQL: {query}\n   Params: {params}")
                cursor.execute(query, params)
                
                if fetch_result:
                    results = cursor.fetchall()
                    self._log(f"✅ {operation_type} Success → {len(results)} rows fetched")
                    return results
                elif fetch_returning:
                    returning_id = cursor.fetchone()
                    self._log(f"✅ {operation_type} executed successfully → Returning ID: {returning_id}")
                    return returning_id
                else:
                    self._log(f"✅ {operation_type} executed successfully")
                    try:
                        return cursor.rowcount
                    except Exception:
                        return None
        
        # Manual connection mode
        if not self.connection:
            raise Exception("Not connected to database. Call connect() first or use auto_connection=True.")
        
        try:
            self._log(f"\n{icon} Executing {operation_type}:\n   SQL: {query}\n   Params: {params}")
            self.cursor.execute(query, params)
            
            if fetch_result:
                results = self.cursor.fetchall()
                self._log(f"✅ {operation_type} Success → {len(results)} rows fetched")
                return results
            elif fetch_returning:
                returning_id = self.cursor.fetchone()
                self.connection.commit()
                self._log(f"✅ {operation_type} executed successfully → Returning ID: {returning_id}")
                return returning_id
            else:
                self.connection.commit()
                self._log(f"✅ {operation_type} executed successfully")
                return None
        except Exception as e:
            if self.connection:
                self.connection.rollback()
            raise Exception(f"Error executing query: {e}")

    def execute_query(self, query: str, params: Optional[Sequence[Any]] = None, auto_connection: bool = True):
        """Execute a SELECT query and return results.
        
        Args:
            query: SQL query string
            params: Query parameters
            auto_connection: If True, automatically get/return connection from pool.
                           If False, requires manual connect() before calling.
        """
        return self._execute_with_connection(query, params, auto_connection, 'Query', fetch_result=True)

    def execute_non_query(self, query: str, params: Optional[Sequence[Any]] = None, auto_connection: bool = True) -> int|None:
        """Execute INSERT, UPDATE, DELETE queries.
        
        Args:
            query: SQL query string
            params: Query parameters
            auto_connection: If True, automatically get/return connection from pool.
                           If False, requires manual connect() before calling.
        """
        self._execute_with_connection(query, params, auto_connection, 'Non-Query')

    def execute_non_query_returning(self, query: str, params: Optional[Sequence[Any]] = None, auto_connection: bool = True):
        """Execute INSERT query and return the generated ID.
        
        Args:
            query: SQL query string
            params: Query parameters
            auto_connection: If True, automatically get/return connection from pool.
                           If False, requires manual connect() before calling.
        """
        return self._execute_with_connection(query, params, auto_connection, 'Non-Query (Returning)', fetch_returning=True)

    def close(self) -> None:
        """Return connection to the pool."""
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.connection and self._local_connection:
            pool_instance = self._get_pool()
            pool_instance.putconn(self.connection)
            self.connection = None
            self._local_connection = False
            self._log("\n" + "="*60 + "\n� Connection returned to pool\n" + "="*60)
    
    @classmethod
    def close_all_connections(cls, connection_key: str = None):
        """
        Close connections in the pool.
        
        Args:
            connection_key: If provided, close only this connection's pool.
                          If None, close all pools (call when shutting down app).
        """
        if connection_key:
            # Close specific pool
            if connection_key in cls._pools:
                with cls._pool_lock:
                    if connection_key in cls._pools:
                        cls._pools[connection_key].closeall()
                        del cls._pools[connection_key]
                        logger.info(f"- Postgres pool for '{connection_key}' closed")
                        
                # Remove instance if exists
                if connection_key in cls._instances:
                    with cls._instance_lock:
                        if connection_key in cls._instances:
                            del cls._instances[connection_key]
        else:
            # Close all pools
            if cls._pools:
                with cls._pool_lock:
                    for key, pool_instance in cls._pools.items():
                        pool_instance.closeall()
                        logger.info(f"- Postgres pool for '{key}' closed")
                    cls._pools.clear()
                
                # Clear all instances
                with cls._instance_lock:
                    cls._instances.clear()
                    
                logger.info("- All Postgres pools closed")
    
    @classmethod
    def get_instance_info(cls) -> dict:
        """
        Get information about all active instances and pools.
        
        Returns:
            Dict with instance and pool information
        """
        return {
            'instances': list(cls._instances.keys()),
            'pools': list(cls._pools.keys()),
            'instance_count': len(cls._instances),
            'pool_count': len(cls._pools)
        }


class SQLiteDB:
    _pools = {}  # One pool per database file
    _pool_lock = threading.Lock()
    
    def __init__(self, database_path: str = "", debug: bool = False, logger_instance: logging.Logger = None, maxconn: int = None):
        # Load from config if not provided
        try:
            from ..config import settings as cfg
            if not database_path:
                database_path = cfg.sqlite.db_path
            if maxconn is None:
                maxconn = cfg.sqlite.pool_max
        except ImportError:
            # Fallback to environment variables
            if not database_path:
                database_path = os.getenv('SQLITE_DB_PATH', './data/cip_debug.db')
            if maxconn is None:
                maxconn = int(os.getenv('SQLITE_POOL_MAX', '5'))
        self.debug = debug
        self.database_path = database_path
        self.maxconn = maxconn
        self.connection: Optional[sqlite3.Connection] = None
        self.cursor: Optional[sqlite3.Cursor] = None
        self.logger = logger_instance if logger_instance else logger
        self._local_connection = False

    def _log(self, msg: str, level: str = 'info'):
        if self.debug:
            if level == 'info':
                self.logger.info(msg)
            elif level == 'warning':
                self.logger.warning(msg)
            elif level == 'error':
                self.logger.error(msg)
            elif level == 'debug':
                self.logger.debug(msg)
    
    def _prepare_params(self, params: Optional[Sequence[Any]]) -> tuple:
        """Convert datetime objects to ISO format strings for SQLite compatibility."""
        if params is None:
            return ()
        
        from datetime import datetime, date
        converted_params = []
        for param in params:
            if isinstance(param, datetime):
                # Convert datetime to ISO format string with microseconds
                converted_params.append(param.strftime('%Y-%m-%d %H:%M:%S.%f'))
            elif isinstance(param, date):
                # Convert date to ISO format string
                converted_params.append(param.strftime('%Y-%m-%d'))
            else:
                converted_params.append(param)
        
        return tuple(converted_params)

    def _prepare_query(self, query: str) -> str:
        query = query.replace('%s', '?')
        
        # Convert PostgreSQL data types to SQLite compatible types
        # TIMESTAMP -> TEXT (SQLite stores dates as TEXT, REAL, or INTEGER)
        query = re.sub(r'\bTIMESTAMP\b', 'TEXT', query, flags=re.IGNORECASE)
        query = re.sub(r'\bTIMESTAMPTZ\b', 'TEXT', query, flags=re.IGNORECASE)
        query = re.sub(r'\bTIMESTAMP\s+WITH\s+TIME\s+ZONE\b', 'TEXT', query, flags=re.IGNORECASE)
        query = re.sub(r'\bTIMESTAMP\s+WITHOUT\s+TIME\s+ZONE\b', 'TEXT', query, flags=re.IGNORECASE)
        
        # Add other common PostgreSQL to SQLite type conversions
        query = re.sub(r'\bSERIAL\b', 'INTEGER', query, flags=re.IGNORECASE)
        query = re.sub(r'\bBIGSERIAL\b', 'INTEGER', query, flags=re.IGNORECASE)
        query = re.sub(r'\bBOOLEAN\b', 'INTEGER', query, flags=re.IGNORECASE)
        
        # add quote `` for table name and column names
        # Replace table names that contain dots (e.g., public.run -> `public.run`)
        # But avoid matching TABLE in "CREATE TABLE" or "TABLE IF NOT EXISTS" contexts
        query = re.sub(r'\b(FROM|JOIN|INTO|UPDATE)\s+([\w\.]+)\b', r'\1 `\2`', query, flags=re.IGNORECASE)
        # Handle TABLE keyword separately to avoid breaking CREATE TABLE IF NOT EXISTS
        query = re.sub(r'\bTABLE\s+(?!IF\s+NOT\s+EXISTS\s+)([\w\.]+)\b', r'TABLE `\1`', query, flags=re.IGNORECASE)
        # Handle TABLE IF NOT EXISTS specifically
        query = re.sub(r'\bTABLE\s+IF\s+NOT\s+EXISTS\s+([\w\.]+)\b', r'TABLE IF NOT EXISTS `\1`', query, flags=re.IGNORECASE)
        
        return query

    def _strip_returning_clause(self, query: str) -> str:
        match = re.search(r"RETURNING\s+.+", query, flags=re.IGNORECASE | re.DOTALL)
        if match:
            stripped = query[:match.start()]
        else:
            stripped = query
        return stripped.rstrip().rstrip(';')

    def _get_pool(self):
        """Get or create connection pool for this database file."""
        if self.database_path not in SQLiteDB._pools:
            with SQLiteDB._pool_lock:
                if self.database_path not in SQLiteDB._pools:
                    SQLiteDB._pools[self.database_path] = {
                        'available': [],
                        'in_use': [],
                        'maxconn': self.maxconn
                    }
                    self._log(f"\n" + "="*60 + f"\n🏊 SQLite Connection Pool Created\n   Database: {self.database_path}\n   Max connections: {self.maxconn}\n" + "="*60)
        return SQLiteDB._pools[self.database_path]
    
    def _create_connection(self):
        """Create a new SQLite connection."""
        conn = sqlite3.connect(
            self.database_path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            check_same_thread=False  # Allow connections to be used across threads
        )
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA foreign_keys = ON;')
        return conn
    
    def connect(self) -> None:
        """Get a connection from the pool."""
        try:
            pool_dict = self._get_pool()
            with SQLiteDB._pool_lock:
                # Try to get an available connection
                if pool_dict['available']:
                    self.connection = pool_dict['available'].pop()
                    pool_dict['in_use'].append(self.connection)
                    self._log("🔗 Connection reused from pool")
                # Create new connection if under limit
                elif len(pool_dict['in_use']) < pool_dict['maxconn']:
                    self.connection = self._create_connection()
                    pool_dict['in_use'].append(self.connection)
                    self._log(f"🔗 New connection created (total in use: {len(pool_dict['in_use'])})")
                else:
                    raise Exception(f"Connection pool exhausted (max: {pool_dict['maxconn']})")
            
            self.cursor = self.connection.cursor()
            self._local_connection = True
            self._log(f"\n" + "="*60 + f"\n🔗 Connected to SQLite database\n   Path: {self.database_path}\n" + "="*60)
        except Exception as e:
            raise Exception(f"Error getting connection from pool: {e}") from e
    
    @contextmanager
    def get_connection(self):
        """Context manager for automatic connection management."""
        conn = None
        cursor = None
        pool_dict = self._get_pool()
        try:
            with SQLiteDB._pool_lock:
                if pool_dict['available']:
                    conn = pool_dict['available'].pop()
                    pool_dict['in_use'].append(conn)
                elif len(pool_dict['in_use']) < pool_dict['maxconn']:
                    conn = self._create_connection()
                    pool_dict['in_use'].append(conn)
                else:
                    raise Exception(f"Connection pool exhausted (max: {pool_dict['maxconn']})")
            
            cursor = conn.cursor()
            self._log("🔗 Connection acquired from pool (context manager)")
            yield conn, cursor
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if cursor:
                cursor.close()
            if conn:
                with SQLiteDB._pool_lock:
                    if conn in pool_dict['in_use']:
                        pool_dict['in_use'].remove(conn)
                        pool_dict['available'].append(conn)
                self._log("🔄 Connection returned to pool")
    
    def _execute_with_connection(self, query: str, params: Optional[Sequence[Any]], 
                                  auto_connection: bool, operation_type: str, 
                                  fetch_result: bool = False, fetch_returning: bool = False):
        """Internal method to handle query execution with auto/manual connection.
        
        Args:
            query: SQL query string
            params: Query parameters
            auto_connection: If True, use auto connection from pool
            operation_type: Type of operation for logging
            fetch_result: If True, fetch all results
            fetch_returning: If True, fetch one row (for RETURNING clause)
        
        Returns:
            Query results, returning ID, or None depending on operation type
        """
        prepared_query = self._prepare_query(query)
        prepared_params = self._prepare_params(params)
        returning_expected = 'RETURNING' in prepared_query.upper() if fetch_returning else False
        
        icon_map = {
            'Query': '📋',
            'Non-Query': '✏️',
            'Non-Query (Returning)': '🔄'
        }
        icon = icon_map.get(operation_type, '📋')
        
        # Auto connection mode
        if auto_connection and not self.connection:
            with self.get_connection() as (conn, cursor):
                self._log(f"\n{icon} Executing {operation_type} (auto):\n   SQL: {prepared_query}\n   Params: {prepared_params}")
                
                # Handle RETURNING clause for SQLite
                if fetch_returning:
                    try:
                        cursor.execute(prepared_query, prepared_params)
                    except sqlite3.OperationalError as e:
                        if returning_expected:
                            cleaned_query = self._strip_returning_clause(prepared_query).rstrip().rstrip(';')
                            self._log(f"⚠️  Retrying without RETURNING clause:\n   SQL: {cleaned_query}\n   Params: {prepared_params}", 'warning')
                            cursor.execute(cleaned_query, prepared_params)
                            returning_expected = False
                        else:
                            raise Exception(f"Error executing query: {e}") from e
                    
                    try:
                        row = cursor.fetchone() if returning_expected else None
                    except sqlite3.ProgrammingError:
                        row = None
                    
                    if row is not None:
                        row_dict = dict(row)
                        result = row_dict.get('id', None)
                    else:
                        result = cursor.lastrowid
                    
                    self._log(f"✅ {operation_type} executed successfully → Returning ID: {result}")
                    return result
                else:
                    cursor.execute(prepared_query, prepared_params)
                    
                    if fetch_result:
                        rows = cursor.fetchall()
                        results = [dict(row) for row in rows]
                        self._log(f"✅ {operation_type} Success → {len(results)} rows fetched")
                        return results
                    else:
                        self._log(f"✅ {operation_type} executed successfully")
                        return None
        
        # Manual connection mode
        if not self.connection or not self.cursor:
            raise Exception("Not connected to database. Call connect() first or use auto_connection=True.")
        
        try:
            self._log(f"\n{icon} Executing {operation_type}:\n   SQL: {prepared_query}\n   Params: {prepared_params}")
            
            # Handle RETURNING clause for SQLite
            if fetch_returning:
                try:
                    self.cursor.execute(prepared_query, prepared_params)
                except sqlite3.OperationalError as e:
                    self.connection.rollback()
                    if returning_expected:
                        cleaned_query = self._strip_returning_clause(prepared_query).rstrip().rstrip(';')
                        self._log(f"⚠️  Retrying without RETURNING clause:\n   SQL: {cleaned_query}\n   Params: {prepared_params}", 'warning')
                        self.cursor.execute(cleaned_query, prepared_params)
                        returning_expected = False
                    else:
                        raise Exception(f"Error executing query: {e}") from e
                
                try:
                    row = self.cursor.fetchone() if returning_expected else None
                except sqlite3.ProgrammingError:
                    row = None
                
                self.connection.commit()
                
                if row is not None:
                    row_dict = dict(row)
                    result = row_dict.get('id', None)
                else:
                    result = self.cursor.lastrowid
                
                self._log(f"✅ {operation_type} executed successfully → Returning ID: {result}")
                return result
            else:
                self.cursor.execute(prepared_query, prepared_params)
                
                if fetch_result:
                    rows = self.cursor.fetchall()
                    results = [dict(row) for row in rows]
                    self._log(f"✅ {operation_type} Success → {len(results)} rows fetched")
                    return results
                else:
                    self.connection.commit()
                    self._log(f"✅ {operation_type} executed successfully")
                    return None
        except Exception as e:
            if self.connection:
                self.connection.rollback()
            raise Exception(f"Error executing query: {e}") from e

    def execute_query(self, query: str, params: Optional[Sequence[Any]] = None, auto_connection: bool = True):
        """Execute a SELECT query and return results.
        
        Args:
            query: SQL query string
            params: Query parameters
            auto_connection: If True, automatically get/return connection from pool.
                           If False, requires manual connect() before calling.
        """
        return self._execute_with_connection(query, params, auto_connection, 'Query', fetch_result=True)

    def execute_non_query(self, query: str, params: Optional[Sequence[Any]] = None, auto_connection: bool = True) -> None:
        """Execute INSERT, UPDATE, DELETE queries.
        
        Args:
            query: SQL query string
            params: Query parameters
            auto_connection: If True, automatically get/return connection from pool.
                           If False, requires manual connect() before calling.
        """
        self._execute_with_connection(query, params, auto_connection, 'Non-Query')

    def execute_non_query_returning(self, query: str, params: Optional[Sequence[Any]] = None, auto_connection: bool = True):
        """Execute INSERT query and return the generated ID.
        
        Args:
            query: SQL query string
            params: Query parameters
            auto_connection: If True, automatically get/return connection from pool.
                           If False, requires manual connect() before calling.
        """
        return self._execute_with_connection(query, params, auto_connection, 'Non-Query (Returning)', fetch_returning=True)

    def close(self) -> None:
        """Return connection to the pool."""
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.connection and self._local_connection:
            pool_dict = self._get_pool()
            with SQLiteDB._pool_lock:
                if self.connection in pool_dict['in_use']:
                    pool_dict['in_use'].remove(self.connection)
                    pool_dict['available'].append(self.connection)
            self.connection = None
            self._local_connection = False
            self._log("\n" + "="*60 + "\n� Connection returned to pool\n" + "="*60)
    
    @classmethod
    def close_all_connections(cls):
        """Close all connections in all pools (call when shutting down app)."""
        with cls._pool_lock:
            for db_path, pool_dict in cls._pools.items():
                # Close all connections
                for conn in pool_dict['available'] + pool_dict['in_use']:
                    conn.close()
                pool_dict['available'].clear()
                pool_dict['in_use'].clear()
            cls._pools.clear()
            logger.info("- All SQLite pool connections closed")

if __name__ == "__main__":
    db = SQLiteDB(database_path='./data/cip_debug.db', debug=True)
    try:
        # db.execute_non_query("""CREATE TABLE `public.run` (
        #                            `id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        #                            `start_ts` TEXT,
        #                            `end_ts` TEXT, `recipe_number` INTEGER,
        #                            `recipe_name` TEXT,
        #                            `total_step` INTEGER,
        #                            `pause_count` INTEGER,
        #                            `manual_involved` INTEGER,
        #                            `duration` INTEGER)""")
        # db.execute_non_query("""CREATE TABLE `public.run_step` (
        #                      `id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        #                      `run_id` INTEGER REFERENCES `public.run`(`id`),
        #                      `type` TEXT,
        #                      `step` INTEGER,
        #                      `material` TEXT,
        #                      `start_ts` TEXT,
        #                      `end_ts` TEXT,
        #                      `start_second` INTEGER,
        #                      `end_second` INTEGER)
        #                      """)
        # db.execute_non_query("""CREATE TABLE IF NOT EXISTS `public.run_trend` (
        #                         `id` INTEGER PRIMARY KEY AUTOINCREMENT,
        #                         `run_id` INTEGER REFERENCES `public.run`(`id`),
        #                         `temp_sensor_01` REAL,
        #                         `temp_sensor_02` REAL,
        #                         `temp_sensor_03` REAL,
        #                         `pressure_sensor` REAL,
        #                         `conductivity_sensor` REAL,
        #                         `ph_sensor` REAL,
        #                         `pump_speed` REAL,
        #                         `measured_at` TEXT
        #                     )""")
        # db.execute_non_query("""CREATE TABLE IF NOT EXISTS `public.run_event` (
        #                         `id` INTEGER PRIMARY KEY AUTOINCREMENT,
        #                         `run_id` INTEGER REFERENCES `public.run`(`id`),
        #                         `type` TEXT,
        #                         `param` TEXT,
        #                         `value_raw` TEXT,
        #                         `description` TEXT,
        #                         `updated_at` TEXT
        #                     )""")
        db.execute_non_query("""CREATE TABLE IF NOT EXISTS `public.run_setpoint` (
                                `id` INTEGER PRIMARY KEY AUTOINCREMENT,
                                `run_id` INTEGER REFERENCES `public.run`(`id`),
                                `step` INTEGER,
                                `param` TEXT,
                                `value` TEXT,
                                `updated_at` TEXT
                            )""")
        # results = db.execute_query("SELECT * FROM public.run_step")
        # for row in results:
        #     print(row)
    finally:
        db.close()
