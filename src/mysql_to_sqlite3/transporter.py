"""Enhanced MySQL to SQLite transporter with stall detection and resume capability."""

import logging
import os
import re
import sqlite3
import typing as t
import fnmatch
import time
import json
from contextlib import contextmanager
from datetime import timedelta, datetime
from decimal import Decimal
from math import ceil
from os.path import realpath, exists
from sys import stdout
from typing import Iterator, List, Tuple, Any

import mysql.connector
import typing_extensions as tx
from mysql.connector import CharacterSet, errorcode
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.types import RowItemType
from tqdm import tqdm, trange

from mysql_to_sqlite3.mysql_utils import CHARSET_INTRODUCERS, get_mysql_tables
from mysql_to_sqlite3.sqlite_utils import (
    CollatingSequences,
    Integer_Types,
    adapt_decimal,
    adapt_timedelta,
    convert_date,
    convert_decimal,
    convert_timedelta,
    encode_data_for_sqlite,
)
from mysql_to_sqlite3.types import MySQLtoSQLiteAttributes, MySQLtoSQLiteParams


class MySQLtoSQLite(MySQLtoSQLiteAttributes):
    """Use this class to transfer a MySQL database to SQLite."""

    COLUMN_PATTERN: t.Pattern[str] = re.compile(r"^[^(]+")
    COLUMN_LENGTH_PATTERN: t.Pattern[str] = re.compile(r"\(\d+\)$")

    def __init__(self, **kwargs: tx.Unpack[MySQLtoSQLiteParams]) -> None:
        """Constructor."""
        if kwargs.get("mysql_database") is not None:
            self._mysql_database = str(kwargs.get("mysql_database"))
        else:
            raise ValueError("Please provide a MySQL database")

        if kwargs.get("mysql_user") is not None:
            self._mysql_user = str(kwargs.get("mysql_user"))
        else:
            raise ValueError("Please provide a MySQL user")

        if kwargs.get("sqlite_file") is None:
            raise ValueError("Please provide an SQLite file")
        else:
            self._sqlite_file = realpath(str(kwargs.get("sqlite_file")))

        password: t.Optional[t.Union[str, bool]] = kwargs.get("mysql_password")
        self._mysql_password = password if isinstance(password, str) else None

        self._mysql_host = kwargs.get("mysql_host", "localhost") or "localhost"

        self._mysql_port = kwargs.get("mysql_port", 3306) or 3306

        self._mysql_charset = kwargs.get("mysql_charset", "utf8mb4") or "utf8mb4"

        self._mysql_collation = (
            kwargs.get("mysql_collation") or CharacterSet().get_default_collation(self._mysql_charset.lower())[0]
        )
        if not kwargs.get("mysql_collation") and self._mysql_collation == "utf8mb4_0900_ai_ci":
            self._mysql_collation = "utf8mb4_unicode_ci"

        self._mysql_tables = kwargs.get("mysql_tables") or tuple()

        self._exclude_mysql_tables = kwargs.get("exclude_mysql_tables") or tuple()

        if bool(self._mysql_tables) and bool(self._exclude_mysql_tables):
            raise ValueError("mysql_tables and exclude_mysql_tables are mutually exclusive")

        self._limit_rows = kwargs.get("limit_rows", 0) or 0

        self._min_rows_to_export = kwargs.get("min_rows_to_export", 0) or 0

        if kwargs.get("collation") is not None and str(kwargs.get("collation")).upper() in {
            CollatingSequences.BINARY,
            CollatingSequences.NOCASE,
            CollatingSequences.RTRIM,
        }:
            self._collation = str(kwargs.get("collation")).upper()
        else:
            self._collation = CollatingSequences.BINARY

        self._prefix_indices = kwargs.get("prefix_indices", False) or False

        if bool(self._mysql_tables) or bool(self._exclude_mysql_tables):
            self._without_foreign_keys = True
        else:
            self._without_foreign_keys = bool(kwargs.get("without_foreign_keys", False))

        self._without_data = bool(kwargs.get("without_data", False))
        self._without_tables = bool(kwargs.get("without_tables", False))

        if self._without_tables and self._without_data:
            raise ValueError("Unable to continue without transferring data or creating tables!")

        self._mysql_ssl_disabled = bool(kwargs.get("mysql_ssl_disabled", False))

        self._current_chunk_number = 0

        self._chunk_size = kwargs.get("chunk") or None

        self._buffered = bool(kwargs.get("buffered", False))

        self._vacuum = bool(kwargs.get("vacuum", False))

        self._skip_existing_tables = bool(kwargs.get("skip_existing_tables", False))

        self._quiet = bool(kwargs.get("quiet", False))

        self._logger = self._setup_logger(log_file=kwargs.get("log_file") or None, quiet=self._quiet)

        # BLOB optimization flag
        self._optimize_for_blobs = bool(kwargs.get("optimize_for_blobs", False))

        # Resume support
        self._resume_file = f"{self._sqlite_file}.resume"
        self._resume_state = self._load_resume_state()
        
        # Stall detection
        self._stall_timeout = 300  # 5 minutes
        self._last_progress_time = time.time()
        self._chunk_times = []  # Track chunk processing times
        
        # MySQL connection configuration for large transfers
        self._mysql_connection_config = {
            'user': self._mysql_user,
            'password': self._mysql_password,
            'host': self._mysql_host,
            'port': self._mysql_port,
            'database': self._mysql_database,
            'ssl_disabled': self._mysql_ssl_disabled,
            'charset': self._mysql_charset,
            'collation': self._mysql_collation,
            'autocommit': True,
            'connection_timeout': 30,  # Reduced to 30 seconds
            'use_unicode': True,
            'sql_mode': 'TRADITIONAL',
            'init_command': (
                "SET SESSION wait_timeout=3600, "  # Reduced to 1 hour
                "interactive_timeout=3600, "
                "net_read_timeout=300, "  # 5 minutes
                "net_write_timeout=300"
            )
        }

        sqlite3.register_adapter(Decimal, adapt_decimal)
        sqlite3.register_converter("DECIMAL", convert_decimal)
        sqlite3.register_adapter(timedelta, adapt_timedelta)
        sqlite3.register_converter("DATE", convert_date)
        sqlite3.register_converter("TIME", convert_timedelta)

        self._sqlite = sqlite3.connect(realpath(self._sqlite_file), detect_types=sqlite3.PARSE_DECLTYPES)
        self._sqlite.row_factory = sqlite3.Row

        # Apply BLOB optimizations if requested
        if self._optimize_for_blobs:
            self._logger.warning(
                "BLOB optimization mode enabled. WARNING: Database corruption may occur if process is interrupted!"
            )
            
            # Check if database is new/empty for page_size
            if not exists(self._sqlite_file) or os.path.getsize(self._sqlite_file) == 0:
                self._sqlite.execute("PRAGMA page_size = 32768")  # Must be before any tables
                self._logger.debug("Set SQLite page_size to 32KB for BLOB optimization")
            
            # These can be set anytime
            self._sqlite.execute("PRAGMA cache_size = -2000000")  # 2GB cache
            self._sqlite.execute("PRAGMA temp_store = MEMORY")
            self._sqlite.execute("PRAGMA mmap_size = 10737418240")  # 10GB memory map
            self._logger.info(
                "Applied BLOB optimizations: 2GB cache, memory temp store, 10GB mmap"
            )

        self._sqlite_cur = self._sqlite.cursor()

        self._json_as_text = bool(kwargs.get("json_as_text", False))

        self._sqlite_json1_extension_enabled = not self._json_as_text and self._check_sqlite_json1_extension_enabled()

        # Initial MySQL connection
        self._connect_to_mysql()

    def _load_resume_state(self) -> dict:
        """Load resume state from file."""
        if exists(self._resume_file):
            try:
                with open(self._resume_file, 'r') as f:
                    state = json.load(f)
                    self._logger.info(f"Loaded resume state from {self._resume_file}")
                    return state
            except Exception as e:
                self._logger.warning(f"Failed to load resume state: {e}")
        return {}

    def _save_resume_state(self, table_name: str, offset: int, total_records: int, last_key_values: t.Optional[t.List[t.Any]] = None) -> None:
        """Save resume state to file."""
        try:
            state = self._resume_state.copy()
            state[table_name] = {
                'offset': offset,
                'total_records': total_records,
                'timestamp': datetime.now().isoformat(),
                'last_key_values': last_key_values
            }
            
            def default_serializer(o: t.Any) -> t.Any:
                if isinstance(o, bytes):
                    try:
                        return o.decode('utf-8')
                    except UnicodeDecodeError:
                        return o.decode('latin1')  # Fallback for non-utf8 bytes
                if isinstance(o, (datetime, timedelta, Decimal)):
                    return str(o)
                raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

            with open(self._resume_file, 'w') as f:
                json.dump(state, f, indent=2, default=default_serializer)

            if last_key_values:
                self._logger.debug(f"Saved resume state: table={table_name}, offset={offset}, last_key={last_key_values}")
            else:
                self._logger.debug(f"Saved resume state: table={table_name}, offset={offset}")
        except Exception as e:
            self._logger.warning(f"Failed to save resume state: {e}")

    def _clear_resume_state(self, table_name: str) -> None:
        """Clear resume state for a completed table."""
        try:
            if table_name in self._resume_state:
                del self._resume_state[table_name]
                with open(self._resume_file, 'w') as f:
                    json.dump(self._resume_state, f, indent=2)
                self._logger.debug(f"Cleared resume state for table {table_name}")
        except Exception as e:
            self._logger.warning(f"Failed to clear resume state: {e}")

    def _check_stall(self, table_name: str, chunk_num: int) -> bool:
        """Check if transfer has stalled."""
        current_time = time.time()
        time_since_progress = current_time - self._last_progress_time
        
        if time_since_progress > self._stall_timeout:
            self._logger.warning(
                f"STALL DETECTED: No progress for {time_since_progress:.0f} seconds "
                f"on table '{table_name}', chunk {chunk_num}"
            )
            
            # Check if processing times are increasing exponentially
            if len(self._chunk_times) >= 3:
                recent_times = self._chunk_times[-3:]
                if recent_times[-1] > recent_times[-2] * 1.5 and recent_times[-2] > recent_times[-3] * 1.5:
                    self._logger.warning(
                        "Chunk processing times increasing exponentially: "
                        f"{recent_times[-3]:.1f}s -> {recent_times[-2]:.1f}s -> {recent_times[-1]:.1f}s"
                    )
                    return True
            
            return True
        return False

    def _connect_to_mysql(self, is_reconnect: bool = False) -> None:
        """Establish MySQL connection with proper configuration."""
        try:
            # Close existing connection if it exists
            if hasattr(self, '_mysql') and self._mysql:
                try:
                    self._mysql.close()
                except:
                    pass  # Ignore errors when closing broken connections
            
            _mysql_connection = mysql.connector.connect(**self._mysql_connection_config)
            if isinstance(_mysql_connection, MySQLConnectionAbstract):
                self._mysql = _mysql_connection
            else:
                raise ConnectionError("Unable to connect to MySQL")
            
            if not self._mysql.is_connected():
                raise ConnectionError("Unable to connect to MySQL")

            # Create cursors with appropriate settings - always create fresh cursors
            self._mysql_cur = self._mysql.cursor(buffered=False, raw=True)  # Unbuffered for large results
            self._mysql_cur_prepared = self._mysql.cursor(prepared=True)
            self._mysql_cur_dict = self._mysql.cursor(buffered=True, dictionary=True)  # Buffered for metadata
            
            # Log the current max_allowed_packet value for information
            try:
                self._mysql_cur_dict.execute("SHOW VARIABLES LIKE 'max_allowed_packet'")
                result = self._mysql_cur_dict.fetchone()
                if result:
                    max_packet_mb = int(result['Value']) / (1024 * 1024)
                    self._logger.info(f"MySQL max_allowed_packet: {max_packet_mb:.2f} MB")
                    if max_packet_mb < 64:
                        self._logger.warning(
                            f"MySQL max_allowed_packet is only {max_packet_mb:.2f} MB. "
                            "Consider asking your DBA to increase it for better performance with large data."
                        )
            except:
                pass  # Non-critical, just for information
            
            # Only log when it's an initial connection or a reconnection
            if is_reconnect:
                self._logger.info("MySQL connection re-established successfully")
            elif not hasattr(self, '_initial_connection_logged'):
                self._logger.info("MySQL connection established successfully")
                self._initial_connection_logged = True
            
        except mysql.connector.Error as err:
            self._logger.error("Failed to connect to MySQL: %s", err)
            raise

    @contextmanager
    def _mysql_connection_manager(self, cursor_type: str = 'raw') -> Iterator[Any]:
        """Context manager for MySQL operations with automatic reconnection."""
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                # Check connection health and handle various disconnection scenarios
                connection_lost = False
                try:
                    if not self._mysql.is_connected():
                        connection_lost = True
                    else:
                        # Additional check: try a simple ping (but don't log every ping)
                        self._mysql.ping(reconnect=False, attempts=1)
                except (mysql.connector.Error, mysql.connector.OperationalError, mysql.connector.InterfaceError):
                    connection_lost = True
                
                if connection_lost:
                    self._logger.warning("MySQL connection lost, reconnecting...")
                    self._connect_to_mysql(is_reconnect=True)
                
                # Get appropriate cursor - recreate cursors after reconnection
                if cursor_type == 'dict':
                    cursor = self._mysql_cur_dict
                elif cursor_type == 'prepared':
                    cursor = self._mysql_cur_prepared
                else:
                    cursor = self._mysql_cur
                
                yield cursor
                break  # Success, exit retry loop
                
            except (
                mysql.connector.Error, 
                mysql.connector.OperationalError, 
                mysql.connector.InterfaceError,
                mysql.connector.DatabaseError
            ) as err:
                # Check for specific connection reset errors
                connection_reset_errors = [
                    errorcode.CR_SERVER_LOST,
                    errorcode.CR_SERVER_GONE_ERROR,
                    errorcode.CR_CONNECTION_ERROR,
                    errorcode.ER_SERVER_SHUTDOWN,
                    2013,  # Lost connection to MySQL server during query
                    2006,  # MySQL server has gone away
                    2055,  # Lost connection to MySQL server at 'reading initial communication packet'
                ]
                
                is_connection_error = (
                    hasattr(err, 'errno') and err.errno in connection_reset_errors
                ) or (
                    "Lost connection" in str(err) or 
                    "MySQL server has gone away" in str(err) or
                    "Connection reset by peer" in str(err) or
                    "Broken pipe" in str(err) or
                    "Can't connect to MySQL server" in str(err)
                )
                
                if attempt < max_retries - 1:
                    if is_connection_error:
                        self._logger.warning(
                            "MySQL connection reset detected (attempt %d/%d): %s. Reconnecting in %d seconds...",
                            attempt + 1, max_retries, err, retry_delay
                        )
                    else:
                        self._logger.warning(
                            "MySQL operation failed (attempt %d/%d): %s. Retrying in %d seconds...",
                            attempt + 1, max_retries, err, retry_delay
                        )
                    
                    time.sleep(retry_delay)
                    
                    # Force reconnection for connection errors
                    try:
                        if hasattr(self, '_mysql') and self._mysql:
                            try:
                                self._mysql.close()
                            except:
                                pass  # Ignore errors when closing broken connection
                        self._connect_to_mysql(is_reconnect=True)
                    except Exception as reconnect_err:
                        self._logger.warning("Reconnection failed: %s", reconnect_err)
                        # Increase retry delay exponentially for failed reconnections
                        retry_delay = min(retry_delay * 2, 60)
                else:
                    self._logger.error("MySQL operation failed after %d attempts: %s", max_retries, err)
                    raise

    @classmethod
    def _setup_logger(
        cls, log_file: t.Optional[t.Union[str, "os.PathLike[t.Any]"]] = None, quiet: bool = False
    ) -> logging.Logger:
        formatter: logging.Formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        logger: logging.Logger = logging.getLogger(cls.__name__)
        logger.setLevel(logging.DEBUG)

        # Clear any existing handlers to prevent duplicates
        logger.handlers.clear()

        if not quiet:
            screen_handler = logging.StreamHandler(stream=stdout)
            screen_handler.setFormatter(formatter)
            logger.addHandler(screen_handler)

        if log_file:
            file_handler = logging.FileHandler(realpath(log_file), mode="w")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        # Prevent propagation to root logger to avoid duplicate messages
        logger.propagate = False

        return logger

    @classmethod
    def _valid_column_type(cls, column_type: str) -> t.Optional[t.Match[str]]:
        return cls.COLUMN_PATTERN.match(column_type.strip())

    @classmethod
    def _column_type_length(cls, column_type: str) -> str:
        suffix: t.Optional[t.Match[str]] = cls.COLUMN_LENGTH_PATTERN.search(column_type)
        if suffix:
            return suffix.group(0)
        return ""

    @staticmethod
    def _decode_column_type(column_type: t.Union[str, bytes]) -> str:
        if isinstance(column_type, str):
            return column_type
        if isinstance(column_type, bytes):
            try:
                return column_type.decode()
            except (UnicodeDecodeError, AttributeError):
                pass
        return str(column_type)

    @classmethod
    def _translate_type_from_mysql_to_sqlite(
        cls, column_type: t.Union[str, bytes], sqlite_json1_extension_enabled=False
    ) -> str:
        _column_type: str = cls._decode_column_type(column_type)

        # This could be optimized even further, however is seems adequate.
        match: t.Optional[t.Match[str]] = cls._valid_column_type(_column_type)
        if not match:
            raise ValueError(f'"{_column_type}" is not a valid column_type!')

        data_type: str = match.group(0).upper()

        if data_type.endswith(" UNSIGNED"):
            data_type = data_type.replace(" UNSIGNED", "")

        if data_type in {
            "BIGINT",
            "BLOB",
            "BOOLEAN",
            "DATE",
            "DATETIME",
            "DECIMAL",
            "DOUBLE",
            "FLOAT",
            "INTEGER",
            "MEDIUMINT",
            "NUMERIC",
            "REAL",
            "SMALLINT",
            "TIME",
            "TINYINT",
            "YEAR",
        }:
            return data_type
        if data_type in {
            "BIT",
            "BINARY",
            "LONGBLOB",
            "MEDIUMBLOB",
            "TINYBLOB",
            "VARBINARY",
        }:
            return "BLOB"
        if data_type in {"NCHAR", "NVARCHAR", "VARCHAR"}:
            return data_type + cls._column_type_length(_column_type)
        if data_type == "CHAR":
            return "CHARACTER" + cls._column_type_length(_column_type)
        if data_type == "INT":
            return "INTEGER"
        if data_type in "TIMESTAMP":
            return "DATETIME"
        if data_type == "JSON" and sqlite_json1_extension_enabled:
            return "JSON"
        return "TEXT"

    @classmethod
    def _translate_default_from_mysql_to_sqlite(
        cls,
        column_default: RowItemType = None,
        column_type: t.Optional[str] = None,
        column_extra: RowItemType = None,
    ) -> str:
        is_binary: bool
        is_hex: bool
        if isinstance(column_default, bytes):
            if column_type in {
                "BIT",
                "BINARY",
                "BLOB",
                "LONGBLOB",
                "MEDIUMBLOB",
                "TINYBLOB",
                "VARBINARY",
            }:
                if column_extra in {"DEFAULT_GENERATED", "default_generated"}:
                    for charset_introducer in CHARSET_INTRODUCERS:
                        if column_default.startswith(charset_introducer.encode()):
                            is_binary = False
                            is_hex = False
                            for b_prefix in ("B", "b"):
                                if column_default.startswith(rf"{charset_introducer} {b_prefix}\\'".encode()):
                                    is_binary = True
                                    break
                            for x_prefix in ("X", "x"):
                                if column_default.startswith(rf"{charset_introducer} {x_prefix}\\'".encode()):
                                    is_hex = True
                                    break
                            column_default = (
                                column_default.replace(charset_introducer.encode(), b"")
                                .replace(rb"x\\\'", b"")
                                .replace(rb"X\\\'", b"")
                                .replace(rb"b\\\'", b"")
                                .replace(rb"B\\\'", b"")
                                .replace(rb"\\\'", b"")
                                .replace(rb"'", b"")
                                .strip()
                            )
                            if is_binary:
                                return f"DEFAULT '{chr(int(column_default, 2))}'"
                            if is_hex:
                                return f"DEFAULT x'{column_default.decode()}'"
                            break
                return f"DEFAULT x'{column_default.hex()}'"
            try:
                column_default = column_default.decode()
            except (UnicodeDecodeError, AttributeError):
                pass
        if column_default is None:
            return ""
        if isinstance(column_default, bool):
            if column_type == "BOOLEAN" and sqlite3.sqlite_version >= "3.23.0":
                if column_default:
                    return "DEFAULT(TRUE)"
                return "DEFAULT(FALSE)"
            return f"DEFAULT '{int(column_default)}'"
        if isinstance(column_default, str):
            if column_default.lower() == "curtime()":
                return "DEFAULT CURRENT_TIME"
            if column_default.lower() == "curdate()":
                return "DEFAULT CURRENT_DATE"
            if column_default.lower() in {"current_timestamp()", "now()"}:
                return "DEFAULT CURRENT_TIMESTAMP"
            if column_extra in {"DEFAULT_GENERATED", "default_generated"}:
                if column_default.upper() in {
                    "CURRENT_TIME",
                    "CURRENT_DATE",
                    "CURRENT_TIMESTAMP",
                }:
                    return f"DEFAULT {column_default.upper()}"
                for charset_introducer in CHARSET_INTRODUCERS:
                    if column_default.startswith(charset_introducer):
                        is_binary = False
                        is_hex = False
                        for b_prefix in ("B", "b"):
                            if column_default.startswith(rf"{charset_introducer} {b_prefix}\\'"):
                                is_binary = True
                                break
                        for x_prefix in ("X", "x"):
                            if column_default.startswith(rf"{charset_introducer} {x_prefix}\\'"):
                                is_hex = True
                                break
                        column_default = (
                            column_default.replace(charset_introducer, "")
                            .replace(r"x\\\'", "")
                            .replace(r"X\\\'", "")
                            .replace(r"b\\\'", "")
                            .replace(r"B\\\'", "")
                            .replace(r"\\\'", "")
                            .replace(r"'", "")
                            .strip()
                        )
                        if is_binary:
                            return f"DEFAULT '{chr(int(column_default, 2))}'"
                        if is_hex:
                            return f"DEFAULT x'{column_default}'"
                        return f"DEFAULT '{column_default}'"
            return "DEFAULT '{}'".format(column_default.replace(r"\\\'", r"''"))
        return "DEFAULT '{}'".format(str(column_default).replace(r"\\\'", r"''"))

    @classmethod
    def _data_type_collation_sequence(
        cls, collation: str = CollatingSequences.BINARY, column_type: t.Optional[str] = None
    ) -> str:
        if column_type and collation != CollatingSequences.BINARY:
            if column_type.startswith(
                (
                    "CHARACTER",
                    "NCHAR",
                    "NVARCHAR",
                    "TEXT",
                    "VARCHAR",
                )
            ):
                return f"COLLATE {collation}"
        return ""

    def _check_sqlite_json1_extension_enabled(self) -> bool:
        try:
            self._sqlite_cur.execute("PRAGMA compile_options")
            return "ENABLE_JSON1" in set(row[0] for row in self._sqlite_cur.fetchall())
        except sqlite3.Error:
            return False

    def _build_create_table_sql(self, table_name: str) -> t.Tuple[str, str]:
        """Builds the CREATE TABLE and CREATE INDEX SQL statements."""
        sql: str = f'CREATE TABLE IF NOT EXISTS "{table_name}" ('
        primary: str = ""
        indices: str = ""

        with self._mysql_connection_manager('dict') as cursor:
            cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
            rows: t.Sequence[t.Optional[t.Dict[str, RowItemType]]] = cursor.fetchall()

        primary_keys: int = sum(1 for row in rows if row is not None and row["Key"] == "PRI")

        for row in rows:
            if row is not None:
                column_type = self._translate_type_from_mysql_to_sqlite(
                    column_type=row["Type"],  # type: ignore[arg-type]
                    sqlite_json1_extension_enabled=self._sqlite_json1_extension_enabled,
                )
                if row["Key"] == "PRI" and row["Extra"] == "auto_increment" and primary_keys == 1:
                    if column_type in Integer_Types:
                        sql += '\n\t"{name}" INTEGER PRIMARY KEY AUTOINCREMENT,'.format(
                            name=row["Field"].decode() if isinstance(row["Field"], bytes) else row["Field"],
                        )
                    else:
                        self._logger.warning(
                            'Primary key "%s" in table "%s" is not an INTEGER type! Skipping.',
                            row["Field"],
                            table_name,
                        )
                else:
                    sql += '\n\t"{name}" {type} {notnull} {default} {collation},'.format(
                        name=row["Field"].decode() if isinstance(row["Field"], bytes) else row["Field"],
                        type=column_type,
                        notnull="NULL" if row["Null"] == "YES" else "NOT NULL",
                        default=self._translate_default_from_mysql_to_sqlite(row["Default"], column_type, row["Extra"]),
                        collation=self._data_type_collation_sequence(self._collation, column_type),
                    )

        with self._mysql_connection_manager('dict') as cursor:
            cursor.execute(
                """
                SELECT s.INDEX_NAME AS `name`,
                    IF (NON_UNIQUE = 0 AND s.INDEX_NAME = 'PRIMARY', 1, 0) AS `primary`,
                    IF (NON_UNIQUE = 0 AND s.INDEX_NAME <> 'PRIMARY', 1, 0) AS `unique`,
                    {auto_increment}
                    GROUP_CONCAT(s.COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS `columns`,
                    GROUP_CONCAT(c.COLUMN_TYPE ORDER BY SEQ_IN_INDEX) AS `types`
                FROM information_schema.STATISTICS AS s
                JOIN information_schema.COLUMNS AS c
                    ON s.TABLE_SCHEMA = c.TABLE_SCHEMA
                    AND s.TABLE_NAME = c.TABLE_NAME
                    AND s.COLUMN_NAME = c.COLUMN_NAME
                WHERE s.TABLE_SCHEMA = %s
                AND s.TABLE_NAME = %s
                GROUP BY s.INDEX_NAME, s.NON_UNIQUE {group_by_extra}
                """.format(
                    auto_increment=(
                        "IF (c.EXTRA = 'auto_increment', 1, 0) AS `auto_increment`,"
                        if primary_keys == 1
                        else "0 as `auto_increment`,"
                    ),
                    group_by_extra=" ,c.EXTRA" if primary_keys == 1 else "",
                ),
                (self._mysql_database, table_name),
            )
            mysql_indices: t.Sequence[t.Optional[t.Dict[str, RowItemType]]] = cursor.fetchall()

        for index in mysql_indices:
            if index is not None:
                index_name: str
                if isinstance(index["name"], bytes):
                    index_name = index["name"].decode()
                elif isinstance(index["name"], str):
                    index_name = index["name"]
                else:
                    index_name = str(index["name"])

                # check if the index name collides with any table name
                with self._mysql_connection_manager('dict') as cursor:
                    cursor.execute(
                        """
                        SELECT COUNT(*) AS `count`
                        FROM information_schema.TABLES
                        WHERE TABLE_SCHEMA = %s
                        AND TABLE_NAME = %s
                        """,
                        (self._mysql_database, index_name),
                    )
                    collision: t.Optional[t.Dict[str, RowItemType]] = cursor.fetchone()

                table_collisions: int = 0
                if collision is not None:
                    table_collisions = int(collision["count"])  # type: ignore[arg-type]

                columns: str = ""
                if isinstance(index["columns"], bytes):
                    columns = index["columns"].decode()
                elif isinstance(index["columns"], str):
                    columns = index["columns"]

                types: str = ""
                if isinstance(index["types"], bytes):
                    types = index["types"].decode()
                elif isinstance(index["types"], str):
                    types = index["types"]

                if len(columns) > 0:
                    if index["primary"] in {1, "1"}:
                        if (index["auto_increment"] not in {1, "1"}) or any(
                            self._translate_type_from_mysql_to_sqlite(
                                column_type=_type,
                                sqlite_json1_extension_enabled=self._sqlite_json1_extension_enabled,
                            )
                            not in Integer_Types
                            for _type in types.split(",")
                        ):
                            primary += "\n\tPRIMARY KEY ({columns})".format(
                                columns=", ".join(f'"{column}"' for column in columns.split(","))
                            )
                    else:
                        indices += '''CREATE {unique} INDEX IF NOT EXISTS "{name}" ON "{table}" ({columns});'''.format(
                            unique="UNIQUE" if index["unique"] in {1, "1"} else "",
                            name=(
                                f"{table_name}_{index_name}"
                                if (table_collisions > 0 or self._prefix_indices)
                                else index_name
                            ),
                            table=table_name,
                            columns=", ".join(f'"{column}"' for column in columns.split(","))
                        )

        sql += primary
        sql = sql.rstrip(", ")

        if not self._without_tables and not self._without_foreign_keys:
            server_version: t.Optional[t.Tuple[int, ...]] = self._mysql.get_server_version()
            with self._mysql_connection_manager('dict') as cursor:
                cursor.execute(
                    """
                    SELECT k.COLUMN_NAME AS `column`,
                           k.REFERENCED_TABLE_NAME AS `ref_table`,
                           k.REFERENCED_COLUMN_NAME AS `ref_column`,
                           c.UPDATE_RULE AS `on_update`,
                           c.DELETE_RULE AS `on_delete`
                    FROM information_schema.TABLE_CONSTRAINTS AS i
                    {JOIN} information_schema.KEY_COLUMN_USAGE AS k
                        ON i.CONSTRAINT_NAME = k.CONSTRAINT_NAME
                        AND i.TABLE_NAME = k.TABLE_NAME
                    {JOIN} information_schema.REFERENTIAL_CONSTRAINTS AS c
                        ON c.CONSTRAINT_NAME = i.CONSTRAINT_NAME
                        AND c.TABLE_NAME = i.TABLE_NAME
                    WHERE i.TABLE_SCHEMA = %s
                    AND i.TABLE_NAME = %s
                    AND i.CONSTRAINT_TYPE = %s
                    """.format(
                        JOIN=(
                            "JOIN"
                            if (server_version is not None and server_version[0] == 8 and server_version[2] > 19)
                            else "LEFT JOIN"
                        )
                    ),
                    (self._mysql_database, table_name),
                )
                for foreign_key in cursor.fetchall():
                    if foreign_key is not None:
                        sql += (
                            ',\n\tFOREIGN KEY("{column}") REFERENCES "{ref_table}" ("{ref_column}") '
                            "ON UPDATE {on_update} "
                            "ON DELETE {on_delete}".format(**foreign_key)  # type: ignore[str-bytes-safe]
                        )

        sql += "\n);"

        return sql, indices

    def _create_table(self, table_name: str, create_indexes: bool = True) -> str:
        """Create table with connection management."""
        try:
            table_sql, index_sql = self._build_create_table_sql(table_name)
            if create_indexes:
                self._sqlite_cur.executescript(table_sql + index_sql)
            else:
                self._sqlite_cur.executescript(table_sql)
            self._sqlite.commit()
            return index_sql

        except mysql.connector.Error as err:
            self._logger.error(
                "MySQL failed reading table definition from table %s: %s",
                table_name, err
            )
            raise
        except sqlite3.Error as err:
            self._logger.error("SQLite failed creating table %s: %s", table_name, err)
            raise

    def _get_table_keys(self, table_name: str) -> t.Optional[t.List[str]]:
        """Get primary key or first unique key for a table."""
        with self._mysql_connection_manager('dict') as cursor:
            # Find the best key to use for pagination
            # Priority: 1. Primary Key, 2. First Unique Key
            cursor.execute("""
                SELECT INDEX_NAME, NON_UNIQUE, GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) as COLUMNS
                FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                GROUP BY INDEX_NAME, NON_UNIQUE
                ORDER BY NON_UNIQUE, INDEX_NAME
            """, (self._mysql_database, table_name))
            
            keys = cursor.fetchall()
            
            primary_key = next((k['COLUMNS'].split(',') for k in keys if k['INDEX_NAME'] == 'PRIMARY'), None)
            if primary_key:
                self._logger.debug(f"Using PRIMARY KEY {primary_key} for keyset pagination on table '{table_name}'")
                return primary_key
            
            unique_key = next((k['COLUMNS'].split(',') for k in keys if k['NON_UNIQUE'] == 0), None)
            if unique_key:
                self._logger.debug(f"Using UNIQUE KEY {unique_key} for keyset pagination on table '{table_name}'")
                return unique_key

        self._logger.warning(f"No primary or unique key found for table '{table_name}'. Falling back to less efficient OFFSET pagination.")
        return None

    def _chunked_data_iterator_with_resume(self, table_name: str, total_records: int) -> Iterator[t.Tuple[int, t.List[t.Tuple[t.Any, ...]], dict]]:
        """Iterator that yields data chunks with resume support and timing info."""
        start_offset = 0
        last_key_values = None

        if table_name in self._resume_state:
            state = self._resume_state[table_name]
            start_offset = state.get('offset', 0)
            last_key_values = state.get('last_key_values')
            if last_key_values:
                self._logger.info(f"Resuming table '{table_name}' from last key: {last_key_values}")
            else:
                self._logger.info(f"Resuming table '{table_name}' from offset {start_offset}")

        if self._chunk_size is None or self._chunk_size <= 0:
            self._logger.warning("Chunk size not set, defaulting to 10000. This is not recommended for large tables without a key.")
            self._chunk_size = 10000

        key_columns = self._get_table_keys(table_name)
        
        offset = start_offset
        chunk_num = (offset // self._chunk_size) if self._chunk_size > 0 else 0
        
        while offset < total_records:
            chunk_start_time = time.time()
            
            with self._mysql_connection_manager('raw') as cursor:
                query = f"SELECT * FROM `{table_name}`"
                params = ()

                # Keyset Pagination (if possible)
                if key_columns:
                    order_by_clause = " ORDER BY " + ", ".join([f"`{col}` ASC" for col in key_columns])
                    if last_key_values:
                        where_clause = " WHERE (" + ",".join([f"`{col}`" for col in key_columns]) + ") > (" + ",".join(["%s"] * len(key_columns)) + ")"
                        query += where_clause
                        params = tuple(last_key_values)
                    query += order_by_clause
                else:  # Fallback to OFFSET
                    query += " ORDER BY (SELECT NULL)"  # Required for LIMIT on some MySQL versions

                # Add LIMIT
                limit_clause = f" LIMIT {self._chunk_size}"
                if self._limit_rows > 0:
                    remaining = min(self._limit_rows - offset, self._chunk_size)
                    if remaining <= 0:
                        break
                    limit_clause = f" LIMIT {remaining}"
                
                if not key_columns:  # Add OFFSET only for fallback
                    limit_clause += f" OFFSET {offset}"

                query += limit_clause
                
                self._logger.debug(f"Executing chunk {chunk_num + 1}: {query} with params {params}")
                
                try:
                    query_start = time.time()
                    cursor.execute(query, params)
                    query_time = time.time() - query_start
                    
                    fetch_start = time.time()
                    chunk = cursor.fetchall()
                    fetch_time = time.time() - fetch_start
                    
                    if not chunk:
                        break
                    
                    process_start = time.time()
                    processed_chunk = [tuple(row) for row in chunk]
                    process_time = time.time() - process_start
                    
                    total_chunk_time = time.time() - chunk_start_time
                    
                    new_last_key_values = None
                    if key_columns:
                        try:
                            col_names = [desc[0].decode() if isinstance(desc[0], bytes) else desc[0] for desc in cursor.description]
                            key_indices = [col_names.index(col) for col in key_columns]
                            new_last_key_values = [processed_chunk[-1][i] for i in key_indices]
                        except ValueError as e:
                            self._logger.error(f"Could not find key column in result set: {e}. Disabling keyset pagination.")
                            key_columns = None # Disable for next chunks

                    yield offset, processed_chunk, {
                        'query_time': query_time,
                        'fetch_time': fetch_time,
                        'process_time': process_time,
                        'total_time': total_chunk_time,
                        'rows': len(chunk),
                        'last_key': new_last_key_values
                    }
                    
                    offset += len(chunk)
                    chunk_num += 1
                    last_key_values = new_last_key_values
                    
                except Exception as e:
                    self._logger.error(f"Error fetching chunk at offset {offset}: {e}")
                    self._save_resume_state(table_name, offset, total_records, last_key_values if key_columns else None)
                    raise

    def _sanitize_data_for_sqlite(self, chunk_data: t.List[t.Tuple[t.Any, ...]]) -> t.List[t.Tuple[t.Any, ...]]:
        """Sanitize data before insertion into SQLite to avoid syntax errors."""
        sanitized_chunk = []
        for row in chunk_data:
            sanitized_row = tuple(
                encode_data_for_sqlite(col) if col is not None else None for col in row
            )
            sanitized_chunk.append(sanitized_row)
        return sanitized_chunk

    def _transfer_table_data_with_resume(self, table_name: str, sql: str, total_records: int = 0) -> None:
        """Transfer table data with accurate progress tracking."""
        try:
            if total_records == 0:
                return
            
            start_offset = 0
            last_key_values = None
            if table_name in self._resume_state:
                state = self._resume_state[table_name]
                start_offset = state.get('offset', 0)
                last_key_values = state.get('last_key_values')

            remaining_records = total_records - start_offset
            total_chunks = int(ceil(total_records / self._chunk_size)) if self._chunk_size else 1
            
            if start_offset > 0:
                self._logger.info(
                    f"📂 RESUMING table '{table_name}' from row {start_offset:,} / {total_records:,} "
                    f"({start_offset/total_records*100:.1f}% already done)"
                )
            else:
                self._logger.info(
                    f"📊 STARTING table '{table_name}' with {total_records:,} total rows "
                    f"(chunk size: {self._chunk_size:,} rows)"
                )
            
            progress_bar = trange(
                total_chunks, 
                initial=start_offset // self._chunk_size if self._chunk_size else 0,
                total=int(ceil(total_records / self._chunk_size)) if self._chunk_size else 1,
                disable=self._quiet, 
                desc=f"Transferring {table_name}"
            )
            
            self._chunk_times = []
            chunks_since_reconnect = 0
            transfer_start_time = time.time()
            total_bytes_transferred = 0
            last_log_time = time.time()
            rows_at_start = start_offset
            last_progress_rows = start_offset
            
            for offset, chunk_data, timing_info in self._chunked_data_iterator_with_resume(table_name, total_records):
                chunk_num = (offset // self._chunk_size) + 1 if self._chunk_size > 0 else 1
                last_key_values = timing_info.get('last_key')
                
                if timing_info['fetch_time'] > self._stall_timeout:
                    self._logger.warning(
                        f"⚠️  STALL DETECTED: Chunk {chunk_num} fetch took {timing_info['fetch_time']:.1f}s "
                        f"(threshold: {self._stall_timeout}s). Saving progress and reconnecting..."
                    )
                    self._save_resume_state(table_name, offset, total_records, last_key_values)
                    
                    self._logger.info("🔄 Forcing MySQL reconnection...")
                    self._connect_to_mysql(is_reconnect=True)
                    
                    self._chunk_times = []
                    chunks_since_reconnect = 0
                    continue
                
                chunk_bytes = sum(len(str(col).encode('utf-8', 'replace')) for row in chunk_data for col in row if col is not None)
                total_bytes_transferred += chunk_bytes
                
                try:
                    insert_start = time.time()
                    sanitized_chunk_data = self._sanitize_data_for_sqlite(chunk_data)
                    self._sqlite_cur.executemany(sql, sanitized_chunk_data)
                    insert_time = time.time() - insert_start
                    
                    commit_start = time.time()
                    self._sqlite.commit()
                    commit_time = time.time() - commit_start
                    
                    total_chunk_time = timing_info['total_time'] + insert_time + commit_time
                    self._chunk_times.append(total_chunk_time)
                    
                    chunks_since_reconnect += 1
                    current_row = offset + len(chunk_data)
                    
                    current_time = time.time()
                    should_log = (
                        current_time - last_log_time >= 30 or 
                        chunks_since_reconnect % 10 == 0 or
                        total_chunk_time > 60
                    )
                    
                    if should_log:
                        elapsed_total = current_time - transfer_start_time
                        elapsed_interval = current_time - last_log_time
                        rows_in_interval = current_row - last_progress_rows
                        
                        overall_rate = (current_row - rows_at_start) / elapsed_total if elapsed_total > 0 else 0
                        interval_rate = rows_in_interval / elapsed_interval if elapsed_interval > 0 and rows_in_interval > 0 else overall_rate
                        
                        mb_transferred = total_bytes_transferred / 1024 / 1024
                        mb_rate = (total_bytes_transferred / (1024*1024)) / elapsed_total if elapsed_total > 0 else 0
                        
                        rows_remaining = total_records - current_row
                        eta_str = str(timedelta(seconds=int(rows_remaining / interval_rate))) if interval_rate > 0 else "Unknown"
                        
                        if len(self._chunk_times) >= 3:
                            recent_avg = sum(self._chunk_times[-3:]) / 3
                            if len(self._chunk_times) >= 6:
                                older_avg = sum(self._chunk_times[-6:-3]) / 3
                                if recent_avg > older_avg * 1.5: perf_indicator = "🔴 SLOWING"
                                elif recent_avg > older_avg * 1.2: perf_indicator = "🟡 DEGRADING"
                                elif recent_avg < older_avg * 0.8: perf_indicator = "🟢 IMPROVING"
                                else: perf_indicator = "🔵 STABLE"
                            else: perf_indicator = "🔵 WARMING UP"
                        else: perf_indicator = "🔵 STARTING"
                        
                        timing_breakdown = (
                            f"query: {timing_info['query_time']:.1f}s, "
                            f"fetch: {timing_info['fetch_time']:.1f}s, "
                            f"process: {timing_info['process_time']:.1f}s, "
                            f"insert: {insert_time:.1f}s, "
                            f"commit: {commit_time:.1f}s"
                        )
                        
                        self._logger.info(
                            f"📈 PROGRESS: {current_row:,} / {total_records:,} rows "
                            f"({current_row/total_records*100:.1f}%) | "
                            f"Chunk {chunk_num} took {total_chunk_time:.1f}s ({timing_breakdown}) | "
                            f"Speed: {interval_rate:.0f} rows/s, {mb_rate:.1f} MB/s | "
                            f"Total: {mb_transferred:.1f} MB | "
                            f"ETA: {eta_str} | {perf_indicator}"
                        )
                        
                        last_log_time = current_time
                        last_progress_rows = current_row
                    
                    if chunks_since_reconnect % 10 == 0:
                        self._save_resume_state(table_name, current_row, total_records, last_key_values)
                        self._logger.debug(f"💾 Saved resume checkpoint at row {current_row:,}")
                    
                    if total_chunk_time > 120:
                        self._logger.warning(
                            f"⚠️  SLOW CHUNK: Chunk {chunk_num} took {total_chunk_time:.1f}s total "
                            f"(fetch: {timing_info['fetch_time']:.1f}s for {chunk_bytes/1024/1024:.1f} MB). "
                            f"Consider reducing chunk size or checking network/server load."
                        )
                    
                    progress_bar.update(1)
                    self._last_progress_time = time.time()
                    
                except sqlite3.Error as err:
                    self._logger.error(
                        f"❌ SQLite error at chunk {chunk_num} (offset {offset:,}): {err}"
                    )
                    self._save_resume_state(table_name, offset, total_records, last_key_values)
                    raise
            
            progress_bar.close()
            
            total_time = time.time() - transfer_start_time
            total_mb = total_bytes_transferred / 1024 / 1024
            rows_transferred = total_records - rows_at_start
            
            self._logger.info(
                f"✅ COMPLETED table '{table_name}': {rows_transferred:,} rows, "
                f"{total_mb:.1f} MB in {str(timedelta(seconds=int(total_time)))} "
                f"({rows_transferred/total_time:.0f} rows/s, {total_mb/total_time:.1f} MB/s average)"
            )
            
            self._clear_resume_state(table_name)
            
        except KeyboardInterrupt:
            current_row = offset if 'offset' in locals() else start_offset
            self._logger.warning(
                f"⚠️  INTERRUPTED at row {current_row:,} / {total_records:,} "
                f"({current_row/total_records*100:.1f}%). Progress saved - run again to resume."
            )
            self._save_resume_state(table_name, current_row, total_records, last_key_values if 'last_key_values' in locals() else None)
            raise
        except Exception as err:
            self._logger.error(f"❌ Transfer failed for table '{table_name}': {err}")
            raise

    def _transfer_table_data(self, table_name: str, sql: str, total_records: int = 0) -> None:
        """Wrapper to use resume-capable transfer."""
        self._transfer_table_data_with_resume(table_name, sql, total_records)

    def _get_tables_to_transfer(self) -> t.List[str]:
        """Get the list of tables to transfer based on configured filters."""
        all_tables = get_mysql_tables(self._mysql)
        
        if not self._mysql_tables and not self._exclude_mysql_tables:
            return all_tables

        if self._mysql_tables:
            tables_to_transfer = []
            for table in all_tables:
                for pattern in self._mysql_tables:
                    if fnmatch.fnmatch(table, pattern):
                        tables_to_transfer.append(table)
                        break
        elif self._exclude_mysql_tables:
            tables_to_transfer = []
            for table in all_tables:
                excluded = False
                for pattern in self._exclude_mysql_tables:
                    if fnmatch.fnmatch(table, pattern):
                        excluded = True
                        break
                if not excluded:
                    tables_to_transfer.append(table)
        else:
            tables_to_transfer = all_tables
        
        # Remove duplicates while preserving order
        seen = set()
        tables_to_transfer = [x for x in tables_to_transfer if not (x in seen or seen.add(x))]
        
        if self._min_rows_to_export > 0:
            filtered_tables = []
            for table in tables_to_transfer:
                with self._mysql_connection_manager('dict') as cursor:
                    cursor.execute(
                        f"SELECT TABLE_ROWS FROM information_schema.TABLES "
                        f"WHERE TABLE_SCHEMA = '{self._mysql_database}' AND TABLE_NAME = '{table}'"
                    )
                    row_count = cursor.fetchone()
                    if row_count and row_count['TABLE_ROWS'] >= self._min_rows_to_export:
                        filtered_tables.append(table)
            return filtered_tables
        
        return tables_to_transfer

    def transfer(self) -> None:
        """The primary transfer method with robust error handling."""
        tables = self._get_tables_to_transfer()

        try:
            # Turn off foreign key checking in SQLite while transferring data
            self._sqlite_cur.execute("PRAGMA foreign_keys=OFF")
            
            # Optimize SQLite for bulk inserts
            if self._optimize_for_blobs:
                # Balanced and crash-safe optimizations for BLOB data
                self._sqlite_cur.execute("PRAGMA journal_mode=WAL")
                self._sqlite_cur.execute("PRAGMA synchronous=NORMAL")
                self._sqlite_cur.execute("PRAGMA locking_mode=EXCLUSIVE")
                self._logger.warning(
                    "Using balanced and crash-safe BLOB optimizations."
                )
            else:
                # Standard optimizations (current behavior)
                self._sqlite_cur.execute("PRAGMA synchronous=OFF")
                self._sqlite_cur.execute("PRAGMA journal_mode=MEMORY")
                self._sqlite_cur.execute("PRAGMA cache_size=1000000")
            
            for table_name in tables:
                if isinstance(table_name, bytes):
                    table_name = table_name.decode()

                # Check if table is already completed (not in resume state)
                if table_name in self._resume_state:
                    self._logger.info(f"Resuming incomplete transfer for table: {table_name}")
                elif self._skip_existing_tables:
                    self._sqlite_cur.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", 
                        (table_name,)
                    )
                    if self._sqlite_cur.fetchone():
                        self._logger.info(f"Skipping existing table: {table_name}")
                        continue

                self._logger.info(
                    "%s%sTransferring table %s",
                    "[WITHOUT DATA] " if self._without_data else "",
                    "[ONLY DATA] " if self._without_tables else "",
                    table_name,
                )

                index_sql = ""
                if not self._without_tables:
                    # Check if table exists (for resume case)
                    self._sqlite_cur.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", 
                        (table_name,)
                    )
                    if not self._sqlite_cur.fetchone():
                        index_sql = self._create_table(table_name, create_indexes=False)

                if not self._without_data:
                    # Get total record count
                    with self._mysql_connection_manager('dict') as cursor:
                        if self._limit_rows > 0:
                            cursor.execute(
                                f"SELECT COUNT(*) AS `total_records` "
                                f"FROM (SELECT * FROM `{table_name}` LIMIT {self._limit_rows}) AS `table`"
                            )
                        else:
                            cursor.execute(f"SELECT COUNT(*) AS `total_records` FROM `{table_name}`")
                        
                        result = cursor.fetchone()
                        total_records_count = int(result["total_records"]) if result else 0

                    if total_records_count > 0:
                        # Get column information for building SQL
                        with self._mysql_connection_manager('raw') as cursor:
                            cursor.execute(f"SELECT * FROM `{table_name}` LIMIT 0")
                            columns = tuple(column[0] for column in cursor.description)
                        
                        # Build SQL for insertion
                        sql = '''
                            INSERT OR IGNORE
                            INTO "{table}" ({fields})
                            VALUES ({placeholders})
                        '''.format(
                            table=table_name,
                            fields=", ".join('"{}"'.format(col) for col in columns),
                            placeholders=("?, " * len(columns)).rstrip(" ,"),
                        )
                        
                        # Use a single transaction for the whole table
                        try:
                            self._sqlite_cur.execute("BEGIN IMMEDIATE")
                            self._transfer_table_data(
                                table_name=table_name,
                                sql=sql,
                                total_records=total_records_count,
                            )
                            self._sqlite.commit()
                        except Exception as e:
                            self._sqlite.rollback()
                            raise e

                if not self._without_tables and index_sql:
                    self._logger.info(f"Creating indexes for table {table_name}")
                    self._sqlite_cur.executescript(index_sql)
                    self._sqlite.commit()

        except Exception:
            raise
        finally:
            # Restore SQLite settings
            if self._optimize_for_blobs:
                # Restore safer settings for BLOB mode
                self._sqlite_cur.execute("PRAGMA synchronous=NORMAL")  # Safer than FULL
                self._sqlite_cur.execute("PRAGMA journal_mode=WAL")  # Better than DELETE for large DBs
            else:
                # Standard restore
                self._sqlite_cur.execute("PRAGMA synchronous=FULL")
                self._sqlite_cur.execute("PRAGMA journal_mode=DELETE")
            
            self._sqlite_cur.execute("PRAGMA foreign_keys=ON")

        if self._vacuum:
            self._logger.info("Vacuuming created SQLite database file.\nThis might take a while.")
            self._sqlite_cur.execute("VACUUM")
            self._logger.info("Analyzing database.")
            self._sqlite_cur.execute("ANALYZE")
            self._logger.info("Checking database integrity.")
            self._sqlite_cur.execute("PRAGMA quick_check;")


        self._logger.info("Done!")
        
        # Clean up resume file if all tables completed
        if exists(self._resume_file) and not self._resume_state:
            try:
                os.remove(self._resume_file)
                self._logger.info(f"Removed resume file: {self._resume_file}")
            except:
                pass

        if self._mysql.is_connected():
            self._mysql.close()
        self._sqlite.close()
