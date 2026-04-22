import mysql.connector
import logging

logging.basicConfig(filename='pz.log', filemode='w', level=logging.INFO)


class SQLTable:
    def __init__(self, db_config, table_name, engine='mysql'):
        self.db_config = db_config
        self.table_name = table_name
        self.engine = engine
        self.connection = None
        self.cursor = None
        self.columns = []
        self.connect()

    def connect(self):
        try:
            if self.engine == 'mysql':
                import mysql.connector
                self.connection = mysql.connector.connect(**self.db_config)
                self.cursor = self.connection.cursor(dictionary=True)
            elif self.engine == 'postgresql':
                import psycopg2
                import psycopg2.extras
                pg_config = {
                    'host': self.db_config.get('host'),
                    'user': self.db_config.get('user'),
                    'password': self.db_config.get('password'),
                    'port': self.db_config.get('port', 5432),
                    'dbname': self.db_config.get('database') or self.db_config.get('dbname')
                }
                self.connection = psycopg2.connect(**pg_config)
                self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            logging.info("Подключение к базе данных успешно установлено")
            self._update_column_names()
            return self.connection
        except Exception as e:
            logging.error(f"Ошибка подключения к базе данных: {e}")
            raise e

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            if self.engine == 'mysql' and self.connection.is_connected():
                self.connection.close()
            elif self.engine == 'postgresql':
                self.connection.close()
            logging.info("Соединение закрыто")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def _update_column_names(self):
        try:
            if self.engine == 'mysql':
                self.cursor.execute(f"SHOW COLUMNS FROM {self.table_name}")
                self.columns = [row['Field'] for row in self.cursor.fetchall()]
            else:
                self.cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = %s
                """, (self.table_name,))
                self.columns = [row['column_name'] for row in self.cursor.fetchall()]
        except Exception as e:
            logging.error(f"Ошибка при получении списка колонок: {e}")

    def _check_table_exists(self):
        if self.engine == 'mysql':
            self.cursor.execute("SHOW TABLES LIKE %s", (self.table_name,))
            return self.cursor.fetchone() is not None
        else:
            self.cursor.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = %s
                )
            """, (self.table_name,))
            return self.cursor.fetchone()['exists']

    def _find_primary_key(self):
        if self.engine == 'mysql':
            self.cursor.execute(f"SHOW KEYS FROM {self.table_name} WHERE Key_name = 'PRIMARY'")
            result = self.cursor.fetchone()
            return result['Column_name'] if result else None
        else:
            self.cursor.execute("""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass AND i.indisprimary
            """, (self.table_name,))
            result = self.cursor.fetchone()
            return result['attname'] if result else None

    def _log(self, query, params=None):
        logging.info(f"Query: {query} | Params: {params}")

    def create_table(self, columns_def):
        if self.engine == 'postgresql':
            columns_def = columns_def.replace('AUTO_INCREMENT', 'SERIAL')
        query = f"CREATE TABLE IF NOT EXISTS {self.table_name} ({columns_def})"
        self._log(query, ())
        self.cursor.execute(query)
        self.connection.commit()
        self._update_column_names()

    def drop_table(self):
        query = f"DROP TABLE IF EXISTS {self.table_name}"
        self.cursor.execute(query)
        self.connection.commit()

    def insert(self, data):
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        if self.engine == 'postgresql':
            query = f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders}) RETURNING id"
            self.cursor.execute(query, list(data.values()))
            self.connection.commit()
            return self.cursor.fetchone()['id']
        else:
            query = f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})"
            self.cursor.execute(query, list(data.values()))
            self.connection.commit()
            return self.cursor.lastrowid

    def insert_many(self, rows):
        if not rows:
            return 0
        columns = ', '.join(rows[0].keys())
        placeholders = ', '.join(['%s'] * len(rows[0]))
        query = f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})"
        values = [list(row.values()) for row in rows]

        if self.engine == 'postgresql':
            from psycopg2.extras import execute_values
            execute_values(self.cursor, query, values)
        else:
            self.cursor.executemany(query, values)

        self.connection.commit()
        return self.cursor.rowcount

    def _build_where(self, filters=None, condition=None):
        if condition:
            return f" WHERE {condition}", []
        if not filters:
            return "", []
        clauses = []
        params = []
        for key, val in filters.items():
            if isinstance(val, tuple) and len(val) == 2:
                op, v = val
                clauses.append(f"{key} {op} %s")
                params.append(v)
            else:
                clauses.append(f"{key} = %s")
                params.append(val)
        return " WHERE " + " AND ".join(clauses), params

    def select(self, columns='*', filters=None, condition=None, order_by=None, limit=None):
        where_clause, params = self._build_where(filters, condition)
        query = f"SELECT {columns} FROM {self.table_name}{where_clause}"
        if order_by:
            query += f" ORDER BY {order_by}"
        if limit:
            query += f" LIMIT {limit}"
        self._log(query, params)
        self.cursor.execute(query, params)
        return self.cursor.fetchall()

    def select_one(self, columns='*', filters=None, condition=None):
        results = self.select(columns, filters, condition, limit=1)
        return results[0] if results else None

    def update(self, data, filters=None, condition=None):
        set_clause = ', '.join([f"{k} = %s" for k in data.keys()])
        where_clause, params = self._build_where(filters, condition)
        query = f"UPDATE {self.table_name} SET {set_clause}{where_clause}"
        self._log(query, list(data.values()) + params)
        self.cursor.execute(query, list(data.values()) + params)
        self.connection.commit()
        return self.cursor.rowcount

    def delete(self, filters=None, condition=None):
        where_clause, params = self._build_where(filters, condition)
        query = f"DELETE FROM {self.table_name}{where_clause}"
        self._log(query, params)
        self.cursor.execute(query, params)
        self.connection.commit()
        return self.cursor.rowcount

    def join_query(self, other_table, on, join_type='INNER', columns='*', filters=None, condition=None):
        join_type = join_type.upper()
        if join_type not in ('INNER', 'LEFT', 'RIGHT', 'FULL'):
            raise ValueError("Допустимые типы JOIN: INNER, LEFT, RIGHT, FULL")

        if join_type == 'FULL' and self.engine == 'mysql':
            return self._full_join_mysql(other_table, on, columns, filters, condition)

        where, params = self._build_where(filters, condition)
        query = f"SELECT {columns} FROM {self.table_name} {join_type} JOIN {other_table} ON {on}{where}"
        self._log(query, params)
        self.cursor.execute(query, params)
        return self.cursor.fetchall()

    def _full_join_mysql(self, other_table, on, columns, filters, condition):
        where, params = self._build_where(filters, condition)
        left_q = f"SELECT {columns} FROM {self.table_name} LEFT JOIN {other_table} ON {on}{where}"
        right_q = f"SELECT {columns} FROM {self.table_name} RIGHT JOIN {other_table} ON {on}{where}"

        self.cursor.execute(left_q, params)
        left = self.cursor.fetchall()
        self.cursor.execute(right_q, params)
        right = self.cursor.fetchall()

        seen = set()
        merged = []
        for row in left + right:
            key = tuple(sorted(row.items()))
            if key not in seen:
                seen.add(key)
                merged.append(row)
        return merged

    def union_query(self, queries, distinct=True):
        operator = 'UNION' if distinct else 'UNION ALL'
        full_sql = f' {operator} '.join([q for q, _ in queries])
        all_params = []
        for _, p in queries:
            if p:
                all_params.extend(p)
        self._log(full_sql, all_params)
        self.cursor.execute(full_sql, all_params)
        return self.cursor.fetchall()
