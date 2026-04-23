import mysql.connector
import psycopg2
import psycopg2.extras
import logging

logging.basicConfig(filename='pz.log', filemode='w', level=logging.INFO)


class SQLTable:
    def __init__(self, db_config, table_name, engine='mysql'):
        self.db_config = db_config.copy()
        self.table_name = table_name
        self.engine = engine.lower()
        self.connection = None
        self.cursor = None
        self.connect()

    def connect(self):
        try:
            if self.engine == 'mysql':
                if 'dbname' in self.db_config:
                    self.db_config['database'] = self.db_config.pop('dbname')
                self.connection = mysql.connector.connect(**self.db_config)
                self.cursor = self.connection.cursor(dictionary=True)
            elif self.engine == 'postgresql':
                if 'database' in self.db_config:
                    self.db_config['dbname'] = self.db_config.pop('database')
                self.connection = psycopg2.connect(**self.db_config)
                self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            logging.info(f"Подключение к {self.engine.upper()} успешно")
        except Exception as e:
            logging.error(f"Ошибка подключения: {e}")
            raise e

    def _build_where(self, filters=None):
        if not filters:
            return "", []
        clauses, params = [], []
        for col, val in filters.items():
            if isinstance(val, (list, tuple)) and len(val) == 2:
                operator, value = val
                clauses.append(f"{col} {operator} %s")
                params.append(value)
            else:
                clauses.append(f"{col} = %s")
                params.append(val)
        return " WHERE " + " AND ".join(clauses), params

    def select(self, columns='*', filters=None, order_by=None, limit=None):
        where_sql, params = self._build_where(filters)
        query = f"SELECT {columns} FROM {self.table_name}{where_sql}"
        if order_by: query += f" ORDER BY {order_by}"
        if limit: query += f" LIMIT {limit}"
        self.cursor.execute(query, params)
        return self.cursor.fetchall()

    def insert(self, data):
        cols = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO {self.table_name} ({cols}) VALUES ({placeholders})"
        self.cursor.execute(query, list(data.values()))
        self.connection.commit()

    def update(self, data, filters=None):
        where_sql, w_params = self._build_where(filters)
        set_clause = ', '.join([f"{k} = %s" for k in data.keys()])
        query = f"UPDATE {self.table_name} SET {set_clause}{where_sql}"
        self.cursor.execute(query, list(data.values()) + w_params)
        self.connection.commit()

    def delete(self, filters=None):
        where_sql, params = self._build_where(filters)
        query = f"DELETE FROM {self.table_name}{where_sql}"
        self.cursor.execute(query, params)
        self.connection.commit()

    def create_table(self, definition):
        if self.engine == 'postgresql':
            definition = definition.replace('INT AUTO_INCREMENT', 'SERIAL')
            definition = definition.replace('id INT', 'id SERIAL')
        query = f"CREATE TABLE IF NOT EXISTS {self.table_name} ({definition})"
        self.cursor.execute(query)
        self.connection.commit()

    def drop_table(self):
        self.cursor.execute(f"DROP TABLE IF EXISTS {self.table_name} CASCADE")
        self.connection.commit()

    def join_query(self, other_table, on, join_type="INNER", columns="*", filters=None):
        jt = join_type.upper()
        where_sql, params = self._build_where(filters)

        if jt == "FULL" and self.engine == "mysql":
            # эмуляция
            q_left = f"SELECT {columns} FROM {self.table_name} LEFT JOIN {other_table} ON {on}{where_sql}"
            q_right = f"SELECT {columns} FROM {self.table_name} RIGHT JOIN {other_table} ON {on}{where_sql}"

            self.cursor.execute(q_left, params)
            res_left = self.cursor.fetchall()

            self.cursor.execute(q_right, params)
            res_right = self.cursor.fetchall()

            # дедупликация
            seen, merged = set(), []
            for r in res_left + res_right:
                key = tuple(sorted(r.items(), key=lambda x: x[0]))
                if key not in seen:
                    seen.add(key)
                    merged.append(r)
            return merged
        else:
            query = f"SELECT {columns} FROM {self.table_name} {jt} JOIN {other_table} ON {on}{where_sql}"
            self.cursor.execute(query, params)
            return self.cursor.fetchall()

    def union_query(self, queries, union_all=False):
        op = " UNION ALL " if union_all else " UNION "
        full_query = op.join([q for q, _ in queries])
        all_params = []
        for _, p in queries:
            all_params.extend(p)
        self.cursor.execute(full_query, all_params)
        return self.cursor.fetchall()

    def disconnect(self):
        if self.cursor: self.cursor.close()
        if self.connection: self.connection.close()
