import sqlite3
import mysql.connector
from mysql.connector import errors

# Параметры подключения к SQLite
sqlite_db_path = "./saper.db"

# Параметры подключения к MySQL
mysql_host = "127.0.0.1"
mysql_user = "root"
mysql_password = ""
mysql_db = "FOCH"  # Имя вашей базы данных

try:
    # Создание соединения с SQLite
    sqlite_conn = sqlite3.connect(sqlite_db_path)

    # Создание двух соединений (SQLite и MySQL)
    mysql_conn = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_db,
        charset="utf8mb4",  # Установите кодировку явно
        collation="utf8mb4_bin",  # Установите культуру явно
    )
    mysql_cursor = mysql_conn.cursor()

    # Создание таблицы в MySQL
    create_table_query = """
        CREATE TABLE IF NOT EXISTS `sfsafasfaf_asf_asf_34` (
            `post_id` INTEGER,
            `post` TEXT,
            `img` LONGBLOB
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
    """
    mysql_cursor.execute(create_table_query)

    # Итерация по таблицам SQLite и их конвертация в MySQL
    sqlite_cursor = sqlite_conn.cursor()
    sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = sqlite_cursor.fetchall()

    for table in tables:
        table_name = table[0]

        # Игнорирование системной таблицы `sqlite_sequence`
        if table_name != "sqlite_sequence":
            # Получение данных из SQLite
            sqlite_cursor.execute(f"SELECT * FROM {table_name}")
            rows = sqlite_cursor.fetchall()

            # Создание таблицы в MySQL
            create_table_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ("
            sqlite_cursor.execute(f"PRAGMA table_info({table_name});")
            columns = sqlite_cursor.fetchall()

            column_definitions = []
            for column in columns:
                column_name = column[1]
                # Игнорирование столбца `seq`
                if column_name != "seq":
                    column_type = column[2]
                    column_definitions.append(f"`{column_name}` {column_type}")

            create_table_query += ", ".join(column_definitions) + ");"
            mysql_cursor.execute(create_table_query)

            # Вставка данных в MySQL (вставка по частям)
            chunk_size = 1000
            for row_chunk in [rows[i:i + chunk_size] for i in range(0, len(rows), chunk_size)]:
                for row in row_chunk:
                    # Игнорирование столбца `seq`
                    row_values = []
                    for idx, value in enumerate(row):
                        if columns[idx][1] == "img":
                            if value is not None:
                                row_values.append(mysql.connector.Binary(value))
                            else:
                                row_values.append(None)
                        else:
                            row_values.append(value)

                    placeholders = ", ".join(["%s"] * len(row_values))
                    insert_query = f"INSERT INTO `{table_name}` VALUES ({placeholders})"
                    try:
                        mysql_cursor.execute(insert_query, row_values)
                    except errors.DatabaseError as e:
                        print(f"Error inserting data into {table_name}: {e}")

    # Коммит изменений в MySQL и закрытие соединений
    mysql_conn.commit()
    print("Data transfer successful.")
except errors.DatabaseError as e:
    print(f"Error: {e}")
finally:
    # В любом случае закрываем соединения
    if 'sqlite_conn' in locals():
        sqlite_conn.close()
    if 'mysql_conn' in locals():
        mysql_conn.close()
