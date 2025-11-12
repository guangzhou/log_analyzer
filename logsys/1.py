import sqlite3

# 1. 连接到 SQLite 数据库文件
db_path = 'logsys.db'  # 替换为你的 .db 文件路径
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# 2. 查看数据库中所有的表
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print("数据库中的表：")
for table in tables:
    print(f"  - {table[0]}")

# 3. 遍历每个表，查看其结构和前几行数据
for table_name in tables:
    table = table_name[0]
    print(f"\n{'='*50}")
    print(f"表名: {table}")

    # 查看表结构（列信息）
    cursor.execute(f"PRAGMA table_info({table});")
    columns = cursor.fetchall()
    print("结构（字段）:")
    for col in columns:
        print(f"  {col[1]} ({col[2]}) {'(主键)' if col[5] else ''}")

    # 查看前5行数据
    try:
        cursor.execute(f"SELECT * FROM {table} LIMIT 5;")
        rows = cursor.fetchall()
        print("前5行数据:")
        if rows:
            for row in rows:
                print(f"  {row}")
        else:
            print("  （无数据）")
    except Exception as e:
        print(f"  读取数据时出错: {e}")

# 关闭连接
conn.close()