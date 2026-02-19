
import mysql.connector

host = '117.72.162.13'
user = 'root'
password = 'WZHwzh123!!!'
database = 'quant'

print("=" * 60)
print("开始解锁")
print("=" * 60)

conn = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)

cursor = conn.cursor()

print("查看当前进程:")
cursor.execute("SHOW FULL PROCESSLIST")
processes = cursor.fetchall()
for p in processes:
    print(f"ID: {p[0]}, 命令: {p[4]}, 状态: {p[6]}, 信息: {p[7]}")

print("\n正在杀死相关进程...")
for p in processes:
    pid = p[0]
    info = p[7]
    if info and ('DELETE' in str(info) or 'UPDATE' in str(info) or 'ALTER' in str(info) or 'INSERT' in str(info)):
        try:
            print(f"  杀死进程 {pid}...")
            cursor.execute(f"KILL {pid}")
            print(f"    ✓ 成功")
        except Exception as e:
            print(f"    ✗ 失败: {e}")

cursor.close()
conn.close()

print("\n解锁完成！")
print("=" * 60)
