from pymongo import MongoClient, ASCENDING
import os
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

client = MongoClient(
    host=os.getenv('mongo_host'),
    port=int(os.getenv('mongo_port')),
    username=os.getenv('mongo_username'),
    password=os.getenv('mongo_password'),
    authSource=os.getenv('auth_source')
)

# 选择数据库和集合
db = client['stock_data']

# 指定Z盘路径
z_drive_path = 'Y:\\'

# 要查找的文件名
target_files = [
    '1101_2023-01-03_ticks.csv',
    '1101_2023-01-03_openprice.csv',
    '1101_2023-01-04_ticks.csv',
    '1101_2023-01-04_openprice.csv',
    '1101_2023-01-05_ticks.csv',
    '1101_2023-01-05_openprice.csv',
    '1101_2023-01-06_ticks.csv',
    '1101_2023-01-06_openprice.csv',
    '1101_2024-01-02_ticks.csv',
    '1101_2024-01-02_openprice.csv',
    '1314_2024-01-04_ticks.csv',
    '1314_2024-01-04_openprice.csv',
]

# 检查Z盘是否存在
if os.path.exists(z_drive_path):
    # 初始化变量
    data_dict = {}

    # 遍历Z盘中的所有文件和文件夹
    for root, dirs, files in os.walk(z_drive_path):
        for file in files:
            # if file.endswith('.csv'):
            if file in target_files:
                parts = file.split('_')
                stock_code = parts[0]
                date = parts[1]
                data_type = parts[2].replace('.csv', '')
                
                file_path = os.path.join(root, file)
                file_path = file_path.replace("\\", "/")
                
                # 读取CSV文件
                df = pd.read_csv(file_path)

                # 将DataFrame转换为字典
                data_records = df.to_dict('records')

                # 初始化 stock_code 字典
                if stock_code not in data_dict:
                    data_dict[stock_code] = {}

                # 初始化 date 字典
                if date not in data_dict[stock_code]:
                    data_dict[stock_code][date] = {
                        'ticks': [],
                        'openprice': []
                    }

                # 根据 data_type 存储数据
                if 'ticks' in data_type:
                    data_dict[stock_code][date]['ticks'].extend(data_records)
                elif 'openprice' in data_type:
                    data_dict[stock_code][date]['openprice'].extend(data_records)
                    
    # 将数据存储到 MongoDB
    for stock_code, stock_data in data_dict.items():
        collection = db[stock_code]
        # 为当前 collection 创建一个日期索引，确保按日期升序排列
        collection.create_index([("date", ASCENDING)])
        
        for date, date_data in stock_data.items():
            document = {"date": date}
            document.update(date_data)
            
            # 使用 update_one 进行插入或更新操作
            collection.update_one(
                {"date": date},  # 查询条件：匹配相同的日期
                {"$set": document},  # 如果找到相同日期的文档，更新其内容
                upsert=True  # 如果没有找到，插入新文档
            )

    print("数据已插入MongoDB")
else:
    print("Z盘不存在或无法访问")


# # 选择数据库和集合
# db = client['stock_data']
# collection = db['tick_data']

# def process_file(filepath, stock_code):
#     with open(filepath, 'r') as file:
#         data = file.read()
#         # 将数据存储到 MongoDB
#         collection.update_one(
#             {'_id': stock_code},
#             {'$set': {'data': data}},
#             upsert=True
#         )

# def traverse_directory(base_path):
#     for root, dirs, files in os.walk(base_path):
#         for file in files:
#             if 'ticks' in file:
#                 stock_code = file.split('_')[0]
#                 filepath = os.path.join(root, file)
#                 process_file(filepath, stock_code)
#                 print(f"Processed {filepath}")

# # 替换成您的数据目录路径
# base_path = 'path/to/your/directory'
# traverse_directory(base_path)

# print("All files processed.")


# # 創建新的資料庫
# new_db = client['my_new_database']  # 替換為您想要的資料庫名稱

# # 創建新的集合
# new_collection = new_db['my_new_collection']  # 替換為您想要的集合名稱

# # 插入一個文檔
# new_document = {"key": "value"}  # 替換為您想要插入的文檔
# result = new_collection.insert_one(new_document)

# print(f"Inserted document ID: {result.inserted_id}")


# # 使用指定的資料庫
# db = client['my_new_database']  # 替換為您的資料庫名稱

# # 獲取並列出所有集合名稱
# collections = db.list_collection_names()

# # 列印集合名稱
# print(f"{db.name} 中的集合:")
# for collection_name in collections:
#     print(collection_name)


# # 使用指定的資料庫
# db = client['my_new_database']  # 替換為您的資料庫名稱

# # 使用指定的集合
# collection = db['my_new_collection']  # 替換為您的集合名稱

# # 準備要插入的文檔
# new_document = {
#     "name": "John Doe",
#     "age": 30,
#     "email": "john.doe@example.com"
# }

# # 插入文檔到集合中
# result = collection.insert_one(new_document)

# # 輸出插入的文檔 ID
# print(f"Inserted document ID: {result.inserted_id}")


# # 使用特定資料庫
# db_name = 'my_new_database'  # 替換為您要使用的資料庫名稱
# db = client[db_name]

# # 使用特定集合
# collection_name = 'my_new_collection'  # 替換為您要使用的集合名稱
# collection = db[collection_name]

# # 查詢所有文檔
# documents = collection.find()

# # 列印每個文檔的內容
# print(f"{db_name} 中的 {collection_name} 的所有文檔:")
# for document in documents:
#     print(document)


# # 測試連接
# try:
#     # 列出資料庫
#     dbs = client.list_database_names()
#     print("Connected to MongoDB. Databases:")
#     print(dbs)
# except Exception as e:
#     print("Error connecting to MongoDB:", e)