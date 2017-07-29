from sql.dbHandle import *
db = dbHandle('test.sqlite') #連線這個檔案, 如果不存在就新增空的
# resetTable(db.cur_db)  # 這句會刪掉SQL內所有資料
db.insertDB() #把資料夾內所有檔案餵進去SQL