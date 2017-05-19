@echo off
echo -------------長期且持續的累積-------------

REM 來源路徑
REM 查詢股號
REM 目的檔名
REM 開始日期
REM 結束日期
REM 間隔時間

C:\ENV_PY34\Scripts\python.exe .\籌碼分析.py^
 "..\\籌碼資料\\"^
 "2330台積電"^
 "2330台積電籌碼整理"^
 "20170505"^
 "20170511"^
 1^
 3530000000^
 
REM C:\ENV_PY34\Scripts\python.exe .\籌碼分析.py^
REM "..\\籌碼資料\\"^
REM "1723中碳"^
REM "1723中碳籌碼整理"^
REM "20170410"^
REM "2017014"^
REM 1^
REM 3530000000
 
REM C:\ENV_PY34\Scripts\python.exe .\籌碼分析.py^
REM "..\\籌碼資料\\"^
REM "1218泰山"^
REM "1218泰山籌碼整理1214_0108"^
REM "20161214"^
REM "20170108"^
REM 3^
REM 3530000000
echo 轉換完成

pause
