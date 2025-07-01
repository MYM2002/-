1.配置环境 pip install selenium webdriver-manager psutil mysql-connector-python
2.配置数据库 将源代码文件夹中的数据库源代码.txt中代码复制到mysql中运行，然后进行数据库连接配置
# 数据库连接配置
        self.db_config = {
            'host': 'localhost',
            'user': 'your_username',
            'password': 'your_password',
            'database': 'douban_movie_db',
            'charset': 'utf8mb4' 
        }
3.在项目目录下运行 app.py（一定要先运行再打开前端页面）
4.打开http://127.0.0.1:5000（前端页面），确定要爬取的电影数量
5.等待爬取完成，或者主动点击停止爬取
6.在数据库中查看爬取结果或者导出csv文件进行查看