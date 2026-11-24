import json
from pathlib import Path

######################  京东云 日志文件 留存地址  #############################
log_file_window_path = r"F:\QDatas\logs"
log_file_linux_path = r"/opt/Logs"
log_file_mac_path = r"/Users/wanghuihui/QuantsProject/Logs"


######################  个人 配置 留存地址  #############################
personal_window_path = r"F:\QDatas\ss_property"
personal_linux_path = r"/opt/ss_property"
personal_mac_path = r"/Users/wanghuihui/QuantsProject/ss_property"

personal_property_file = r"personal_property.txt"


from CommonProperties.set_config import read_personal_property
personal_property_dict = read_personal_property()

# 远程 mysql
origin_mysql_user = personal_property_dict["origin_mysql_user"]
origin_mysql_password = personal_property_dict["origin_mysql_password"]
origin_mysql_host = personal_property_dict["origin_mysql_host"]
origin_mysql_database = personal_property_dict["origin_mysql_database"]

# 本地 mysql
local_mysql_user = personal_property_dict["local_mysql_user"]
local_mysql_password = personal_property_dict["local_mysql_password"]
local_mysql_database = personal_property_dict["local_mysql_database"]
local_mysql_host = personal_property_dict["local_mysql_host"]

# tushare
ts_token = personal_property_dict["ts_token"]

# insight 账号
insight_user = personal_property_dict["insight_user"]
insight_password = personal_property_dict["insight_password"]


