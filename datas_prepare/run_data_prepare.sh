#!/bin/bash
# 定义日志文件名（格式：run_log_YYYY-MM-DD.txt）
LOG_FILE="/opt/run_logs/run_log_$(date +%Y-%m-%d).txt"
# 执行Python脚本，并将输出（stdout和stderr）写入日志文件
python3 /opt/Quant/datas_prepare/setup_data_prepare.py >> "$LOG_FILE" 2>&1