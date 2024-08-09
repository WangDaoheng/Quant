import sys
import time
import logging



if __name__ =="__main__":


    # 设置基本的日志配置
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    # 总循环次数
    total_loops = 50

    for i in range(1, total_loops + 1):
        # 模拟一些工作
        time.sleep(0.1)

        # 打印进度日志，使用 \r 将光标返回到行首
        sys.stdout.write(f"\r当前是第 {i} 次循环，总共 {total_loops} 次循环")
        sys.stdout.flush()

    # 循环结束后打印换行符，以确保后续输出在新行开始
    sys.stdout.write("\n")

    # 记录循环完成的日志
    logging.info("循环已完成。")

















