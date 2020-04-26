# coding:utf8
"""
file name: log_config.py
date: 2020/3/24
author: huangxuanfeng
"""
import sys, os
import time, datetime

reload(sys)
sys.setdefaultencoding('utf-8')

import logging


def get_log_session(param_log_path=None):
    # logging.basicConfig函数对日志的输出格式及方式做相关配置
    # logging.basicConfig(level=logging.DEBUG,
    #                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

    logging.basicConfig(filename=param_log_path + '/metahub' + '.log',
                        format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                        level=logging.DEBUG,
                        filemode='a',
                        datefmt='%Y%m%d-%H'
    )
    return logging

    # # 第一步，创建一个logger
    # logger = logging.getLogger()
    # logger.setLevel(logging.DEBUG)  # Log等级总开关
    #
    # # 第二步，创建一个handler，用于写入日志文件
    # rq = time.strftime('%Y%m%d%H', time.localtime(time.time()))
    # log_path = os.path.dirname(os.getcwd()) + '/metahub-logs/'
    # if param_log_path != None:
    #     log_path = param_log_path + '/metahub-logs/'
    # log_name = log_path + rq + '.log'
    #
    # fh = logging.FileHandler(log_name, mode='a')
    # fh.setLevel(logging.DEBUG)  # 输出到file的log等级的开关
    #
    # # 第三步，定义handler的输出格式
    # formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
    # fh.setFormatter(formatter)
    #
    # # 第四步，将handler添加到logger里面
    # logger.addHandler(fh)

    # return logger

