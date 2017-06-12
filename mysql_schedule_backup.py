#! /usr/bin/env python
# -*-coding:utf-8 -*-

import datetime
import json
import os
import shutil
import time
import zipfile

import schedule

import conf


def delete_outdate_file(path, keep_day, end_date, file_type):
    """
    删除过期的文件 时间以文件的创建日期为准
    :param path: 文件或者文件夹的路径
    :param keep_day: 文件保存的时间
    :param end_date: 日期截止时间
    :param file_type: 文件类型 支持*表示全部
    :return: 
    """
    if os.path.isdir(path):
        # 遍历文件夹 判断创建时间是否过期 然后删除
        # todo 递归的文件夹 只会删除文件夹下面的文件 文件夹没有删除
        for dirpath, _, filenames in os.walk(path):

            # 过滤要删除的文件类型
            if file_type != "*":
                filenames = filter(lambda x: x.endswith(file_type), filenames)

            for filename in filenames:
                file_path = os.path.join(dirpath, filename)

                file_crt_date = datetime.datetime.fromtimestamp(os.path.getctime(file_path))
                if is_outdate(file_crt_date, keep_day, end_date):
                    delete_exists(file_path)

    elif os.path.isfile(path):
        # 删除创建日期过期文件
        file_crt_date = datetime.datetime.fromtimestamp(os.path.getctime(path))
        if is_outdate(file_crt_date, keep_day, end_date):
            delete_exists(path)


def is_outdate(check_day, keep_day, end_day):
    """
    判断日期是否过期 过期返回True 没有过期返回False
    :param check_day: 要校验的日期
    :param keep_day: 保留的天数
    :param end_day: 开始计算日期的天数
    :return: True or False
    """
    if isinstance(end_day, datetime.datetime) and isinstance(check_day, datetime.datetime):
        delta = end_day - check_day
        return delta.days >= keep_day


def zip_file_folder(src_path, tgt_path, file_name):
    """
    压缩文件或文件夹 将src_path压缩成tgt_path/file_name形式
    :param src_path: 要压缩的文件夹或文件路径
    :param tgt_path: 目标文件夹路径
    :param file_name: 目标文件名
    :return: 
    """
    with zipfile.ZipFile(os.path.join(tgt_path, file_name), 'w', zipfile.ZIP_DEFLATED) as zip_obj:
        if os.path.isdir(src_path):
            for dirpath, _, filenames in os.walk(src_path):
                for filename in filenames:
                    zip_obj.write(os.path.join(dirpath, filename), filename)
        elif os.path.isfile(src_path):
            zip_obj.write(src_path, os.path.basename(src_path))


def create_not_exists(path):
    """
    确保文件夹存在 如果不存在新建一个空文件夹
    :param path: 文件夹路径
    :return: 
    """
    if not os.path.exists(path):
        os.makedirs(path)


def delete_exists(path):
    """
    确保文件夹不存在 存在的话删除该文件夹
    :param path: 文件夹路径
    :return: 
    """
    if os.path.isfile(path):
        os.remove(path)
    elif os.path.isdir(path):
        shutil.rmtree(path)


def wrt_conf_file(path, file_name, content):
    """
    将配置文件信息写入指定路径
    :param path: 生成文件路径
    :param file_name: 生成文件名
    :param content: 文件内容
    :return: 
    """
    with open(os.path.join(path, file_name), "w") as wrt:
        wrt.write(json.dumps(content, indent=4))


def run_mysqldump(usr, pwd, host, port, db_name, table_name, backup_folder, backup_day):
    """
    运行mysqldump命令
    :param usr: 用户名
    :param pwd: 密码
    :param db_name: 备份数据库名
    :param table_name: 表名
    :param backup_folder: 备份文件夹名
    :param backup_day: 备份日期
    :return: 
    """
    backup_path = "{backup_folder}/{backup_day}_{db_name}.sql" \
        .format(backup_folder=backup_folder, backup_day=backup_day, db_name=db_name)
    command = "mysqldump -u{usr} -p{pwd} -h{host} -P{port} {db_name} {table_name} > {backup_path}" \
        .format(usr=usr, pwd=pwd, host=host, port=port, db_name=db_name, table_name=table_name, backup_path=backup_path)
    os.system(command)


def backup_mysql(conf):
    """
    备份mysql数据
    :param conf: 备份的配置文件
    :return: 
    """

    setting = conf["setting"]
    connection = conf["db_connection"]

    root = setting["root"]
    keep_day = setting["keep_day"]
    is_zip = setting["is_zip"]

    # 获取备份文件夹路径
    backup_day = datetime.datetime.now().strftime("%Y%m%d")
    backup_folder = os.path.join(root, backup_day)

    # 判断是否存在并创建
    create_not_exists(backup_folder)

    # 写入配置文件
    wrt_conf_file(backup_folder, "conf.json", bacup_conf)

    # 遍历配置文件 进行备份
    for sub_job in connection:
        usr = sub_job["usr"]
        pwd = sub_job["pwd"]
        host = sub_job["host"]
        port = sub_job["port"]
        db_and_table = sub_job["db_and_table"]

        # 运行mysqldump备份数据
        for db_name in db_and_table.iterkeys():
            table_list = db_and_table[db_name]
            table_name = " ".join(table_list) if table_list != ["*"] else ""
            run_mysqldump(usr, pwd, host, port, db_name, table_name, backup_folder, backup_day)

    # 根据配置是否压缩
    if is_zip == "true":
        zip_file_folder(backup_folder, root, "{backup_day}.zip".format(backup_day=backup_day))
        delete_exists(backup_folder)

    # 删除过期的文件
    delete_outdate_file(root, keep_day, datetime.datetime.strptime(backup_day, "%Y%m%d"), ".zip")


if __name__ == "__main__":
    # 从配置文件中读取配置信息
    bacup_conf = conf.job

    SLEEP_TIME = 30
    # 测试定时运行程序 每分钟运行一次
    # schedule.every().minutes.do(backup_mysql, bacup_conf)
    # 每天固定时间运行程序
    schedule.every().day.at("06:00").do(backup_mysql, bacup_conf)

    while True:
        schedule.run_pending()
        time.sleep(SLEEP_TIME)
