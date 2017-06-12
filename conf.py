#!/usr/bin/env python
# -*- coding:utf-8 -*-

job = {
    "setting": {
        # 备份的根目录
        "root": "F:/mysql_schedule_backup",
        # 备份保持的时间
        "keep_day": 60,
        # 是否压缩备份
        "is_zip": "true"
    },
    "db_connection": [
        # 备份的第一个数据库
        {
            "usr": "root",
            "pwd": "mysql",
            "host": "127.0.0.1",
            "port": "3306",
            "db_and_table": {
                # "backup_database": ["backup_table_1", "backup_table_2", ...]
                "tmp": [
                    "tb1",
                    "tb2"
                ],
                # "backup_database": ["*"]   # *默认全库备份
                "world": [
                    "*"
                ]
            }
        },
        # 备份第二个数据库
        {
            "usr": "root",
            "pwd": "mysql",
            "host": "127.0.0.1",
            "port": "3306",
            "db_and_table": {
                # "backup_database": ["backup_table_1", "backup_table_2", ...]
                "sakila": [
                    "*"
                ]
            }
        }
    ]
}