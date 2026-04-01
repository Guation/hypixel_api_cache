#!/usr/bin/env python3
# -*- coding:utf-8 -*-

__author__ = "Guation"

import aiosqlite, asyncio, os, time
from logging import debug, info, warning, error, basicConfig, DEBUG, INFO, WARN

OLD_DB_PATH = 'hypixel_split.db.old'
NEW_DB_PATH = 'hypixel_split.db'

async def migrate():
    if os.path.isfile(OLD_DB_PATH):
        error("已存在重建痕迹， 请删除上次备份的旧数据库 %s 或将其覆盖 %s 以重新运行重建任务", OLD_DB_PATH, NEW_DB_PATH)
        return
    if not os.path.isfile(NEW_DB_PATH):
        error("数据库 %s 不存在， 无法重建", NEW_DB_PATH)
        return
    os.rename(NEW_DB_PATH, OLD_DB_PATH)
    async with aiosqlite.connect(OLD_DB_PATH) as old_db, aiosqlite.connect(NEW_DB_PATH) as new_db:
        await new_db.execute('PRAGMA journal_mode=WAL')
        await new_db.execute('PRAGMA synchronous=NORMAL')
        await new_db.execute('PRAGMA foreign_keys=ON')

        await new_db.execute('''
            CREATE TABLE IF NOT EXISTS players (
                id INTEGER PRIMARY KEY,
                uuid BLOB(16) NOT NULL UNIQUE,
                expired INTEGER NOT NULL
            )
        ''')

        await new_db.execute('''
            CREATE TABLE IF NOT EXISTS player_data (
                id INTEGER PRIMARY KEY,
                data BLOB NOT NULL,
                FOREIGN KEY(id) REFERENCES players(id) ON DELETE CASCADE
            )
        ''')

        await new_db.commit()

        counter = 0
        start_time = time.perf_counter()

        async with old_db.execute("SELECT id, uuid, expired FROM players") as cursor:
            async for row in cursor:
                counter += 1
                await new_db.execute(
                    "INSERT INTO players (id, uuid, expired) VALUES (?, ?, ?)",
                    row
                )
                if counter % 1000 == 0:
                    info("表1已迁移 %d 已耗时 %fs", counter, time.perf_counter() - start_time)
        counter = 0
        async with old_db.execute("SELECT id, data FROM player_data") as cursor:
            async for row in cursor:
                counter += 1
                await new_db.execute(
                    "INSERT INTO player_data (id, data) VALUES (?, ?)",
                    row
                )
                if counter % 1000 == 0:
                    info("表2已迁移 %d 已耗时 %fs", counter, time.perf_counter() - start_time)
        await new_db.commit()
        info("总共迁移 %d 总耗时 %fs", counter, time.perf_counter() - start_time)

def main():
    basicConfig(
        level=INFO,
        format='[%(levelname)8s | %(asctime)s] %(message)s')
    asyncio.run(migrate())

if __name__ == '__main__':
    main()
