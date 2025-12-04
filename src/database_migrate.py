#!/usr/bin/env python3
# -*- coding:utf-8 -*-

__author__ = "Guation"

import aiosqlite, asyncio, os, json, time, traceback, ssl, socket
from logging import debug, info, warning, error, basicConfig, DEBUG, INFO, WARN
import uuid as uuid_lib
import zstandard as zstd

CCTX = zstd.ZstdCompressor(level=22)
DCTX = zstd.ZstdDecompressor()

OLD_DB_PATH = 'hypixel.db'
NEW_DB_PATH = 'hypixel_zstd.db'

async def migrate():
    if not os.path.isfile(OLD_DB_PATH):
        error("旧表不存在， 无法迁移")
        return
    if os.path.isfile(NEW_DB_PATH):
        error("新表已存在， 无法迁移")
        return
    async with aiosqlite.connect(OLD_DB_PATH) as old_db:
        async with aiosqlite.connect(NEW_DB_PATH) as new_db:
            await new_db.execute('PRAGMA journal_mode=WAL')
            await new_db.execute('PRAGMA synchronous=NORMAL')
            await new_db.execute('''
                CREATE TABLE IF NOT EXISTS player_cache (
                    uuid TEXT PRIMARY KEY,
                    data BLOB NOT NULL,
                    expired INTEGER NOT NULL
                )
            ''')
            await new_db.commit()

            counter = 0
            start_time = time.perf_counter()

            async with old_db.execute("SELECT uuid, data, expired FROM player_cache") as cursor:
                async for uuid, data_json, expired in cursor:
                    counter += 1
                    compressed = CCTX.compress(data_json.encode("utf-8"))
                    await new_db.execute(
                        "INSERT OR REPLACE INTO player_cache (uuid, data, expired) VALUES (?, ?, ?)",
                        (uuid, compressed, expired)
                    )
                    if counter % 100 == 0:
                        info("已迁移 %d 已耗时 %fs", counter, time.perf_counter() - start_time)
            await new_db.commit()
            info("总共迁移 %d 总耗时 %fs", counter, time.perf_counter() - start_time)

def main():
    basicConfig(
        level=INFO,
        format='[%(levelname)8s | %(asctime)s] %(message)s')
    asyncio.run(migrate())

if __name__ == '__main__':
    main()
