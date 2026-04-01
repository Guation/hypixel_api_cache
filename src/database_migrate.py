#!/usr/bin/env python3
# -*- coding:utf-8 -*-

__author__ = "Guation"

import aiosqlite, asyncio, os, time
from logging import debug, info, warning, error, basicConfig, DEBUG, INFO, WARN
import zstandard as zstd
import uuid as uuid_lib

CCTX = zstd.ZstdCompressor(level=22)
DCTX = zstd.ZstdDecompressor()

OLD1_DB_PATH = 'hypixel.db'
OLD2_DB_PATH = 'hypixel_zstd.db'
NEW_DB_PATH = 'hypixel_split.db'

async def migrate():
    if not os.path.isfile(OLD1_DB_PATH) and not os.path.isfile(OLD2_DB_PATH):
        error("旧表不存在， 无法迁移")
        return
    if os.path.isfile(NEW_DB_PATH):
        error("新表已存在， 无法迁移")
        return
    async with aiosqlite.connect(NEW_DB_PATH) as new_db:
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
        if os.path.isfile(OLD1_DB_PATH):
            async with aiosqlite.connect(OLD1_DB_PATH) as old_db:
                counter = 0
                start_time = time.perf_counter()

                async with old_db.execute("SELECT uuid, data, expired FROM player_cache") as old_cursor:
                    async for uuid, data, expired in old_cursor:
                        counter += 1
                        new_cursor = await new_db.execute("""
                            INSERT INTO players (uuid, expired)
                            VALUES (?, ?)
                            ON CONFLICT(uuid) DO UPDATE SET expired = excluded.expired
                            RETURNING id
                        """, (uuid_lib.UUID(uuid).bytes, expired))

                        player_id = (await new_cursor.fetchone())[0]
                        await new_cursor.close()

                        await new_db.execute("""
                            INSERT INTO player_data (id, data)
                            VALUES (?, ?)
                            ON CONFLICT(id) DO UPDATE SET data = excluded.data
                        """, (player_id, CCTX.compress(data.encode("utf-8"))))
                        if counter % 1000 == 0:
                            info("已迁移 %d 已耗时 %fs", counter, time.perf_counter() - start_time)
                await new_db.commit()
                info("总共迁移 %d 总耗时 %fs", counter, time.perf_counter() - start_time)
        elif os.path.isfile(OLD2_DB_PATH):
            async with aiosqlite.connect(OLD2_DB_PATH) as old_db:
                counter = 0
                start_time = time.perf_counter()

                async with old_db.execute("SELECT uuid, data, expired FROM player_cache") as old_cursor:
                    async for uuid, data, expired in old_cursor:
                        counter += 1
                        new_cursor = await new_db.execute("""
                            INSERT INTO players (uuid, expired)
                            VALUES (?, ?)
                            ON CONFLICT(uuid) DO UPDATE SET expired = excluded.expired
                            RETURNING id
                        """, (uuid_lib.UUID(uuid).bytes, expired))

                        player_id = (await new_cursor.fetchone())[0]
                        await new_cursor.close()

                        await new_db.execute("""
                            INSERT INTO player_data (id, data)
                            VALUES (?, ?)
                            ON CONFLICT(id) DO UPDATE SET data = excluded.data
                        """, (player_id, data))
                        if counter % 1000 == 0:
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
