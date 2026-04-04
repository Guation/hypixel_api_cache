#!/usr/bin/env python3
# -*- coding:utf-8 -*-

__author__ = "Guation"

import aiosqlite, aiohttp, asyncio, os, orjson, time, traceback, socket
from logging import debug, info, warning, error, basicConfig, DEBUG, INFO, WARN
from aiohttp import web
import uuid as uuid_lib
import zstandard as zstd

CCTX = zstd.ZstdCompressor(level=22)
DCTX = zstd.ZstdDecompressor()

class fetch_data_t():
    def __init__(self, uuid: str, user_name: str, user_uuid: str):
        self.uuid = uuid_lib.UUID(uuid)
        self.user_name = user_name
        self.user_uuid = uuid_lib.UUID(user_uuid)
        self.bytes = self.uuid.bytes

    def __str__(self):
        return str(self.uuid)

DB_PATH = 'hypixel_split.db'
FETCH_QUEUE: asyncio.Queue[fetch_data_t] = asyncio.Queue()

async def setup_database():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('PRAGMA journal_mode=WAL')
        await db.execute('PRAGMA synchronous=NORMAL')
        await db.execute('PRAGMA foreign_keys=ON')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS players (
                id INTEGER PRIMARY KEY,
                uuid BLOB(16) NOT NULL UNIQUE,
                expired INTEGER NOT NULL
            )
        ''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS player_data (
                id INTEGER PRIMARY KEY,
                data BLOB NOT NULL,
                FOREIGN KEY(id) REFERENCES players(id) ON DELETE CASCADE
            )
        ''')

        await db.commit()

async def http_get(url: str, headers = None):
    try:
        async with aiohttp.ClientSession(headers=headers, connector=aiohttp.TCPConnector(timeout_ceil_threshold=1, family=socket.AF_INET)) as session:
            async with session.get(url) as response:
                return response.status, await response.read(), response.headers
    except Exception:
        error(traceback.format_exc())
        return None, None

async def get_cached_expired(uuid: fetch_data_t) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT expired FROM players WHERE uuid = ?",
            (uuid.bytes,)
        )
        row = await cursor.fetchone()
        await cursor.close()

        if row:
            expired = row[0]
            return expired < time.time()

    return True

async def get_cached_data(uuid: fetch_data_t) -> tuple[bytes | None, bool]:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            SELECT d.data, p.expired
            FROM players p
            JOIN player_data d ON p.id = d.id
            WHERE p.uuid = ?
        """, (uuid.bytes,))

        row = await cursor.fetchone()
        await cursor.close()

        if row:
            data, expired = row
            return data, expired < time.time()

    return None, True

async def put_cached_data(uuid: fetch_data_t, data: bytes, expired: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA foreign_keys=ON")
        await db.execute("BEGIN")

        cursor = await db.execute("""
            INSERT INTO players (uuid, expired)
            VALUES (?, ?)
            ON CONFLICT(uuid) DO UPDATE SET expired = excluded.expired
            RETURNING id
        """, (uuid.bytes, int(time.time()) + expired))

        player_id = (await cursor.fetchone())[0]
        await cursor.close()

        await db.execute("""
            INSERT INTO player_data (id, data)
            VALUES (?, ?)
            ON CONFLICT(id) DO UPDATE SET data = excluded.data
        """, (player_id, data))

        await db.commit()

async def fetch_from_upstream(key: str):
    while True:
        try:
            uuid = await FETCH_QUEUE.get()
            if not await get_cached_expired(uuid):
                continue
            info("start query %s from %s(%s)", uuid, uuid.user_name, uuid.user_uuid)
            status, data, headers = await http_get(f"https://api.hypixel.net/v2/player?uuid={uuid}", headers={"API-Key": key})
            info("ratelimit-remaining: %s", headers.get("ratelimit-remaining"))
            if status == 200:
                data = orjson.loads(data)
                data["name"] = None if data["player"] == None else data["player"]["displayname"]
                data["timestamp"] = int(time.time())
                await put_cached_data(uuid, CCTX.compress(orjson.dumps(data)), 3600)
                info("player %s hypixel api name %s", uuid, data["name"])
                await asyncio.sleep(0.1)
            else:
                if status == 429:
                    await FETCH_QUEUE.put(uuid)
                    warning("server ratelimit info: %s", data)
                    warning("wait ratelimit-reset %s", headers.get("ratelimit-reset"))
                    await asyncio.sleep(int(headers.get("ratelimit-reset")) + 1)
                else:
                    error("player %s hypixel api fail [%s] [%s]", uuid, status, data)
                    await asyncio.sleep(10)
                continue
        except Exception:
            error(traceback.format_exc())
            await asyncio.sleep(60)

def get_fetch_data(request: web.Request):
    try:
        return None, fetch_data_t(request.match_info['uuid'], request.headers["User-Name"], request.headers['User-Uuid'])
    except ValueError:
        return web.Response(
            text='{"error": "Invalid UUID format"}',
            status=400,
            content_type='application/json'
        ), None
    except (AttributeError, KeyError):
        return web.Response(
            text='{"error": "Incomplete request"}',
            status=400,
            content_type='application/json'
        ), None

def send_player_data(cached_data: bytes, expired: bool, uuid: fetch_data_t):
    try:
        return web.Response(
            body=DCTX.decompress(cached_data),
            content_type='application/json',
            headers={"expired": str(expired)}
        )
    except Exception:
        error("Decompress player(%s) data fail", uuid, stack_info=True)
        return web.Response(
            text='{"error": "Decompress player data fail"}',
            status=403,
            content_type='application/json'
        )

async def key_handle(request: web.Request):
    response, uuid = get_fetch_data(request)
    if response:
        return response
    cached_data, expired = await get_cached_data(uuid)
    if expired and request.headers.get("Protocol-Version") == "20251018":
        await FETCH_QUEUE.put(uuid)
    if cached_data:
        return send_player_data(cached_data, expired, uuid)
    else:
        return web.Response(
            text='{"error": "Waiting for data"}',
            status=202,
            content_type='application/json'
        )

async def nokey_handle(request: web.Request):
    response, uuid = get_fetch_data(request)
    if response:
        return response
    cached_data, expired = await get_cached_data(uuid)
    if cached_data:
        return send_player_data(cached_data, False, uuid)
    else:
        return web.Response(
            text='{"error": "No API key configured"}',
            status=403,
            content_type='application/json'
        )

async def root_handle(request: web.Request):
    return web.Response(
        body=orjson.dumps({"timestamp": int(time.time())}),
        content_type='application/json'
    )

async def create_app(key: str):
    if not os.path.isfile(DB_PATH):
        await setup_database()
    app = web.Application()
    if key:
        app.router.add_get('/{uuid}', key_handle)
    else:
        app.router.add_get('/{uuid}', nokey_handle)
    app.router.add_get('/', root_handle)
    return app

def main():
    basicConfig(
        level=INFO,
        format='[%(levelname)8s | %(asctime)s] %(message)s')
    api_key = os.environ.get("HYPIXEL")
    if not api_key:
        warning("No API key configured")
    app = asyncio.run(create_app(api_key))
    loop = asyncio.new_event_loop()
    if api_key:
        loop.create_task(fetch_from_upstream(api_key))
    web.run_app(app, host='127.0.0.1', port=8001, loop=loop, access_log=None)

if __name__ == '__main__':
    main()
