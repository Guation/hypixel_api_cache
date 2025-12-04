#!/usr/bin/env python3
# -*- coding:utf-8 -*-

__author__ = "Guation"

import aiosqlite, aiohttp, asyncio, os, json, time, traceback, socket
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

    def __str__(self):
        return str(self.uuid)

DB_PATH = 'hypixel_zstd.db'
FETCH_QUEUE: asyncio.Queue[fetch_data_t] = asyncio.Queue()

async def setup_database():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('PRAGMA journal_mode=WAL')
        await db.execute('PRAGMA synchronous=NORMAL')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS player_cache (
                uuid TEXT PRIMARY KEY,
                data BLOB NOT NULL,
                expired INTEGER NOT NULL
            )
        ''')
        await db.commit()

async def http_get(url: str, headers = None):
    try:
        async with aiohttp.ClientSession(headers=headers, connector=aiohttp.TCPConnector(timeout_ceil_threshold=1, family=socket.AF_INET)) as session:
            async with session.get(url) as response:
                return response.status, await response.text(), response.headers
    except Exception:
        error(traceback.format_exc())
        return None, None

async def get_cached_data(uuid: fetch_data_t) -> tuple[bytes | None, bool]:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT data, expired FROM player_cache WHERE uuid = ?",
            (str(uuid),)
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row:
            data, expired = row
            return data, expired < time.time()
    return None, True

async def put_cached_data(uuid: fetch_data_t, data: bytes, expired: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO player_cache (uuid, data, expired) VALUES (?, ?, ?)",
            (str(uuid), data, int(time.time()) + expired)
        )
        await db.commit()

async def fetch_from_upstream(key: str):
    while True:
        try:
            uuid = await FETCH_QUEUE.get()
            if not (await get_cached_data(uuid))[1]:
                continue
            info("start query %s from %s(%s)", uuid, uuid.user_name, uuid.user_uuid)
            status, data, headers = await http_get(f"https://api.hypixel.net/v2/player?uuid={uuid}", headers={"API-Key": key})
            info("ratelimit-remaining: %s", headers.get("ratelimit-remaining"))
            if status == 200:
                data = json.loads(data)
                data["name"] = None if data["player"] == None else data["player"]["displayname"]
                data["timestamp"] = int(time.time())
                await put_cached_data(uuid, CCTX.compress(json.dumps(data).encode("utf-8")), 3600)
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

async def handle_request(request: web.Request, key: str):
    try:
        uuid = fetch_data_t(request.match_info['uuid'], request.headers["User-Name"], request.headers['User-Uuid'])
    except ValueError:
        return web.Response(
            text='{"error": "Invalid UUID format"}',
            status=400,
            content_type='application/json'
        )
    except (AttributeError, KeyError):
        return web.Response(
            text='{"error": "Incomplete request"}',
            status=400,
            content_type='application/json'
        )
    
    cached_data, expired = await get_cached_data(uuid)
    if expired and request.headers.get("Protocol-Version") == "20251018" and key:
        await FETCH_QUEUE.put(uuid)
    if cached_data:
        try:
            return web.Response(
                text=DCTX.decompress(cached_data).decode("utf-8"),
                content_type='application/json'
            )
        except Exception:
            error("Decompress player(%s) data fail", uuid, stack_info=True)
            return web.Response(
                text='{"error": "Decompress player data fail"}',
                status=403,
                content_type='application/json'
            )
    elif key:
        return web.Response(
            text='{"error": "Waiting for data"}',
            status=202,
            content_type='application/json'
        )
    else:
        return web.Response(
            text='{"error": "No API key configured"}',
            status=403,
            content_type='application/json'
        )

async def create_app(key: str):
    if not os.path.isfile(DB_PATH):
        await setup_database()
    app = web.Application()
    async def handler(request):
        return await handle_request(request, key)
    app.router.add_get('/{uuid}', handler)
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
    loop.create_task(fetch_from_upstream(api_key))
    web.run_app(app, host='127.0.0.1', port=8001, loop=loop, access_log=None)

if __name__ == '__main__':
    main()
