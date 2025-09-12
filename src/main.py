#!/usr/bin/env python3
# -*- coding:utf-8 -*-

__author__ = "Guation"

import aiosqlite, aiohttp, asyncio, os, json, time, traceback
from logging import debug, info, warning, error, basicConfig, DEBUG, INFO, WARN
from aiohttp import web
import uuid as uuid_lib

DB_PATH = 'hypixel.db'
FETCH_QUEUE = asyncio.Queue()

async def setup_database():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('PRAGMA journal_mode=WAL')
        await db.execute('PRAGMA synchronous=NORMAL')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS player_cache (
                uuid TEXT PRIMARY KEY,
                data TEXT NOT NULL,
                expired INTEGER NOT NULL
            )
        ''')
        await db.commit()

async def http_get(url: str, headers = None):
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(url) as response:
                return response.status, await response.text()
    except Exception:
        error(traceback.format_exc())
        return None, None

async def get_cached_data(uuid: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT data, expired FROM player_cache WHERE uuid = ?",
            (uuid,)
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row:
            data, expired = row
            return data, bool(expired < int(time.time()))
    return None, True

async def put_cached_data(uuid: str, data: str, expired: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO player_cache (uuid, data, expired) VALUES (?, ?, ?)",
            (uuid, data, int(time.time()) + expired)
        )
        await db.commit()

async def fetch_from_upstream(key: str):
    while True:
        try:
            uuid = await FETCH_QUEUE.get()
            if not (await get_cached_data(uuid))[1]:
                warning("continue")
                continue
            warning("start query %s", uuid)
            status, data = await http_get(f"https://api.ashcon.app/mojang/v2/user/{uuid}")
            if status == 404 or status == 400:
                await put_cached_data(uuid, json.dumps({"name": None, "timestamp": int(time.time())}), 1800)
                error("player %s not found [%s]", uuid, status)
                continue
            elif status == 200:
                data = json.loads(data)
                player_name = data["username"]
                warning("player %s name %s", uuid, player_name)
            else:
                if status != 429:
                    error("player %s mojang api error [%s]", uuid, status)
                else:
                    await FETCH_QUEUE.put(uuid)
                await asyncio.sleep(1)
                continue
            status, data = await http_get(f"https://api.hypixel.net/v2/player?uuid={uuid}", headers={"API-Key": key})
            if status == 200:
                data = json.loads(data)
                data["name"] = player_name
                data["timestamp"] = int(time.time())
                await put_cached_data(uuid, json.dumps(data), 1800)
                if data["player"] != None:
                    warning("player %s hypixel api name %s", uuid, data["player"]["displayname"])
                else:
                    warning("player %s hypixel api name %s", uuid, None)
            else:
                if status != 429:
                    error("player %s hypixel api fail [%s]", uuid, status)
                else:
                    await FETCH_QUEUE.put(uuid)
                await asyncio.sleep(1)
                continue
        except Exception:
            error(traceback.format_exc())

async def handle_request(request: web.Request):
    try:
        uuid = str(uuid_lib.UUID(request.match_info.get('uuid')))
    except (ValueError, AttributeError):
        return web.Response(
            text='{"error": "Invalid UUID format"}',
            status=400,
            content_type='application/json'
        )
    
    cached_data, expired = await get_cached_data(uuid)
    if expired:
        await FETCH_QUEUE.put(uuid)
    if cached_data:
        return web.Response(
            text=cached_data,
            content_type='application/json'
        )
    else:
        return web.Response(
            text='{"error": "Waiting for data"}',
            status=202,
            content_type='application/json'
        )

async def create_app():
    if not os.path.isfile(DB_PATH):
        await setup_database()
    app = web.Application()
    app.router.add_get('/{uuid}', handle_request)
    return app

def main():
    basicConfig(
        level=WARN,
        format='[%(levelname)8s] %(asctime)s <%(module)s.%(funcName)s>:%(lineno)d\n[%(levelname)8s] %(message)s')
    app = asyncio.run(create_app())
    loop = asyncio.new_event_loop()
    loop.create_task(fetch_from_upstream(os.environ["HYPIXEL"]))
    web.run_app(app, host='127.0.0.1', port=8001, loop=loop)

if __name__ == '__main__':
    main()
