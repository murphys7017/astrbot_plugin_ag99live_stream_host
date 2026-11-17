"""Loopback acceptance of the real blivedm client with modern HTTP dependencies."""

import asyncio
import json
import struct
import brotli
import blivedm
from aiohttp import ClientSession, CookieJar, web
from blivedm.clients import web as client_web


def packet(body, operation=5, version=0):
    return struct.pack(">I2H2I", 16 + len(body), 16, version, operation, 1) + body


async def main():
    connections = 0
    received = []
    done = asyncio.Event()

    async def api(request):
        path = request.path
        if path == "/nav":
            data = {
                "wbi_img": {
                    "img_url": "https://test/" + "a" * 32 + ".png",
                    "sub_url": "https://test/" + "b" * 32 + ".png",
                }
            }
        elif path == "/room":
            data = {"room_id": 1, "uid": 2}
        else:
            data = {
                "host_list": [{"host": "127.0.0.1", "wss_port": 443}],
                "token": "local-test",
            }
        response = web.Response(
            body=brotli.compress(json.dumps({"code": 0, "data": data}).encode()),
            headers={"Content-Encoding": "br", "Content-Type": "application/json"},
        )
        if path == "/buvid":
            response.set_cookie("buvid3", "loopback-client")
        return response

    async def websocket(request):
        nonlocal connections
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        connections += 1
        await ws.receive()  # real client's AUTH packet
        await ws.send_bytes(packet(b'{"code":0}', 8, 1))
        inner = packet(
            json.dumps({"cmd": "COMPAT_CHECK", "connection": connections}).encode()
        )
        await ws.send_bytes(packet(brotli.compress(inner), version=3))
        if connections == 1:
            await ws.close()  # exercise the client's reconnect path
        else:
            async for _ in ws:
                pass
        return ws

    app = web.Application()
    app.router.add_get("/sub", websocket)
    app.router.add_get("/{name}", api)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    base = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
    client_web.WBI_INIT_URL = base + "/nav"
    client_web.ROOM_INIT_URL = base + "/room"
    client_web.DANMAKU_SERVER_CONF_URL = base + "/conf"
    client_web.BUVID_INIT_URL = base + "/buvid"

    class LocalClient(blivedm.BLiveClient):
        def _get_ws_url(self, retry_count):
            return base.replace("http:", "ws:") + "/sub"

    class Handler(blivedm.HandlerInterface):
        def handle(self, client, command):
            if command.get("cmd") == "COMPAT_CHECK":
                received.append(command["connection"])
                if len(received) == 2:
                    done.set()

    try:
        async with ClientSession(cookie_jar=CookieJar(unsafe=True)) as session:
            client = LocalClient(1, session=session)
            client.set_reconnect_policy(lambda *_: 0.05)
            client.set_handler(Handler())
            client.start()
            try:
                await asyncio.wait_for(done.wait(), 10)
                assert received == [1, 2], received
                print(
                    "PASS: br JSON room initialization, WebSocket auth, Brotli messages, reconnect"
                )
            finally:
                await client.stop_and_close()
    finally:
        await runner.cleanup()


asyncio.run(main())
