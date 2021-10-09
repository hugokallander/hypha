"""Provide the server."""
import argparse
import asyncio
import os
from contextvars import copy_context
from os import environ as env
from typing import Union

import shortuuid
import socketio
import uvicorn
from dotenv import find_dotenv, load_dotenv
from fastapi import FastAPI
from fastapi.logger import logger
from fastapi.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from imjoy import __version__ as VERSION
from imjoy.core import EventBus, UserInfo, VisibilityEnum, WorkspaceInfo
from imjoy.core.auth import parse_token
from imjoy.core.connection import BasicConnection
from imjoy.core.interface import CoreInterface
from imjoy.core.plugin import DynamicPlugin
from imjoy.http import HTTPProxy

ENV_FILE = find_dotenv()
if ENV_FILE:
    load_dotenv(ENV_FILE)


def initialize_socketio(sio, core_interface, bus: EventBus):
    """Initialize socketio."""
    # pylint: disable=too-many-statements, unused-variable

    @sio.event
    async def connect(sid, environ):
        """Handle event called when a socketio client is connected to the server."""
        if "HTTP_AUTHORIZATION" in environ:
            try:
                authorization = environ["HTTP_AUTHORIZATION"]  # JWT token
                user_info = parse_token(authorization)
                uid = user_info.id
            except Exception as err:  # pylint: disable=broad-except
                logger.exception("Authentication failed: %s", err)
                # The connect event handler can return False
                # to reject the connection with the client.
                return False
            logger.info("User connected: %s", uid)
        else:
            uid = shortuuid.uuid()
            user_info = UserInfo(
                id=uid,
                is_anonymous=True,
                email=None,
                parent=None,
                roles=[],
                scopes=[],
                expires_at=None,
            )
            logger.info("Anonymized User connected: %s", uid)

        if uid == "root":
            logger.info("Root user is not allowed to connect remotely")
            return False

        if uid not in core_interface.all_users:
            core_interface.all_users[uid] = user_info
        core_interface.all_users[uid].add_session(sid)
        core_interface.all_sessions[sid] = core_interface.all_users[uid]
        bus.emit("user_connected", core_interface.all_users[uid])

    @sio.event
    async def echo(sid, data):
        """Echo service for testing."""
        return data

    @sio.event
    async def register_plugin(sid, config):
        user_info = core_interface.all_sessions[sid]
        ws = config.get("workspace") or user_info.id
        config["workspace"] = ws
        config["name"] = config.get("name") or shortuuid.uuid()
        workspace = core_interface.get_workspace(ws)
        if workspace is None:
            if ws == user_info.id:
                # only registered user can have persistent workspace
                persistent = not user_info.is_anonymous
                # create the user workspace automatically
                workspace = WorkspaceInfo(
                    name=ws,
                    owners=[user_info.id],
                    visibility=VisibilityEnum.protected,
                    persistent=persistent,
                )
                core_interface.register_workspace(workspace)
            else:
                return {"success": False, "detail": f"Workspace {ws} does not exist."}

        if user_info.id != ws and not core_interface.check_permission(
            workspace, user_info
        ):
            return {
                "success": False,
                "detail": f"Permission denied for workspace: {ws}",
            }

        name = config["name"].replace("/", "-")  # prevent hacking of the plugin name
        plugin_id = f"{ws}/{name}"
        config["id"] = plugin_id
        sio.enter_room(sid, plugin_id)

        async def send(data):
            await sio.emit(
                "plugin_message",
                data,
                room=plugin_id,
            )

        connection = BasicConnection(send)
        plugin = DynamicPlugin(
            config,
            core_interface.get_interface(),
            core_interface.get_codecs(),
            connection,
            workspace,
            user_info,
        )

        user_info.set_plugin(plugin.id, plugin)
        workspace_plugins = workspace.get_plugins()
        if plugin.name in workspace_plugins:
            # kill the plugin if already exist
            asyncio.ensure_future(plugin.terminate(True))
            user_info.remove_plugin(plugin.id)
        workspace.set_plugin(plugin.name, plugin)
        logger.info("New plugin registered successfully (%s)", plugin_id)

        bus.emit(
            "plugin_registered",
            plugin,
        )
        return {"success": True, "plugin_id": plugin_id}

    @sio.event
    async def plugin_message(sid, data):
        user_info = core_interface.all_sessions[sid]
        plugin_id = data["plugin_id"]
        ws, name = os.path.split(plugin_id)
        workspace = core_interface.get_workspace(ws)
        if not workspace:
            return {"success": False, "detail": f"Workspace not found: {ws}"}
        if user_info.id != ws and not core_interface.check_permission(
            workspace, user_info
        ):
            logger.error(
                "Permission denied: workspace=%s, user_id=%s", workspace, user_info.id
            )
            return {"success": False, "detail": "Permission denied"}

        plugin = workspace.get_plugin(name)
        if not plugin:
            logger.warning("Plugin %s not found in workspace %s", name, workspace.name)
            return {
                "success": False,
                "detail": f"Plugin {name} not found in workspace {workspace.name}",
            }

        core_interface.current_user.set(user_info)
        core_interface.current_plugin.set(plugin)
        core_interface.current_workspace.set(workspace)
        ctx = copy_context()
        ctx.run(plugin.connection.handle_message, data)
        return {"success": True}

    @sio.event
    async def disconnect(sid):
        """Event handler called when the client is disconnected."""
        user_info = core_interface.all_sessions[sid]
        core_interface.all_users[user_info.id].remove_session(sid)
        # if the user has no more all_sessions
        user_sessions = core_interface.all_users[user_info.id].get_sessions()
        if not user_sessions:
            del core_interface.all_users[user_info.id]
            user_plugins = user_info.get_plugins()
            for pid, plugin in list(user_plugins.items()):
                # TODO: how to allow plugin running when the user disconnected
                # we will also need to handle the case when the user login again
                # the plugin should be reclaimed for the user
                plugin.workspace.remove_plugin(plugin.name)
                # if there is no plugins in the workspace then we remove it
                workspace_plugins = plugin.workspace.get_plugins()
                if not workspace_plugins and not plugin.workspace.persistent:
                    core_interface.unregister_workspace(plugin.workspace.name)
                asyncio.ensure_future(plugin.terminate())
                user_info.remove_plugin(pid)

                # TODO: if a workspace has no plugins anymore
                # we should destroy it completely
                # Importantly, if we want to recycle the workspace name,
                # we need to make sure we don't mess up with the permission
                # with the plugins of the previous owners
                plugin_services = plugin.workspace.get_services()
                for service in list(plugin_services.values()):
                    if service.get_provider() == plugin:
                        plugin.workspace.remove_service(service.name)
        del core_interface.all_sessions[sid]
        bus.emit("plugin_disconnected", {"sid": sid})

    bus.emit("socketio_ready", None)


def create_application(allow_origins, base_path) -> FastAPI:
    """Set up the server application."""
    # pylint: disable=unused-variable

    app = FastAPI(
        title="ImJoy Core Server",
        description=(
            "A server for managing imjoy plugins and \
                enabling remote procedure calls"
        ),
        version=VERSION,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["Content-Type", "Authorization"],
    )

    return app


def setup_socketio_server(
    app: FastAPI,
    core_interface: CoreInterface,
    port: int,
    base_path: str = "/",
    allow_origins: Union[str, list] = "*",
    **kwargs,
) -> None:
    """Set up the socketio server."""
    socketio_path = base_path.rstrip("/") + "/socket.io"

    HTTPProxy(core_interface)

    @app.get(base_path)
    async def root():
        return {
            "name": "ImJoy Engine",
            "version": VERSION,
            "all_users": {
                uid: user_info.get_sessions()
                for uid, user_info in core_interface.all_users.items()
            },
            "all_workspaces": {
                w.name: len(w.get_plugins()) for w in core_interface.get_all_workspace()
            },
        }

    @app.get(base_path.rstrip("/") + "/liveness")
    async def liveness(req: Request) -> JSONResponse:
        try:
            await sio.emit("liveness")
        except Exception:  # pylint: disable=broad-except
            return JSONResponse({"status": "DOWN"}, status_code=503)
        return JSONResponse({"status": "OK"})

    if allow_origins == ["*"]:
        allow_origins = "*"
    sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins=allow_origins)

    _app = socketio.ASGIApp(socketio_server=sio, socketio_path=socketio_path)

    app.mount("/", _app)
    app.sio = sio

    initialize_socketio(sio, core_interface, core_interface.event_bus)

    @app.on_event("startup")
    async def startup_event():
        core_interface.event_bus.emit("startup")

    @app.on_event("shutdown")
    def shutdown_event():
        core_interface.event_bus.emit("shutdown")

    return sio


def start_server(args):
    """Start the socketio server."""
    if args.allow_origin:
        args.allow_origin = args.allow_origin.split(",")
    else:
        args.allow_origin = env.get("ALLOW_ORIGINS", "*").split(",")
    application = create_application(args.allow_origin, args.base_path)
    core_interface = CoreInterface(application)
    setup_socketio_server(application, core_interface, **vars(args))
    if args.host in ("127.0.0.1", "localhost"):
        print(
            "***Note: If you want to enable access from another host, "
            "please start with `--host=0.0.0.0`.***"
        )
    uvicorn.run(application, host=args.host, port=int(args.port))


def get_argparser():
    """Return the argument parser."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="host for the socketio server",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=3000,
        help="port for the socketio server",
    )
    parser.add_argument(
        "--allow-origin",
        type=str,
        default="*",
        help="origins for the socketio server",
    )
    parser.add_argument(
        "--base-path",
        type=str,
        default="/",
        help="the base path for the server",
    )
    return parser


if __name__ == "__main__":
    arg_parser = get_argparser()
    opt = arg_parser.parse_args()
    start_server(opt)
