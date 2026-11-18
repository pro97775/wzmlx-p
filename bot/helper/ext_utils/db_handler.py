from importlib import import_module
from time import time
from uuid import uuid4

from aiofiles import open as aiopen
from aiofiles.os import path as aiopath
from pymongo import AsyncMongoClient, UpdateOne
from pymongo.errors import PyMongoError
from pymongo.server_api import ServerApi

from ... import LOGGER, qbit_options, rss_dict, user_data
from ...core.config_manager import Config
from ...core.tg_client import TgClient, db_partition_id
from .file_types import get_batch_file_key


def _bot_id():
    if TgClient.ID:
        return str(TgClient.ID)
    token = getattr(Config, "BOT_TOKEN", "") or ""
    return token.split(":", 1)[0] or "0"


def _part():
    if not TgClient.PARTITION:
        TgClient.PARTITION = db_partition_id(_bot_id())
    return TgClient.PARTITION


def _new_uuid():
    return uuid4().hex


class DbManager:
    def __init__(self):
        self._return = True
        self._conn = None
        self.db = None

    async def connect(self):
        try:
            if self._conn is not None:
                await self._conn.close()
            self._conn = AsyncMongoClient(
                Config.DATABASE_URL, server_api=ServerApi("1")
            )
            self.db = self._conn.wzmlx
            self._return = False
            try:
                await self.db.batch_files[_part()].create_index(
                    [
                        ("user_id", 1),
                        ("engine", 1),
                        ("hash", 1),
                        ("uploaded", 1),
                    ]
                )
            except PyMongoError as e:
                LOGGER.error(f"Failed to create batch file index: {e}")
        except PyMongoError as e:
            LOGGER.error(f"Error in DB connection: {e}")
            self.db = None
            self._return = True
            self._conn = None

    async def disconnect(self):
        self._return = True
        if self._conn is not None:
            await self._conn.close()
        self._conn = None

    async def update_deploy_config(self):
        if self._return:
            return
        settings = import_module("config")
        config_file = {
            key: value.strip() if isinstance(value, str) else value
            for key, value in vars(settings).items()
            if not key.startswith("__")
        }
        await self.db.settings.deployConfig.replace_one(
            {"_id": _part()}, config_file, upsert=True
        )

    async def update_config(self, dict_):
        if self._return:
            LOGGER.warning("update_config skipped: DB not connected")
            return
        await self.db.settings.config.update_one(
            {"_id": _part()}, {"$set": dict_}, upsert=True
        )

    async def update_aria2(self, key, value):
        if self._return:
            return
        await self.db.settings.aria2c.update_one(
            {"_id": _part()}, {"$set": {key: value}}, upsert=True
        )

    async def update_qbittorrent(self, key, value):
        if self._return:
            return
        await self.db.settings.qbittorrent.update_one(
            {"_id": _part()}, {"$set": {key: value}}, upsert=True
        )

    async def save_qbit_settings(self):
        if self._return:
            return
        await self.db.settings.qbittorrent.update_one(
            {"_id": _part()}, {"$set": qbit_options}, upsert=True
        )

    async def update_private_file(self, path):
        if self._return:
            return
        db_path = path.replace(".", "__")
        if await aiopath.exists(path):
            async with aiopen(path, "rb+") as pf:
                pf_bin = await pf.read()
            await self.db.settings.files.update_one(
                {"_id": _part()}, {"$set": {db_path: pf_bin}}, upsert=True
            )
            if path == "config.py":
                await self.update_deploy_config()
        else:
            await self.db.settings.files.update_one(
                {"_id": _part()}, {"$unset": {db_path: ""}}, upsert=True
            )

    async def update_nzb_config(self):
        if self._return:
            return
        async with aiopen("sabnzbd/SABnzbd.ini", "rb+") as pf:
            nzb_conf = await pf.read()
        await self.db.settings.nzb.replace_one(
            {"_id": _part()}, {"SABnzbd__ini": nzb_conf}, upsert=True
        )

    async def update_user_data(self, user_id):
        if self._return:
            return
        data = user_data.get(user_id, {})
        data = data.copy()
        for key in ("THUMBNAIL", "RCLONE_CONFIG", "TOKEN_PICKLE", "USER_COOKIE_FILE"):
            data.pop(key, None)
        pipeline = [
            {
                "$replaceRoot": {
                    "newRoot": {
                        "$mergeObjects": [
                            data,
                            {
                                "$arrayToObject": {
                                    "$filter": {
                                        "input": {"$objectToArray": "$$ROOT"},
                                        "as": "field",
                                        "cond": {
                                            "$in": [
                                                "$$field.k",
                                                [
                                                    "THUMBNAIL",
                                                    "RCLONE_CONFIG",
                                                    "TOKEN_PICKLE",
                                                    "USER_COOKIE_FILE",
                                                ],
                                            ]
                                        },
                                    }
                                }
                            },
                        ]
                    }
                }
            }
        ]
        await self.db.users[_part()].update_one(
            {"_id": user_id}, pipeline, upsert=True
        )

    async def update_user_doc(self, user_id, key, path=""):
        if self._return:
            return
        if path:
            async with aiopen(path, "rb+") as doc:
                doc_bin = await doc.read()
            await self.db.users[_part()].update_one(
                {"_id": user_id}, {"$set": {key: doc_bin}}, upsert=True
            )
        else:
            await self.db.users[_part()].update_one(
                {"_id": user_id}, {"$unset": {key: ""}}, upsert=True
            )

    async def rss_update_all(self):
        if self._return:
            return
        for user_id in list(rss_dict.keys()):
            await self.db.rss[_part()].replace_one(
                {"_id": user_id}, rss_dict[user_id], upsert=True
            )

    async def rss_update(self, user_id):
        if self._return:
            return
        await self.db.rss[_part()].replace_one(
            {"_id": user_id}, rss_dict[user_id], upsert=True
        )

    async def rss_delete(self, user_id):
        if self._return:
            return
        await self.db.rss[_part()].delete_one({"_id": user_id})

    async def add_incomplete_task(
        self, cid, link, tag, command="", user_id=0, reply_to_msg_id=0, thread_id=0
    ):
        if self._return:
            return
        await self.db.tasks[_part()].update_one(
            {"link": link},
            {
                "$setOnInsert": {
                    "_id": _new_uuid(),
                    "cid": cid,
                    "tag": tag,
                    "link": link,
                    "command": command,
                    "user_id": user_id,
                    "reply_to_msg_id": reply_to_msg_id,
                    "thread_id": thread_id,
                }
            },
            upsert=True,
        )

    async def get_pm_uids(self):
        if self._return:
            return
        return [doc["_id"] async for doc in self.db.pm_users[_part()].find({})]

    async def set_pm_users(self, user_id):
        if self._return:
            return
        if not bool(await self.db.pm_users[_part()].find_one({"_id": user_id})):
            await self.db.pm_users[_part()].insert_one({"_id": user_id})
            LOGGER.info(f"New PM User Added : {user_id}")

    async def rm_pm_user(self, user_id):
        if self._return:
            return
        await self.db.pm_users[_part()].delete_one({"_id": user_id})

    async def rm_complete_task(self, link):
        if self._return:
            return
        await self.db.tasks[_part()].delete_one({"link": link})

    async def rm_incomplete_task(self, key):
        if self._return:
            return
        await self.db.tasks[_part()].delete_one({"_id": key})

    async def mark_batch_files_downloaded(self, user_id, engine, hash_, files):
        if self._return or not files:
            return
        now = time()
        operations = []
        for file_info in files:
            key = get_batch_file_key(user_id, engine, hash_, file_info["index"])
            operations.append(
                UpdateOne(
                    {"_id": key},
                    {
                        "$set": {
                            "user_id": user_id,
                            "engine": engine,
                            "hash": hash_,
                            "index": int(file_info["index"]),
                            "name": file_info.get("name", ""),
                            "size": int(file_info.get("size") or 0),
                            "downloaded": True,
                            "downloaded_at": now,
                        },
                        "$setOnInsert": {"uploaded": False},
                    },
                    upsert=True,
                )
            )
        try:
            await self.db.batch_files[_part()].bulk_write(
                operations, ordered=False
            )
        except PyMongoError as e:
            LOGGER.error(f"Failed to save downloaded torrent files: {e}")

    async def mark_batch_file_uploaded(
        self,
        user_id,
        engine,
        hash_,
        file_info,
        destination="",
        link="",
    ):
        if self._return:
            return
        key = get_batch_file_key(
            user_id, engine, hash_, file_info["index"]
        )
        now = time()
        try:
            await self.db.batch_files[_part()].update_one(
                {"_id": key},
                {
                    "$set": {
                        "user_id": user_id,
                        "engine": engine,
                        "hash": hash_,
                        "index": int(file_info["index"]),
                        "name": file_info.get("name", ""),
                        "size": int(file_info.get("size") or 0),
                        "uploaded": True,
                        "uploaded_at": now,
                        "destination": str(destination or ""),
                        "link": str(link or ""),
                    }
                },
                upsert=True,
            )
        except PyMongoError as e:
            LOGGER.error(f"Failed to save uploaded torrent file: {e}")

    async def get_uploaded_file_keys(self, user_id, engine, hash_):
        if self._return:
            return set()
        try:
            cursor = self.db.batch_files[_part()].find(
                {
                    "user_id": user_id,
                    "engine": engine,
                    "hash": hash_,
                    "uploaded": True,
                },
                {"_id": 1},
            )
            return {doc["_id"] async for doc in cursor}
        except PyMongoError as e:
            LOGGER.error(f"Failed to load uploaded torrent files: {e}")
            return set()

    async def clear_uploaded_files(self, user_id, engine=None, hash_=None):
        if self._return:
            return 0
        query = {"user_id": user_id}
        if engine:
            query["engine"] = engine
        if hash_:
            query["hash"] = hash_
        try:
            result = await self.db.batch_files[_part()].delete_many(query)
            return result.deleted_count
        except PyMongoError as e:
            LOGGER.error(f"Failed to clear uploaded torrent files: {e}")
            return 0

    async def get_incomplete_tasks(self):
        notifier_dict = {}
        if self._return:
            return notifier_dict
        if await self.db.tasks[_part()].find_one():
            rows = self.db.tasks[_part()].find({})
            async for row in rows:
                link = row.get("link") or row.get("_id")
                if not link:
                    continue
                cid = row["cid"]
                tag = row["tag"]
                task_data = {
                    "key": row["_id"],
                    "link": link,
                    "command": row.get("command", ""),
                    "user_id": row.get("user_id", 0),
                    "reply_to_msg_id": row.get("reply_to_msg_id", 0),
                    "thread_id": row.get("thread_id", 0),
                }
                if cid in notifier_dict:
                    if tag in notifier_dict[cid]:
                        notifier_dict[cid][tag].append(task_data)
                    else:
                        notifier_dict[cid][tag] = [task_data]
                else:
                    notifier_dict[cid] = {tag: [task_data]}
        return notifier_dict

    async def trunc_table(self, name):
        if self._return:
            return
        await self.db[name][_part()].drop()


database = DbManager()
