"""Shared file-batch runner used by torrent downloads."""

from asyncio import gather
from os import path as ospath, symlink

from aiofiles.os import makedirs, remove, rename, rmdir
from aiofiles.os import path as aiopath
from psutil import disk_usage

from ... import (
    DOWNLOAD_DIR,
    LOGGER,
    intervals,
    non_queued_dl,
    non_queued_up,
    queue_dict_lock,
    task_dict,
    task_dict_lock,
)
from ...core.config_manager import Config
from ..ext_utils.bot_utils import sync_to_async
from ..ext_utils.db_handler import database
from ..ext_utils.file_types import (
    filter_uploaded_files,
    get_batch_file_key,
    is_extension_allowed,
    is_file_type_allowed,
    plan_batch,
)
from ..ext_utils.files_utils import clean_download, split_file
from ..ext_utils.links_utils import is_gdrive_id
from ..ext_utils.media_utils import FFMpeg, get_document_type
from ..ext_utils.status_utils import get_readable_file_size
from ..ext_utils.task_manager import check_running_tasks, start_from_queued
from ..mirror_leech_utils.gdrive_utils.upload import GoogleDriveUpload
from ..mirror_leech_utils.rclone_utils.transfer import RcloneTransferHelper
from ..mirror_leech_utils.status_utils.gdrive_status import GoogleDriveStatus
from ..mirror_leech_utils.status_utils.queue_status import QueueStatus
from ..mirror_leech_utils.status_utils.rclone_status import RcloneStatus
from ..mirror_leech_utils.status_utils.telegram_status import TelegramStatus
from ..mirror_leech_utils.upload_utils.telegram_uploader import TelegramUploader
from ..telegram_helper.message_utils import send_message, update_status_message

FREE_SPACE_MARGIN = 256 * 1024 * 1024


class BatchError(Exception):
    pass


class BatchFileJob:
    """Per-file listener proxy used with the normal uploader classes."""

    def __init__(self, listener, file_info, file_path, rel_dir=""):
        object.__setattr__(self, "_parent", listener)
        object.__setattr__(self, "_up_dest", listener.up_dest)
        self.file_info = file_info
        self.path = file_path
        self.rel_dir = rel_dir
        self.name = ospath.basename(file_info["name"])
        self.size = file_info["size"]
        self.is_file = True
        self.dir = listener.dir
        self.up_dir = ""
        self.result = None
        self.error = ""

    def __getattr__(self, item):
        return getattr(self._parent, item)

    @property
    def is_cancelled(self):
        return self._parent.is_cancelled

    @is_cancelled.setter
    def is_cancelled(self, value):
        self._parent.is_cancelled = value

    @property
    def up_dest(self):
        return self._up_dest

    @up_dest.setter
    def up_dest(self, value):
        self._up_dest = value

    async def on_upload_complete(self, *args, **kwargs):
        self.result = (args, kwargs)

    async def on_upload_error(self, error, button=None):
        self.error = str(error)


class BatchRunner:
    engine = "Batch"

    def __init__(
        self,
        listener,
        tor_info,
        files,
        *,
        hash_=None,
        content_path=None,
        task_root=None,
    ):
        self.listener = listener
        if isinstance(tor_info, dict):
            self.hash = hash_ or tor_info.get("hash") or tor_info.get("gid")
            if content_path is None:
                content_path = tor_info.get("content_path", "")
        else:
            self.hash = hash_ or tor_info.hash
            if content_path is None:
                content_path = tor_info.content_path or ""
        self.tag = f"{listener.mid}"
        self.task_root = (task_root or listener.dir).rstrip("/")
        self.content_path = (content_path or "").rstrip("/")
        self.files = self._file_entries(files)
        self.pending = [
            file_info
            for file_info in self.files
            if file_info["size"]
            and is_extension_allowed(
                file_info["name"],
                None,
                listener.excluded_extensions,
            )
            and is_file_type_allowed(file_info["name"], listener.file_types)
        ]
        self.done = set()
        self.links = {}
        self.rclone_destination = ""
        self.dir_id = ""
        self.uploaded_count = 0
        self.uploaded_size = 0
        self.uploaded_keys = set()
        self.skipped_uploaded = 0
        self.skipped_size = 0
        self.seq = 0
        self.batch_offset = 0
        self.error = ""
        # Torrent file names include the torrent's root folder, so they are
        # relative to the task save path, not to content_path.
        self.root = self.task_root
        if not (listener.is_leech or is_gdrive_id(listener.up_dest)):
            destination = listener.up_dest
            if destination.startswith("mrcc:"):
                destination = destination.split("mrcc:", 1)[1]
            self.rclone_destination = destination

    def _file_key(self, file_info):
        return get_batch_file_key(
            self.listener.user_id,
            self.engine,
            self.hash,
            file_info["index"],
        )

    async def _load_uploaded_files(self):
        if not getattr(self.listener, "skip_uploaded_files", False):
            return
        if not Config.DATABASE_URL:
            LOGGER.warning(
                "Skipping already uploaded files requires DATABASE_URL."
            )
            return
        try:
            self.uploaded_keys = await database.get_uploaded_file_keys(
                self.listener.user_id, self.engine, self.hash
            )
        except Exception as error:
            LOGGER.error(f"Could not load uploaded file history: {error}")

    async def _mark_downloaded(self, batch):
        if not getattr(self.listener, "skip_uploaded_files", False):
            return
        try:
            await database.mark_batch_files_downloaded(
                self.listener.user_id, self.engine, self.hash, batch
            )
        except Exception as error:
            LOGGER.error(f"Could not save downloaded file history: {error}")

    async def _mark_uploaded(self, file_info, job):
        if not getattr(self.listener, "skip_uploaded_files", False):
            return
        if not job.result:
            return
        args, _ = job.result
        if self.listener.is_leech and len(args) > 3:
            try:
                corrupted = int(args[3] or 0)
            except (TypeError, ValueError):
                corrupted = 0
            if corrupted:
                LOGGER.warning(
                    f"Not recording {job.name}: "
                    f"{corrupted} part(s) failed to upload"
                )
                return
        link = args[0] if args else ""
        if not link and len(args) > 1 and isinstance(args[1], dict):
            links = args[1]
            link = next(
                (url for url, name in links.items() if name == job.name),
                next(iter(links), ""),
            )
        destination = (
            "telegram"
            if self.listener.is_leech
            else str(getattr(job, "up_dest", "") or "")
        )
        try:
            await database.mark_batch_file_uploaded(
                self.listener.user_id,
                self.engine,
                self.hash,
                file_info,
                destination=destination,
                link=link,
            )
        except Exception as error:
            LOGGER.error(f"Could not save uploaded file history: {error}")

    @staticmethod
    def _file_entries(files):
        entries = []
        for file_info in files:
            if isinstance(file_info, dict):
                entry = {
                    "index": int(file_info["index"]),
                    "name": str(file_info.get("name", "")),
                    "size": int(file_info.get("size") or 0),
                }
                if file_info.get("path"):
                    entry["path"] = str(file_info["path"])
                entries.append(entry)
                continue
            entries.append(
                {
                    "index": int(file_info.index),
                    "name": str(file_info.name),
                    "size": int(file_info.size or 0),
                }
            )
        return entries

    async def run(self):
        try:
            LOGGER.info(
                f"{self.engine} batch start: save_path={self.task_root} "
                f"content_path={self.content_path}"
            )
            await self._load_uploaded_files()
            if self.uploaded_keys:
                skipped_files = [
                    file_info
                    for file_info in self.pending
                    if self._file_key(file_info) in self.uploaded_keys
                ]
                if skipped_files:
                    self.skipped_uploaded = len(skipped_files)
                    self.skipped_size = sum(
                        int(file_info["size"]) for file_info in skipped_files
                    )
                    self.pending = filter_uploaded_files(
                        self.pending, self.uploaded_keys, self._file_key
                    )
                    LOGGER.info(
                        f"{self.engine} batch: skipped "
                        f"{self.skipped_uploaded} file(s) already uploaded"
                    )
            if not self.pending:
                if self.skipped_uploaded:
                    try:
                        await send_message(
                            self.listener.message,
                            f"Skipped {self.skipped_uploaded} file(s) that were "
                            "already uploaded. Nothing left to download.",
                        )
                    except Exception as error:
                        LOGGER.error(f"Could not send batch skip notice: {error}")
                    await self._finish()
                    return
                raise BatchError(
                    "No downloadable files in this torrent. Check the selected "
                    "file types and excluded extensions."
                )
            if self.skipped_uploaded:
                try:
                    await send_message(
                        self.listener.message,
                        f"Skipped {self.skipped_uploaded} already uploaded "
                        f"file(s); {len(self.pending)} file(s) remaining.",
                    )
                except Exception as error:
                    LOGGER.error(f"Could not send batch skip notice: {error}")
            self.listener.batch_total = len(self.pending)
            self.listener.batch_current = 1
            while self.pending:
                if self.listener.is_cancelled or intervals["stopAll"]:
                    await self._abort("")
                    return
                batch = self._next_batch()
                if not batch:
                    break
                if self.done and not await self._acquire_download_slot():
                    await self._abort("")
                    return
                self._check_space(batch)
                await self._apply_batch(batch)
                if not await self._wait_batch(batch):
                    await self._abort("Batch download stopped before completion!")
                    return
                await self._mark_downloaded(batch)
                if self.listener.is_cancelled or intervals["stopAll"]:
                    await self._abort("")
                    return
                if not await self._acquire_upload_slot():
                    await self._abort("")
                    return
                await self._upload_batch(batch)
                if self.listener.is_cancelled or intervals["stopAll"]:
                    await self._abort("")
                    return
                if self.error:
                    await self._abort(self.error)
                    return
                for file_info in batch:
                    if file_info in self.pending:
                        self.pending.remove(file_info)
                    self.done.add(file_info["index"])
            await self._finish()
        except BatchError as error:
            await self._abort(str(error))
        except Exception as error:
            LOGGER.error(f"{self.engine} batch error: {error}")
            await self._abort(str(error))

    async def _release_slot(self):
        async with queue_dict_lock:
            non_queued_dl.discard(self.listener.mid)
            non_queued_up.discard(self.listener.mid)
        await start_from_queued()

    async def _acquire_upload_slot(self):
        add_to_queue, event = await check_running_tasks(self.listener, "up")
        await start_from_queued()
        if add_to_queue:
            await event.wait()
        return not self.listener.is_cancelled and not intervals["stopAll"]

    async def _acquire_download_slot(self):
        add_to_queue, event = await check_running_tasks(self.listener, "dl")
        await start_from_queued()
        if add_to_queue:
            async with task_dict_lock:
                if self.listener.mid in task_dict:
                    task_dict[self.listener.mid] = QueueStatus(
                        self.listener, self.hash, "dl"
                    )
            await event.wait()
        return not self.listener.is_cancelled and not intervals["stopAll"]

    def _next_batch(self):
        return plan_batch(
            self.pending,
            self.listener.files_per_batch,
            self.listener.batch_max_size,
        )

    def _check_space(self, batch):
        needed = sum(file_info["size"] for file_info in batch)
        free = disk_usage(DOWNLOAD_DIR).free
        if needed > max(0, free - FREE_SPACE_MARGIN):
            raise BatchError(
                "Not enough free space for the next batch: "
                f"need {get_readable_file_size(needed)}, "
                f"free {get_readable_file_size(free)}."
            )

    async def _apply_batch(self, batch):
        raise NotImplementedError

    async def _wait_batch(self, batch):
        raise NotImplementedError

    def _candidate_paths(self, file_info):
        name = file_info["name"]
        candidates = [ospath.join(self.task_root, name)]
        if self.content_path:
            root_name = ospath.basename(self.content_path)
            candidates.append(ospath.join(self.content_path, name))
            if name.startswith(f"{root_name}/"):
                candidates.append(
                    ospath.join(self.content_path, name[len(root_name) + 1 :])
                )
            candidates.append(ospath.join(ospath.dirname(self.content_path), name))
        unique = []
        for path in candidates:
            if path not in unique:
                unique.append(path)
        return unique

    async def _resolve_file_path(self, file_info):
        for path in self._candidate_paths(file_info):
            if await aiopath.exists(path):
                return path
        return None

    async def _upload_batch(self, batch):
        await self._stop_download_for_upload(batch)
        for position, file_info in enumerate(batch, start=1):
            if self.listener.is_cancelled or intervals["stopAll"]:
                return
            path = await self._resolve_file_path(file_info)
            if path is None:
                candidates = ", ".join(self._candidate_paths(file_info))
                self.error = "Downloaded file is missing. Tried: " + candidates
                return
            rel_dir = ospath.dirname(file_info["name"])
            job = BatchFileJob(self.listener, file_info, path, rel_dir)
            self.listener.batch_current = self.batch_offset + position
            await self._upload_one(job)
            if job.error:
                self.error = job.error
                return
            self._record_result(job)
            await self._mark_uploaded(file_info, job)
            await self._delete_local(path)
            self.uploaded_count += 1
            self.uploaded_size += file_info["size"]
            await self._restore_status()
        await self._restore_status()

    async def _stop_download_for_upload(self, batch):
        raise NotImplementedError

    async def _upload_one(self, job):
        gid = self.hash
        chat_id = self.listener.message.chat.id
        if self.listener.is_leech:
            staging = ospath.join(
                self.task_root, f"batch-{self.listener.mid}-{self.seq}"
            )
            self.seq += 1
            await makedirs(staging, exist_ok=True)
            staged_path = ospath.join(staging, job.name)
            original_path = job.path
            moved = False
            try:
                try:
                    await sync_to_async(symlink, job.path, staged_path)
                except OSError:
                    await rename(job.path, staged_path)
                    job.path = staged_path
                    moved = True
                if self.listener.split_size and job.size > self.listener.split_size:
                    free = disk_usage(DOWNLOAD_DIR).free
                    if job.size > max(0, free - FREE_SPACE_MARGIN):
                        job.error = (
                            "Not enough free space to split this file for Telegram: "
                            f"need {get_readable_file_size(job.size)}, "
                            f"free {get_readable_file_size(free)}. Use rclone instead."
                        )
                        return job.error
                    if not await self._split_for_telegram(job, staged_path):
                        job.error = f"Could not split {job.name} for Telegram."
                        return job.error
                    if moved:
                        await rename(staged_path, original_path)
                        moved = False
                    else:
                        await remove(staged_path)
                uploader = TelegramUploader(job, staging)
                async with task_dict_lock:
                    task_dict[self.listener.mid] = TelegramStatus(
                        job, uploader, gid, "up"
                    )
                await gather(update_status_message(chat_id), uploader.upload())
            finally:
                if moved and job.error and await aiopath.exists(staged_path):
                    await rename(staged_path, original_path)
                await clean_download(staging)
        elif is_gdrive_id(self.listener.up_dest):
            uploader = GoogleDriveUpload(job, job.path)
            async with task_dict_lock:
                task_dict[self.listener.mid] = GoogleDriveStatus(
                    job, uploader, gid, "up"
                )
            await gather(update_status_message(chat_id), sync_to_async(uploader.upload))
        else:
            job.up_dest = self._rclone_file_destination(job.rel_dir)
            uploader = RcloneTransferHelper(job)
            async with task_dict_lock:
                task_dict[self.listener.mid] = RcloneStatus(job, uploader, gid, "up")
            await gather(update_status_message(chat_id), uploader.upload(job.path))

    async def _split_for_telegram(self, job, file_path):
        split_size = self.listener.split_size
        parts = -(-job.size // split_size)
        if self.listener.equal_splits:
            split_size = (job.size // parts) + (job.size % parts)
        if not self.listener.as_doc and (await get_document_type(file_path))[0]:
            return await FFMpeg(self.listener).split(
                file_path, job.name, parts, split_size
            )
        return await split_file(file_path, split_size, self.listener)

    def _rclone_file_destination(self, rel_dir):
        if not rel_dir or rel_dir == ".":
            return self.listener.up_dest
        return f"{self.listener.up_dest.rstrip('/')}/{rel_dir}"

    async def _restore_status(self):
        raise NotImplementedError

    async def _delete_local(self, path):
        for file_path in (path, f"{path}.!qB", f"{path}.aria2"):
            if await aiopath.exists(file_path):
                try:
                    await remove(file_path)
                except OSError as error:
                    LOGGER.error(f"Could not remove {file_path}: {error}")
        parent = ospath.dirname(path)
        root = self.root.rstrip("/")
        while root and parent.startswith(f"{root}/"):
            try:
                await rmdir(parent)
            except OSError:
                break
            parent = ospath.dirname(parent)

    def _record_result(self, job):
        if not job.result:
            return
        args, kwargs = job.result
        link = args[0] if args else ""
        if self.listener.is_leech:
            links = args[1] if len(args) > 1 and isinstance(args[1], dict) else {}
            for file_link, file_name in links.items():
                self.links[file_link] = file_name
            if link:
                self.links.setdefault(link, job.name)
        elif is_gdrive_id(self.listener.up_dest):
            if link:
                self.links.setdefault(link, job.name)
            self.dir_id = self.dir_id or kwargs.get("dir_id", "")
        else:
            if link:
                self.links.setdefault(link, job.name)

    async def _finish(self):
        await self._remove_torrent()
        processed_count = self.uploaded_count + self.skipped_uploaded
        processed_size = self.uploaded_size + self.skipped_size
        self.listener.size = processed_size
        self.listener.is_file = False
        link = next(iter(self.links), "")
        LOGGER.info(
            f"{self.engine} batch finished: {self.listener.name} - "
            f"{processed_count} file(s), "
            f"{get_readable_file_size(processed_size)}, "
            f"{self.skipped_uploaded} skipped"
        )
        try:
            if self.listener.is_leech:
                await self.listener.on_upload_complete(
                    None, self.links, processed_count, 0
                )
            else:
                await self.listener.on_upload_complete(
                    link,
                    processed_count,
                    0,
                    "File",
                    rclone_path=self.rclone_destination,
                    dir_id=self.dir_id,
                )
        finally:
            await self._release_slot()

    async def _remove_torrent(self):
        raise NotImplementedError

    async def _abort(self, error):
        if error:
            LOGGER.error(f"{self.engine} batch aborted: {error}")
        await self._remove_torrent()
        await clean_download(self.listener.dir)
        await self._release_slot()
        cancelled = self.listener.is_cancelled or intervals["stopAll"]
        if cancelled:
            async with task_dict_lock:
                task_dict.pop(self.listener.mid, None)
            await update_status_message(self.listener.message.chat.id)
            return
        async with task_dict_lock:
            active = self.listener.mid in task_dict
        if active:
            await self.listener.on_download_error(error or "Batch download failed!")
