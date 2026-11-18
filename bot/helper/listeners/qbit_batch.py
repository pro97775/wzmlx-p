"""Batch download support for qBittorrent torrents."""

from asyncio import sleep
from os import path as ospath
from time import time

from aiofiles import open as aiopen
from aiofiles.os import path as aiopath, remove
from aioqbt.api import AddFormBuilder

from ... import (
    LOGGER,
    bot_loop,
    intervals,
    qb_listener_lock,
    qb_torrents,
    task_dict,
    task_dict_lock,
)
from ...core.config_manager import Config
from ...core.torrent_manager import TorrentManager
from ..ext_utils.files_utils import clean_download
from ..ext_utils.task_manager import check_running_tasks
from ..mirror_leech_utils.status_utils.qbit_status import QbittorrentStatus
from ..telegram_helper.message_utils import send_status_message, update_status_message
from .batch import BatchError, BatchRunner
from .qbit_listener import on_download_start

METADATA_TIMEOUT = 300
STOPPED_ABORT = 60
ACTIVE_STATES = (
    "downloading",
    "uploading",
    "stalledDL",
    "stalledUP",
    "queuedDL",
    "queuedUP",
    "forcedDL",
    "forcedUP",
    "checkingDL",
    "checkingUP",
    "metaDL",
    "allocating",
    "moving",
    "unknown",
)


async def _torrent_info(hash_):
    infos = await TorrentManager.qbittorrent.torrents.info(hashes=[hash_])
    return infos[0] if infos else None


async def _wait_for_files(hash_, listener):
    started = time()
    while True:
        if listener.is_cancelled or intervals["stopAll"]:
            raise BatchError("Batch cancelled.")
        tor_info = await _torrent_info(hash_)
        if tor_info is not None:
            if tor_info.state == "error":
                raise BatchError("qBittorrent download failed. Check the torrent.")
            if tor_info.state == "missingFiles":
                raise BatchError("qBittorrent reports missing files for this torrent.")
            try:
                files = await TorrentManager.qbittorrent.torrents.files(hash_)
            except Exception as error:
                LOGGER.error(f"qBittorrent batch file list error: {error}")
                files = []
            if (
                files
                and any(int(getattr(item, "size", 0) or 0) for item in files)
                and tor_info.state not in ("metaDL", "checkingResumeData")
            ):
                return tor_info, files
        if time() - started >= METADATA_TIMEOUT:
            raise BatchError(
                "Could not read the torrent file list. Metadata may be unavailable."
            )
        await sleep(1)


class QbitBatch(BatchRunner):
    engine = "qBittorrent"

    def __init__(self, listener, hash_, files, save_path):
        entries = []
        for file_info in files:
            name = str(file_info.name or "")
            if not name or name.startswith("[METADATA]"):
                continue
            entries.append(
                {
                    "index": int(file_info.index),
                    "name": name,
                    "size": int(file_info.size or 0),
                    "path": ospath.join(save_path, name),
                }
            )
        super().__init__(
            listener,
            {"hash": hash_, "content_path": save_path},
            entries,
            task_root=save_path,
        )
        self.hash = hash_
        self.save_path = save_path

    async def _apply_batch(self, batch):
        batch_ids = [file_info["index"] for file_info in batch]
        all_ids = [file_info["index"] for file_info in self.files]
        try:
            tor_info = await _torrent_info(self.hash)
            if tor_info is not None and tor_info.state in ACTIVE_STATES:
                await TorrentManager.qbittorrent.torrents.stop([self.hash])
            if all_ids:
                await TorrentManager.qbittorrent.torrents.file_prio(
                    self.hash, all_ids, 0
                )
            await TorrentManager.qbittorrent.torrents.file_prio(
                self.hash, batch_ids, 1
            )
            await TorrentManager.qbittorrent.torrents.start([self.hash])
        except Exception as error:
            raise BatchError(
                f"Could not start the next qBittorrent batch: {error}"
            ) from error
        self.batch_offset = len(self.done)
        self.listener.batch_current = self.batch_offset + 1
        self.listener.batch_selected_size = sum(
            file_info["size"] for file_info in batch
        )
        self.listener.batch_completed_size = 0
        async with task_dict_lock:
            if self.listener.mid in task_dict:
                task_dict[self.listener.mid] = QbittorrentStatus(self.listener)
        await update_status_message(self.listener.message.chat.id)

    async def _wait_batch(self, batch):
        indices = {file_info["index"]: file_info for file_info in batch}
        stopped_time = 0
        failures = 0
        while True:
            if self.listener.is_cancelled or intervals["stopAll"]:
                return False
            try:
                tor_info = await _torrent_info(self.hash)
                failures = 0
            except Exception as error:
                LOGGER.error(f"qBittorrent batch status error: {error}")
                failures += 1
                if failures >= 3:
                    return False
                await sleep(3)
                continue
            if tor_info is None:
                return False
            state = tor_info.state
            if state in ("error", "missingFiles"):
                raise BatchError("qBittorrent could not download the current batch.")
            if state in ("stoppedDL", "stoppedUP", "pausedDL", "pausedUP"):
                stopped_time += 3
                if stopped_time >= STOPPED_ABORT:
                    return False
            else:
                stopped_time = 0
            try:
                files = await TorrentManager.qbittorrent.torrents.files(self.hash)
            except Exception as error:
                LOGGER.error(f"qBittorrent batch file status error: {error}")
                await sleep(3)
                continue
            files_by_index = {int(file_info.index): file_info for file_info in files}
            complete = True
            total_size = 0
            completed_size = 0
            for index, file_info in indices.items():
                current = files_by_index.get(index)
                if current is None:
                    complete = False
                    break
                size = int(current.size or file_info["size"] or 0)
                progress = float(getattr(current, "progress", 0) or 0)
                total_size += size
                if size:
                    completed_size += min(size, int(size * progress))
                    if progress < 0.999:
                        complete = False
            if total_size:
                self.listener.batch_selected_size = total_size
                self.listener.batch_completed_size = completed_size
            if complete:
                await sleep(2)
                return True
            await sleep(3)

    async def _stop_download_for_upload(self, batch):
        try:
            await TorrentManager.qbittorrent.torrents.stop([self.hash])
        except Exception as error:
            LOGGER.error(f"qBittorrent batch stop before upload: {error}")

    async def _restore_status(self):
        async with task_dict_lock:
            if self.listener.mid in task_dict:
                task_dict[self.listener.mid] = QbittorrentStatus(self.listener)
        await update_status_message(self.listener.message.chat.id)

    async def _remove_torrent(self):
        try:
            await TorrentManager.qbittorrent.torrents.stop([self.hash])
        except Exception:
            pass
        try:
            await TorrentManager.qbittorrent.torrents.delete([self.hash], False)
        except Exception as error:
            LOGGER.error(f"qBittorrent batch remove error: {error}")
        async with qb_listener_lock:
            qb_torrents.pop(self.tag, None)
        try:
            await TorrentManager.qbittorrent.torrents.delete_tags(tags=[self.tag])
        except Exception as error:
            LOGGER.error(f"qBittorrent batch tag remove error: {error}")

    async def _finish(self):
        await super()._finish()
        await clean_download(self.listener.dir)


async def add_qb_batch(listener, path, ratio, seed_time):
    if Config.DISABLE_TORRENTS:
        await listener.on_download_error("Torrents are disabled in the configuration.")
        return
    local_torrent = await aiopath.exists(listener.link)
    tag = f"{listener.mid}"
    hash_ = None
    try:
        form = AddFormBuilder.with_client(TorrentManager.qbittorrent)
        if local_torrent:
            async with aiopen(listener.link, "rb") as torrent_file:
                data = await torrent_file.read()
            form = form.include_file(data)
        else:
            form = form.include_url(listener.link)
        form = form.savepath(path).tags([tag]).stopped(True)
        add_to_queue, event = await check_running_tasks(listener)
        await TorrentManager.qbittorrent.torrents.add(form.build())

        started = time()
        tor_info = None
        while tor_info is None:
            infos = await TorrentManager.qbittorrent.torrents.info(tag=tag)
            if infos:
                tor_info = infos[0]
                break
            if time() - started >= 60:
                raise BatchError("qBittorrent did not add the torrent.")
            await sleep(1)
        hash_ = tor_info.hash
        listener.name = tor_info.name

        await on_download_start(tag)
        async with qb_listener_lock:
            if tag in qb_torrents:
                qb_torrents[tag]["batch"] = True
        async with task_dict_lock:
            task_dict[listener.mid] = QbittorrentStatus(listener, queued=add_to_queue)
        listener.is_torrent = True

        if not local_torrent:
            await TorrentManager.qbittorrent.torrents.start([hash_])
        tor_info, files = await _wait_for_files(hash_, listener)
        listener.name = tor_info.name
        await TorrentManager.qbittorrent.torrents.stop([hash_])

        batch = QbitBatch(listener, hash_, files, path)
        if add_to_queue:
            await event.wait()
            if listener.is_cancelled:
                return
            async with task_dict_lock:
                if task := task_dict.get(listener.mid):
                    task.queued = False

        await listener.on_download_start()
        if listener.multi <= 1 and not listener.is_rss:
            await send_status_message(listener.message)
        bot_loop.create_task(batch.run())
    except BatchError as error:
        await _cleanup_failed_batch(listener, hash_, tag)
        if listener.is_cancelled or intervals["stopAll"]:
            return
        await listener.on_download_error(str(error))
    except Exception as error:
        LOGGER.error(f"qBittorrent batch setup error: {error}")
        await _cleanup_failed_batch(listener, hash_, tag)
        await listener.on_download_error(f"{error}")
    finally:
        if local_torrent and await aiopath.exists(listener.link):
            await remove(listener.link)


async def _cleanup_failed_batch(listener, hash_, tag):
    if hash_:
        try:
            await TorrentManager.qbittorrent.torrents.stop([hash_])
        except Exception:
            pass
        try:
            await TorrentManager.qbittorrent.torrents.delete([hash_], False)
        except Exception as error:
            LOGGER.error(f"qBittorrent batch cleanup error: {error}")
    async with qb_listener_lock:
        qb_torrents.pop(tag, None)
    try:
        await TorrentManager.qbittorrent.torrents.delete_tags(tags=[tag])
    except Exception as error:
        LOGGER.error(f"qBittorrent batch cleanup tag error: {error}")
