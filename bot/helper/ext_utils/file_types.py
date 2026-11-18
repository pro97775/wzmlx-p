"""Helpers for classifying torrent files and planning download batches."""

from os import path as ospath

FILE_TYPE_GROUPS = {
    "video": {
        "264",
        "265",
        "3g2",
        "3gp",
        "3gp2",
        "3gpp",
        "3gpp2",
        "amv",
        "asf",
        "avc",
        "avi",
        "divx",
        "drc",
        "dv",
        "f4v",
        "flv",
        "gvi",
        "gxf",
        "h264",
        "h265",
        "hevc",
        "ivf",
        "m1v",
        "m2p",
        "m2t",
        "m2ts",
        "m2v",
        "m4s",
        "m4v",
        "mk3d",
        "mkv",
        "mov",
        "mp4",
        "mp4v",
        "mpe",
        "mpeg",
        "mpeg2",
        "mpeg4",
        "mpg",
        "mpv",
        "mts",
        "mxf",
        "mxg",
        "nsv",
        "nut",
        "ogm",
        "ogv",
        "ogx",
        "qt",
        "rm",
        "rmvb",
        "roq",
        "svi",
        "tod",
        "ts",
        "vob",
        "vro",
        "webm",
        "wm",
        "wmv",
        "wtv",
        "xvid",
        "y4m",
    },
    "audio": {
        "aac",
        "ac3",
        "aiff",
        "alac",
        "ape",
        "dts",
        "flac",
        "m4a",
        "mka",
        "mp3",
        "ogg",
        "opus",
        "wav",
        "wma",
    },
    "photo": {
        "avif",
        "bmp",
        "gif",
        "heic",
        "heif",
        "jpeg",
        "jpg",
        "png",
        "raw",
        "svg",
        "tiff",
        "webp",
    },
    "document": {
        "azw3",
        "csv",
        "djvu",
        "doc",
        "docx",
        "epub",
        "md",
        "mobi",
        "odt",
        "pdf",
        "ppt",
        "pptx",
        "rtf",
        "txt",
        "xls",
        "xlsx",
    },
    "apps": {
        "apk",
        "apks",
        "appimage",
        "bat",
        "deb",
        "dmg",
        "exe",
        "ipa",
        "iso",
        "jar",
        "msi",
        "pkg",
        "rpm",
        "xapk",
    },
}


def get_file_type(name: str) -> str:
    """Return the category of a torrent file name, or ``"other"``."""
    file_name = ospath.basename(name or "")
    if "." not in file_name:
        return "other"
    extension = file_name.rsplit(".", 1)[1].strip().lower()
    for category, extensions in FILE_TYPE_GROUPS.items():
        if extension in extensions:
            return category
    return "other"


def get_extension(name: str) -> str:
    file_name = ospath.basename(name or "")
    if "." not in file_name:
        return ""
    return file_name.rsplit(".", 1)[1].strip().lower()


def is_extension_allowed(name: str, included=None, excluded=None) -> bool:
    extension = get_extension(name)
    if included and extension not in included:
        return False
    if isinstance(excluded, str):
        excluded = excluded.split()
    return extension not in {str(item).lower() for item in (excluded or [])}


def normalize_types(value) -> list:
    """Normalize a user/config file-type value into a stable list of keys."""
    if not value:
        return []
    if isinstance(value, str):
        items = value.replace(",", " ").split()
    elif isinstance(value, (list, tuple, set)):
        items = value
    else:
        return []
    selected = {str(item).strip().lower() for item in items}
    return [category for category in FILE_TYPE_GROUPS if category in selected]


def is_file_type_allowed(name: str, allowed_types) -> bool:
    """True when no filter is set or when the file matches a selected type."""
    allowed = normalize_types(allowed_types)
    if not allowed:
        return True
    return get_file_type(name) in allowed


def get_file_size(file_info) -> int:
    if isinstance(file_info, dict):
        return int(file_info.get("size") or 0)
    return int(getattr(file_info, "size", 0) or 0)


def get_batch_file_key(user_id, engine, torrent_hash, file_index) -> str:
    """Stable per-user key for one torrent file."""
    return f"{user_id}:{engine}:{torrent_hash}:{int(file_index)}"


def filter_uploaded_files(files, uploaded_keys, key_builder) -> list:
    """Return only files that are not present in the uploaded-file ledger."""
    if not uploaded_keys:
        return list(files)
    return [file_info for file_info in files if key_builder(file_info) not in uploaded_keys]


def plan_batch(files, max_files: int = 0, max_size: int = 0) -> list:
    """Pick the next batch of files by count and (optionally) total size.

    The first file is always included so an oversized file cannot stall the
    queue forever; the caller checks free space before downloading it.
    """
    batch = []
    total_size = 0
    for file_info in files:
        if max_files and len(batch) >= max_files:
            break
        size = get_file_size(file_info)
        if max_size and batch and total_size + size > max_size:
            break
        batch.append(file_info)
        total_size += size
    return batch
