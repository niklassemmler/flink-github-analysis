import json
import os
import typing
from typing import Optional

from . import log_utils
from .constants import Files

LOG = log_utils.configure_logger()


class Backup:
    KEY = 'cursor'

    @staticmethod
    def _load_file() -> typing.Optional[dict]:
        if not os.path.exists(Files.BACKUP_FILE):
            LOG.info("backup file does not exist")
            return None
        with open(Files.BACKUP_FILE, 'r') as f:
            return json.load(f)

    @staticmethod
    def _save_file(content: dict):
        with open(Files.BACKUP_FILE, 'w') as f:
            json.dump(content, f)

    @classmethod
    def load(cls) -> Optional[dict]:
        LOG.info("loading cursor")
        content = cls._load_file()
        if not content or cls.KEY not in content:
            LOG.info("cannot load backup")
            return None
        cursor = content[cls.KEY]
        LOG.info("loaded cursor %s", cursor)
        return cursor

    @classmethod
    def save(cls, cursors: dict):
        LOG.info("save cursor %s", cursors)
        content = cls._load_file()
        if content:
            content[cls.KEY] = cursors
        else:
            content = {cls.KEY: cursors}
        cls._save_file(content)
