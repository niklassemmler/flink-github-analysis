import json
import logging
import typing
from typing import Callable, NoReturn, Optional

from . import data_access
from . import log_utils
from .backup import Backup
from .constants import keys
from .custom_types import FormatterType
from .graphl_client import GraphQLClient
from .traversal import CursorGenerator

LOG = log_utils.configure_logger()


class DataCollector:
    def __init__(self, client: GraphQLClient, query: str, cursor_generator: CursorGenerator,
                 records_extractor: data_access.AccessPath, record_callback: Callable[[dict], NoReturn], limit: int = 0,
                 step_size: int = 100,
                 resume: bool = False):
        self._client = client
        self._query = query
        self._cursor_generator = cursor_generator
        self._records_extractor = records_extractor
        self._record_callback = record_callback
        self._step_size = step_size
        self._limit = limit  # used for testing
        self._n = 0
        self._cursor_name = keys.END_CURSOR
        self._has_next_name = keys.HAS_NEXT_PAGE
        self._resume = resume

    def _compile_params(self, cursors: typing.Optional[typing.Dict[str, str]]) -> dict:
        if self._limit > 0:
            step_size = max(0, min(self._limit - self._n, self._step_size))
        else:
            step_size = self._step_size
        params = {'step': step_size}
        if cursors:
            params.update(cursors)
        return params

    def run(self):
        LOG.info("data collection start")
        has_next = True
        cursors = {}
        if self._resume:
            cursors = Backup.load()
            LOG.info(f"starting from cursor '{cursors}'")
        try:
            while has_next and (self._limit <= 0 or self._n < self._limit):
                params = self._compile_params(cursors)
                LOG.debug("parameters: %s", params)
                data = self._client.send_graphql_query(self._query, variable_values=params)
                if LOG.isEnabledFor(logging.DEBUG):
                    LOG.debug(f"data: %s", json.dumps(data, indent=2))
                records = self._records_extractor.run(data)
                for record in records:
                    self._record_callback(record)
                cursors = self._cursor_generator.next_cursors(data)
                has_next = cursors is not None
                self._n += len(records)
                LOG.info("Collected %d records", self._n)
        except Exception as ex:
            LOG.error(
                "encountered error with cursor '%s' after %d records. You can resume from this positon via the '-r' argument",
                cursors, self._n)
            if cursors:
                Backup.save(cursors)
            LOG.exception(ex)
            raise


class DataCollectorBuilderException(Exception):
    pass


class DataCollectorBuilder:
    def __init__(self):
        self._client: Optional[GraphQLClient] = None
        self._query: Optional[str] = None
        self._cursor_generator: Optional[CursorGenerator] = None
        self._records_access: Optional[data_access.AccessPath] = None
        self._record_callback: Optional[Callable[[dict], NoReturn]] = None
        self._limit: int = 0
        self._step_size: int = 100
        self._resume = False

    def add_client(self, client: GraphQLClient) -> 'DataCollectorBuilder':
        self._client = client
        return self

    def add_query(self, query: str) -> 'DataCollectorBuilder':
        self._query = query
        return self

    def add_cursor_generator(self, traverser: CursorGenerator) -> 'DataCollectorBuilder':
        self._cursor_generator = traverser
        return self

    def add_records_access(self, records_extractor: Optional[data_access.AccessPath]) -> 'DataCollectorBuilder':
        self._records_access = records_extractor
        return self

    def add_record_callback(self, record_callback: Callable[[dict], NoReturn]) -> 'DataCollectorBuilder':
        self._record_callback = record_callback
        return self

    def add_limit(self, limit: int) -> 'DataCollectorBuilder':
        self._limit = limit
        return self

    def add_step_size(self, step_size: int) -> 'DataCollectorBuilder':
        self._step_size = step_size
        return self

    def enable_resume(self) -> 'DataCollectorBuilder':
        self._resume = True
        return self

    def build(self):
        if not self._client:
            raise DataCollectorBuilderException("'client' not set")
        if not self._query:
            raise DataCollectorBuilderException("'query' not set")
        if not self._cursor_generator:
            raise DataCollectorBuilderException("'traverser' not set")
        if not self._records_access:
            raise DataCollectorBuilderException("'records_access' not set")
        if not self._record_callback:
            raise DataCollectorBuilderException("'record_callback' not set")

        return DataCollector(self._client, self._query, self._cursor_generator,
                             self._records_access, self._record_callback, limit=self._limit,
                             step_size=self._step_size, resume=self._resume)


class JsonWriter:
    def __init__(self, path: str, formatter: typing.Optional[FormatterType] = None, append: bool = False):
        LOG.info(f"Writing to {path}")
        if append:
            self._f = open(path, 'a')
        else:
            self._f = open(path, 'w')
        self._formatter = formatter

    def add(self, record: dict):
        try:
            if self._formatter:
                self._formatter(record)
            json.dump(record, self._f)
            self._f.write('\n')
            self._f.flush()
        except:
            self._f.close()
            raise

    def close(self):
        self._f.close()