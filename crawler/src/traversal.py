import typing

import anytree

from . import data_access
from . import log_utils

LOG = log_utils.configure_logger()


class Cursor(anytree.NodeMixin):
    default_has_next = 'hasNextPage'
    default_cursor_name = 'endCursor'
    default_variable_name = 'cursor'

    def __init__(self, access: data_access.AccessPath, variable_name: str = None, cursor_name: str = None,
                 has_next: str = None, parent: 'Cursor' = None):
        super(Cursor, self).__init__()
        self._access = access
        self._variable_name = variable_name or self.default_variable_name
        self._cursor_name = cursor_name or self.default_cursor_name
        self._has_next = has_next or self.default_has_next
        self._cursor_value = ''
        self.parent = parent

    def _page_info(self, data: dict) -> dict:
        LOG.debug(f'Accessing "{self._access}"')
        return self._access.run(data)

    def has_next(self, data: dict) -> bool:
        try:
            return self._page_info(data)[self._has_next]
        except IndexError:
            return False

    def next_cursor(self, data: dict) -> str:
        self._cursor_value = self._page_info(data)[self._cursor_name]
        return self._cursor_value

    @property
    def variable_name(self) -> str:
        return self._variable_name

    @property
    def cursor_value(self) -> str:
        return self._cursor_value

    def reset_cursor_value(self):
        self._cursor_value = ''

    def __str__(self):
        string = f'Cursor(access={str(self._access)}, variable_name={self._variable_name}, ' +\
               f'has_next={self._has_next}, cursor_name={self._cursor_name}, cursor_value={self._cursor_value}'
        if self.parent:
            return string + f', parent={self.parent.variable_name})'
        return string + ')'


class CursorGenerator:
    def __init__(self, root: Cursor):
        self._root = root

    def next_cursors(self, data: dict) -> typing.Optional[typing.Dict[str, str]]:
        cursors = {}
        looking_for_next_cursor = True
        post_order_iter: typing.Iterator[Cursor] = anytree.PostOrderIter(self._root)
        for node in post_order_iter:
            LOG.debug(f'Accessing node {node}')
            if looking_for_next_cursor and node.has_next(data):
                LOG.debug(f'{node.variable_name}: found next cursor')
                looking_for_next_cursor = False
                cursors[node.variable_name] = node.next_cursor(data)

                # we don't want to re-use the cursor of descendants
                descendants: typing.Iterator[Cursor] = node.descendants
                for descendant in descendants:
                    descendant.reset_cursor_value()

            elif node.cursor_value:
                LOG.debug(f'{node.variable_name}: use previous cursor')
                # only set cursor value if it exists otherwise leave cursor unset
                cursors[node.variable_name] = node.cursor_value
            else:
                LOG.debug(f'{node.variable_name}: use no cursor')

        if looking_for_next_cursor:
            # we have not found any cursor that can be continued
            return None

        return cursors
