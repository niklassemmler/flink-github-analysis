import typing
from . import log_utils


LOG = log_utils.configure_logger()

class AccessPath:
    def __init__(self, segments: typing.List[typing.Union[str, int]]):
        self._segments = segments

    def run(self, data: dict) -> dict:
        content = data
        for el in self._segments:
            try:
                content = content[el]
            except KeyError as e:
                LOG.error("cannot find segment '%s': %s", el, content)
                LOG.exception(e)
                raise
        return content

    def __str__(self):
        return '/'.join([str(x) for x in self._segments])


class AccessPathBuilder:
    def __init__(self):
        self._segments = []

    def add(self, segment: typing.Union[str, int]) -> 'AccessPathBuilder':
        """
        Warning: This method changes the instance's state
        """
        self._segments.append(segment)
        return self

    def copy(self) -> 'AccessPathBuilder':
        copy = AccessPathBuilder()
        copy._segments = [el for el in self._segments]
        return copy

    def build(self) -> AccessPath:
        return AccessPath(self._segments)