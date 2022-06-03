import typing
from typing import Callable

ExtractorType = Callable[[dict], dict]
FormatterType = Callable[[dict], typing.NoReturn]
