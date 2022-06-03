import json
import os
import re
import typing

from gql.transport.exceptions import TransportQueryError

from .constants import *
from .log_utils import configure_logger

LOG = configure_logger()


class NoSecret(Exception):
    pass


class NoQuery(Exception):
    pass


def load_secret() -> str:
    path = os.path.abspath(constants.FILE_NAME_SECRET)
    if not os.path.exists(path):
        raise NoSecret(f"No {constants.FILE_NAME_SECRET} file exists.")
    with open(path, 'r') as f:
        content = f.read()
    for line in content.split("\n"):
        match = re.match(constants.SECRET_REGEX, line)
        if match:
            return match.group(1)
    raise NoSecret(f"{constants.FILE_NAME_SECRET} does not contain secret")


def load_query(name: str):
    LOG.debug("loading query %s", name)
    dir_path = os.path.abspath("graphql")
    path = os.path.join(dir_path, name)
    if not os.path.exists(path):
        raise NoQuery(f"Cannot find query: {name}")
    with open(path, 'r') as f:
        query = f.read()
    LOG.debug("loaded query %s", query)
    return query


def pretty_print(json_dict: typing.Union[dict, str]):
    if isinstance(json_dict, dict):
        print(json.dumps(json_dict, indent=2))
    else:
        print(json_dict)


def no_such_pr_error(ex: TransportQueryError) -> bool:
    try:
        if ex.errors[0]['type'] == "NOT_FOUND":
            return True
    except:
        return False


def to_access_fun(path: str):
    def fun(data: dict) -> dict:
        content = data
        for path_elem in path.split('/'):
            content = content[path_elem]
        return content

    return fun


