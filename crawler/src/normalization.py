import typing
from collections import defaultdict
from functools import wraps, partial

import pandas as pd

from . import log_utils
from .constants import keys

LOG = log_utils.configure_logger()



def _gated(fun: typing.Callable, exception: Exception):
    @wraps(fun)
    def wrapped_fun(*args, **kwargs):
        try:
            return fun(*args, **kwargs)
        except exception as ex:
            LOG.error('encountered %s in %s first argument: \'%s\'', ex.__class__.__name__, fun.__name__, args[0])
            raise
    return wrapped_fun


gated = partial(_gated, exception=Exception)


def extract_columns(record) -> defaultdict:
    columns = defaultdict(list)
    for review in record:
        for key, value in review.items():
            columns[key].append(value)
    return columns


def flatten(record, path):
    rows = []
    for el in record:
        rows += path(el)
    return rows


@gated
def flatten_recursively_inplace(record, names):
    if not isinstance(record, dict) and not isinstance(record, list):
        return
    if isinstance(record, list):
        for el in record:
            flatten_recursively_inplace(el, names)
        return
    for key in list(record.keys()):
        if key in names:
            if record[key]:
                for key2 in list(record[key].keys()):
                    new_key = key + key2[0].upper() + key2[1:]
                    record[new_key] = record[key][key2]
            del record[key]
        else:
            flatten_recursively_inplace(record[key], names)


@gated
def pr_extract_reviews(record) -> pd.DataFrame:
    result = pd.DataFrame(record[keys.REVIEWS][keys.NODES])
    if not result.empty:
        result = result.rename(columns={'authorLogin': 'reviewerLogin'})
        if 'authorLogin' in record:
            result['authorLogin'] = record['authorLogin']
        result[keys.NUMBER] = record[keys.NUMBER]
    return result


@gated
def pr_extract_comments(record) -> pd.DataFrame:
    result = pd.DataFrame(record[keys.COMMENTS][keys.NODES])
    if not result.empty:
        result[keys.NUMBER] = record[keys.NUMBER]
    return result


@gated
def pr_extract_review_threads(record) -> pd.DataFrame:
    rows = flatten(record[keys.REVIEW_THREADS][keys.NODES], lambda x: x[keys.COMMENTS][keys.NODES])
    result = pd.DataFrame(rows)
    if not result.empty:
        result[keys.NUMBER] = record[keys.NUMBER]
    return result


@gated
def pr_extract_labels(record) -> pd.DataFrame:
    result = pd.DataFrame(record[keys.LABELS][keys.NODES])
    if not result.empty:
        result[keys.NUMBER] = record[keys.NUMBER]
    return result


def delete_if_exists(record, key):
    if key in record:
        del record[key]


def first_if_exists_and_delete(record, key, prefix):
    """ extracts first element of subset and subsubset and deletes the original entry"""
    if key in record:
        if keys.NODES in record[key] and len(record[key][keys.NODES]) > 0:
            sub_record = record[key][keys.NODES][0]
            for k in sub_record:
                name = prefix + k[0].upper() + k[1:]
                record[name] = sub_record[k]
        del record[key]


def nested_first_if_exists_and_delete(record, key1, key2, prefix):
    """ extracts first element of subset and subsubset and deletes the original entry"""
    if key1 in record and keys.NODES in record[key1] and len(record[key1][keys.NODES]) > 0:
        sub_record = record[key1][keys.NODES][0]
        if key2 in sub_record and keys.NODES in sub_record[key2] and len(sub_record[key2][keys.NODES]) > 0:
            subsub_record = sub_record[key2][keys.NODES][0]
            for k in subsub_record:
                name = prefix + k[0].upper() + k[1:]
                record[name] = subsub_record[k]

    if key1 in record:
        del record[key1]


def extract_labels(record: dict):
    record['labels'] = record['labels']['nodes']
    result = []
    for el in record['labels']:
        result.append(el['name'])
    record['labels'] = result


@gated
def extract_pr_flat(record) -> pd.DataFrame:
    """ creates a flat representation of a PR """
    first_if_exists_and_delete(record, keys.COMMENTS, keys.FIRST_COMMENT_PREFIX)
    first_if_exists_and_delete(record, keys.REVIEWS, keys.FIRST_REVIEW_PREFIX)
    nested_first_if_exists_and_delete(record, keys.REVIEW_THREADS, keys.COMMENTS, keys.FIRST_REVIEW_THREAD_PREFIX)
    extract_labels(record)
    return pd.Series(record).to_frame().T
