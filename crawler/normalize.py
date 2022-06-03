import json
import typing
import os

import click
import pandas as pd

from src import normalization, log_utils
from src.constants import keys

LOG = log_utils.configure_logger()


@click.group(help="Normalizes and unpacks the github data into a flat JSON format")
def cli():
    pass


def create_output_path(input_path: str, command: str) -> str:
    filename = os.path.splitext(os.path.basename(input_path))[0]
    if filename.startswith('raw_'):
        filename = filename[4:]
    return f"data/{command}_{filename}.txt"


def normalize_this(input: str, output: str, fun: typing.Callable[[dict], pd.DataFrame]):
    dfs = []
    with open(input, 'r') as f:
        for line in f.readlines():
            obj = json.loads(line)
            df = fun(obj)
            if not df.empty:
                dfs.append(df)

    if not dfs:
        LOG.info('No output data')
        return
    full_df = pd.concat(dfs)
    print(f"Writing '{output}'")
    full_df.to_json(output, lines=True, orient='records')


@cli.command(help='extracts flat commit data')
@click.argument("input", type=str)
def extract_commits(input: str):
    output = create_output_path(input, "commits")

    def fun(record: dict) -> pd.DataFrame:
        record['author'] = record['author']['user']
        record['committer'] = record['committer']['user']
        normalization.flatten_recursively_inplace(record, ['author'])
        normalization.flatten_recursively_inplace(record, ['committer'])
        return pd.Series(record).to_frame().T

    normalize_this(input, output, fun)


@cli.command(help='extracts all comments from each PR')
@click.argument("input", type=str)
def extract_pr_comments(input: str):
    output = create_output_path(input, "comments")

    def fun(obj: dict) -> pd.DataFrame:
        record = obj['node']
        normalization.flatten_recursively_inplace(record, ['author'])
        return normalization.pr_extract_comments(record)

    normalize_this(input, output, fun)


@cli.command(help='extracts all review info from each PR')
@click.argument("input", type=str)
def extract_pr_reviews(input: str):
    output = create_output_path(input, "reviews")

    def fun(obj: dict) -> pd.DataFrame:
        record = obj[keys.NODE]
        normalization.flatten_recursively_inplace(record, [keys.AUTHOR])
        return normalization.pr_extract_reviews(record)

    normalize_this(input, output, fun)


@cli.command(help='extracts all review thread info from each PR')
@click.option("-i", "input", type=str, required=True)
def extract_pr_review_threads(input: str):
    output = create_output_path(input, "threads")

    def fun(obj: dict) -> pd.DataFrame:
        record = obj['node']
        normalization.flatten_recursively_inplace(record, ['author'])
        return normalization.pr_extract_review_threads(record)

    normalize_this(input, output, fun)


@cli.command(help='extracts all review and review thread info from each PR')
@click.argument("input", type=str)
def extract_pr_all_reviews(input: str):
    output = create_output_path(input, "all-reviews")

    def fun(obj: dict) -> pd.DataFrame:
        record = obj['node']
        normalization.flatten_recursively_inplace(record, ['author'])
        reviews_df = normalization.pr_extract_reviews(record)
        review_threads_df = normalization.pr_extract_review_threads(record)
        return pd.concat([reviews_df, review_threads_df])

    LOG.warning('Due to Github\'s rate limiting we collect only a subset of all review & review threads')
    normalize_this(input, output, fun)


@cli.command(help="extracts labels associated with a PR")
@click.argument("input", type=str)
def extract_pr_labels(input: str):
    output = create_output_path(input, "labels")

    def fun(obj: dict) -> pd.DataFrame:
        record = obj['node']
        normalization.flatten_recursively_inplace(record, ['author'])
        return normalization.pr_extract_labels(record)

    normalize_this(input, output, fun)


@cli.command(help="extracts a single line per PR")
@click.argument("input", type=str)
def extract_pr_flat(input: str):
    output = create_output_path(input, "pr-flat")

    def fun(obj: dict) -> pd.DataFrame:
        record = obj['node']
        normalization.flatten_recursively_inplace(record, ['author'])
        return normalization.extract_pr_flat(record)

    normalize_this(input, output, fun)


if __name__ == '__main__':
    cli()