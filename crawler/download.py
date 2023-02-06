import logging

import click

from src import data_access, log_utils, utils, traversal
from src.collector import JsonWriter, DataCollectorBuilder
from src.constants import queries, keys
from src.graphl_client import GraphQLClient
from datetime import datetime
from collections import OrderedDict

LOG = log_utils.configure_logger()


def init_builder(config: dict, resume: bool) -> DataCollectorBuilder:
    gql_client = GraphQLClient(config)
    builder = DataCollectorBuilder() \
        .add_client(gql_client)
    if resume:
        builder = builder.enable_resume()
    return builder


@click.group(help="Downloads github analytics data")
@click.option("-v", "--verbose", is_flag=True)
@click.option("-r", "--resume", is_flag=True, help="resumes from last cursor and appends to file")
@click.option("organization", "--orga", type=str, required=True, help="organization (e.g., apache)")
@click.option("project", "--proj", type=str, required=True, help="project (e.g., flink)")
@click.option("--branch", type=str, required=True, default='master', help="branch (main, master, trunk, etc.)")
@click.option("-l", "--limit", type=int, default=0)
@click.pass_context
def cli(ctx, verbose: bool, resume: bool, organization: str, project: str, branch: str, limit: int):
    if verbose:
        log_utils.change_log_level(logging.DEBUG)
    ctx.ensure_object(dict)
    ctx.obj['config'] = OrderedDict()
    ctx.obj['config']['owner'] = organization
    ctx.obj['config']['repository'] = project
    ctx.obj['config']['branch'] = branch
    ctx.obj['limit'] = limit
    ctx.obj['resume'] = resume


def create_output_path(ctx) -> str:
    # let's try it without date
    # date = datetime.now().strftime("%Y%m%d-%Hh%Mm%Ss")
    command = ctx.command.name
    if command.startswith('get-'):
        command = command[4:]
    config = ctx.obj['config']
    elements = [command]
    for k, v in config.items():
        elements.append(f'{k}-{v}')
    if ctx.obj['limit']:
        elements.append("limit{ctx.obj['limit']}")
    filename = "_".join(elements)
    return f"data/{filename}.txt"


@cli.command(help="download commits (fast, from most recent)")
@click.pass_context
def get_commits(ctx):
    builder = init_builder(ctx.obj['config'], ctx.obj['resume'])
    path = create_output_path(ctx)
    query = utils.load_query(queries.COMMITS)
    LOG.debug('query: %s', query)
    writer = JsonWriter(path, append=ctx.obj['resume'])
    LOG.info("create collector")
    builder: DataCollectorBuilder = builder
    base_path = data_access.AccessPathBuilder().add(keys.REPOSITORY).add(keys.REF).add(keys.TARGET).add(keys.HISTORY)
    record_path = base_path.copy().add(keys.NODES).build()
    cursor = traversal.Cursor(base_path.copy().add(keys.PAGE_INFO).build())
    traverser = traversal.CursorGenerator(cursor)
    collector = builder \
        .add_query(query)\
        .add_record_callback(writer.add)\
        .add_records_access(record_path)\
        .add_cursor_generator(traverser)\
        .add_limit(ctx.obj['limit'])\
        .build()
    collector.run()
    writer.close()


@cli.command(help="download PRs with only very limited information per PR (fast, from oldest)")
@click.pass_context
def get_prs_brief(ctx):
    builder = init_builder(ctx.obj['config'], ctx.obj['resume'])
    path = create_output_path(ctx)
    query = utils.load_query(queries.PRS)
    LOG.debug('query: %s', query)
    writer = JsonWriter(path, append=ctx.obj['resume'])
    LOG.info("create collector")
    builder = builder
    base_path = data_access.AccessPathBuilder().add(keys.REPOSITORY).add(keys.PULL_REQUESTS)
    record_path = base_path.copy().add(keys.EDGES).build()
    cursor = traversal.Cursor(base_path.copy().add(keys.PAGE_INFO).build())
    traverser = traversal.CursorGenerator(cursor)
    collector = builder \
        .add_query(query) \
        .add_record_callback(writer.add) \
        .add_records_access(record_path) \
        .add_cursor_generator(traverser) \
        .add_limit(ctx.obj['limit']) \
        .build()
    collector.run()
    writer.close()


def get_user_id(login: str) -> None:
    # TODO: Move somewhere else
    from gql import gql, Client
    from gql.transport.requests import RequestsHTTPTransport
    from src.constants import constants

    with open('graphql/query_user.graphql', 'r') as f:
        query = f.read()

    token = utils.load_secret()
    transport = RequestsHTTPTransport(
        url=constants.URL_GITHUB_GRAPHQL, headers={"Authorization": "bearer " + token}, verify=True, retries=3,
    )
    client = Client(transport=transport, fetch_schema_from_transport=True)
    result = client.execute(gql(query), variable_values={'userLogin': login})
    return result['user']['id']


@cli.command(help="download user info")
@click.option("--login", type=str)
@click.option("--since", default="", type=str)
@click.pass_context
def get_user_commits(ctx, login: str, since: str) -> None:
    config = ctx.obj['config']
    config['login'] = login
    path = create_output_path(ctx)
    user_id = get_user_id(login)
    config['userId'] = {'id': user_id}
    if since:
        config['since'] = since
    builder = init_builder(config, ctx.obj['resume'])
    query = utils.load_query("query_commits_by_user.graphql")
    LOG.debug('query: %s', query)
    writer = JsonWriter(path, append=ctx.obj['resume'])
    LOG.info("create collector")
    builder = builder
    base_path = data_access.AccessPathBuilder().add(keys.REPOSITORY).add(keys.REF).add(keys.TARGET).add(keys.HISTORY)
    record_path = base_path.copy().add(keys.NODES).build()
    cursor = traversal.Cursor(base_path.copy().add(keys.PAGE_INFO).build())
    traverser = traversal.CursorGenerator(cursor)
    collector = builder \
        .add_query(query) \
        .add_record_callback(writer.add) \
        .add_records_access(record_path) \
        .add_cursor_generator(traverser) \
        .add_limit(ctx.obj['limit']) \
        .build()
    collector.run()
    writer.close()


@cli.command(help="download PRs with extensive information on comments, reviews, etc. (slow, from oldest)")
@click.pass_context
def get_prs_long(ctx):
    builder = init_builder(ctx.obj['config'], ctx.obj['resume'])
    path = create_output_path(ctx)
    query = utils.load_query(queries.PRS_FULL)
    LOG.debug('query: %s', query)
    writer = JsonWriter(path, append=ctx.obj['resume'])
    LOG.info("create collector")
    builder = builder
    base_path = data_access.AccessPathBuilder().add(keys.REPOSITORY).add(keys.PULL_REQUESTS)
    record_path = base_path.copy().add(keys.EDGES).build()
    cursor = traversal.Cursor(base_path.copy().add(keys.PAGE_INFO).build())
    traverser = traversal.CursorGenerator(cursor)
    collector = builder \
        .add_query(query) \
        .add_record_callback(writer.add) \
        .add_records_access(record_path) \
        .add_cursor_generator(traverser) \
        .add_limit(ctx.obj['limit'])\
        .build()
    collector.run()
    writer.close()


@cli.command(help="download only review metadata for each PR (slow, from most recent)")
@click.pass_context
def get_pr_reviews(ctx):
    builder = init_builder(ctx.obj['config'], ctx.obj['resume'])
    path = create_output_path(ctx)
    query = utils.load_query(queries.PRS_REVIEWS)
    LOG.debug('query: %s', query)
    writer = JsonWriter(path, append=ctx.obj['resume'])
    base_path = data_access.AccessPathBuilder().add(keys.REPOSITORY).add(keys.PULL_REQUESTS)
    review_path = base_path.copy().add(keys.EDGES).add(0).add(keys.NODE).add(keys.REVIEWS)
    cursor_root = traversal.Cursor(
        base_path.copy().add(keys.PAGE_INFO).build(),
        has_next='hasPreviousPage',
        cursor_name='startCursor',
        variable_name='cursorTop'
    )
    traversal.Cursor(
        review_path.add(keys.PAGE_INFO).build(),
        variable_name='cursorReviews',
        parent=cursor_root
    )
    traverser = traversal.CursorGenerator(cursor_root)
    LOG.info("create collector")
    builder: DataCollectorBuilder = builder
    collector = builder \
        .add_query(query) \
        .add_record_callback(writer.add) \
        .add_records_access(base_path.copy().add(keys.EDGES).build()) \
        .add_cursor_generator(traverser) \
        .add_limit(ctx.obj['limit']) \
        .build()
    collector.run()
    writer.close()


@cli.command(help="download only review thread metadata for each PR (slow, from most recent)")
@click.pass_context
def get_pr_review_threads(ctx):
    builder = init_builder(ctx.obj['config'], ctx.obj['resume'])
    path = create_output_path(ctx)
    query = utils.load_query(queries.PRS_REVIEW_THREADS)
    LOG.debug('query: %s', query)
    writer = JsonWriter(path, append=ctx.obj['resume'])
    base_path = data_access.AccessPathBuilder().add(keys.REPOSITORY).add(keys.PULL_REQUESTS)
    review_thread_path = base_path.copy().add(keys.EDGES).add(0).add(keys.NODE).add(keys.REVIEW_THREADS)
    cursor_root = traversal.Cursor(
        base_path.copy().add(keys.PAGE_INFO).build(),
        has_next='hasPreviousPage',
        cursor_name='startCursor',
        variable_name='cursorTop'
    )
    cursor_review_threads = traversal.Cursor(
        review_thread_path.copy().add(keys.PAGE_INFO).build(),
        variable_name='cursorReviewThreads',
        parent=cursor_root
    )
    traversal.Cursor(
        review_thread_path.copy().add(keys.NODES).add(0).add(keys.COMMENTS).add(keys.PAGE_INFO).build(),
        variable_name='cursorReviewThreadComments',
        parent=cursor_review_threads
    )
    traverser = traversal.CursorGenerator(cursor_root)
    LOG.info("create collector")
    builder: DataCollectorBuilder = builder
    collector = builder \
        .add_query(query) \
        .add_record_callback(writer.add) \
        .add_records_access(base_path.copy().add(keys.EDGES).build()) \
        .add_cursor_generator(traverser) \
        .add_limit(ctx.obj['limit']) \
        .build()
    collector.run()
    writer.close()


if __name__ == '__main__':
    cli()
