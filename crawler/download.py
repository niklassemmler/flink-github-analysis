import logging

import click

from src import data_access, log_utils, utils, traversal
from src.collector import JsonWriter, DataCollectorBuilder
from src.constants import queries, keys
from src.graphl_client import GraphQLClient
from datetime import datetime

LOG = log_utils.configure_logger()


@click.group(help="Downloads github analytics data")
@click.option("-v", "--verbose", is_flag=True)
@click.option("-r", "--resume", is_flag=True, help="resumes from last cursor and appends to file")
@click.option("organization", "--orga", type=str, required=True, help="organization (e.g., apache)")
@click.option("project", "--proj", type=str, required=True, help="project (e.g., flink)")
@click.option("--branch", type=str, required=True, default='master', help="branch (main, master, trunk, etc.)")
@click.option("-l", "limit", type=int, default=0)
@click.pass_context
def cli(ctx, verbose: bool, resume: bool, organization: str, project: str, branch: str, limit: int):
    if verbose:
        log_utils.change_log_level(logging.DEBUG)
    ctx.ensure_object(dict)
    ctx.obj['organization'] = organization
    ctx.obj['project'] = project
    ctx.obj['branch'] = branch
    ctx.obj['limit'] = limit
    gql_parameters = {'owner': organization, 'repostiory': project, 'branch': branch}
    gql_client = GraphQLClient(gql_parameters)
    ctx.obj['client'] = gql_client
    builder = DataCollectorBuilder() \
        .add_client(gql_client)
    ctx.obj['append'] = False
    if resume:
        builder = builder.enable_resume()
        ctx.obj['append'] = True
    ctx.obj['builder'] = builder


def create_output_path(ctx) -> str:
    date = datetime.now().strftime("%Y%m%d-%Hh%Mm%Ss")
    command = ctx.command.name
    if command.startswith('get-'):
        command = command[4:]
    filename = f"raw_{date}_{ctx.obj['organization']}_{ctx.obj['project']}_{ctx.obj['branch']}_{command}"
    if ctx.obj['limit']:
        filename = f"{filename}_limit{ctx.obj['limit']}"
    return f"data/{filename}.txt"


@cli.command(help="download commits (fast, from most recent)")
@click.pass_context
def get_commits(ctx):
    path = create_output_path(ctx)
    query = utils.load_query(queries.COMMITS)
    LOG.debug('query: %s', query)
    writer = JsonWriter(path, append=ctx.obj['append'])
    LOG.info("create collector")
    builder: DataCollectorBuilder = ctx.obj['builder']
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
    path = create_output_path(ctx)
    query = utils.load_query(queries.PRS)
    LOG.debug('query: %s', query)
    writer = JsonWriter(path, append=ctx.obj['append'])
    LOG.info("create collector")
    builder = ctx.obj['builder']
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


@cli.command(help="download PRs with extensive information on comments, reviews, etc. (slow, from oldest)")
@click.pass_context
def get_prs_long(ctx):
    path = create_output_path(ctx)
    query = utils.load_query(queries.PRS_FULL)
    LOG.debug('query: %s', query)
    writer = JsonWriter(path, append=ctx.obj['append'])
    LOG.info("create collector")
    builder = ctx.obj['builder']
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
    path = create_output_path(ctx)
    query = utils.load_query(queries.PRS_REVIEWS)
    LOG.debug('query: %s', query)
    writer = JsonWriter(path, append=ctx.obj['append'])
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
    builder: DataCollectorBuilder = ctx.obj['builder']
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
    path = create_output_path(ctx)
    query = utils.load_query(queries.PRS_REVIEW_THREADS)
    LOG.debug('query: %s', query)
    writer = JsonWriter(path, append=ctx.obj['append'])
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
    builder: DataCollectorBuilder = ctx.obj['builder']
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
