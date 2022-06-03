class constants:
    FILE_NAME_SECRET = "secret"
    SECRET_REGEX = r"[\t\s]*OAUTH_TOKEN[\t\s]*:[\t\s]*(.*)"
    URL_GITHUB_GRAPHQL = "https://api.github.com/graphql"


class queries:
    COMMITS = "query_commits.graphql"
    PRS = "query_pull_requests_brief.graphql"
    PRS_FULL = "query_pull_requests_long.graphql"
    PRS_REVIEWS = "query_pull_requests_reviews.graphql"
    PRS_REVIEW_THREADS = "query_pull_requests_review_threads.graphql"
    COLLABORATORS = "query_collaborators.graphql"
    COLLABORATOR = "query_collaborator.graphql"


class keys:
    FIRST_COMMENT_PREFIX = 'firstComment'
    FIRST_REVIEW_PREFIX = 'firstReview'
    FIRST_REVIEW_THREAD_PREFIX = 'firstReviewThread'
    LABELS = 'labels'
    NUMBER = 'number'
    REPOSITORY = 'repository'
    REF = 'ref'
    TARGET = 'target'
    HISTORY = 'history'
    PAGE_INFO = 'pageInfo'
    END_CURSOR = 'endCursor'
    START_CURSOR = 'startCursor'
    NODE = 'node'
    NODES = 'nodes'
    EDGE = 'edge'
    EDGES = 'edges'
    PULL_REQUESTS = 'pullRequests'
    PULL_REQUEST = 'pullRequest'
    REVIEWS = 'reviews'
    REVIEW_THREADS = 'reviewThreads'
    AUTHOR = 'author'
    COMMITTER = 'committer'
    USER = 'user'
    HAS_NEXT_PAGE = 'hasNextPage'
    CREATED_AT = 'createdAt'
    PUBLISHED_AT = 'createdAt'
    COMMENTS = 'comments'
    HAS_REVIEW = 'hasReviews'
    HAS_THREADED_REVIEW = 'hasThreadedReview'
    HAS_REGULAR_REVIEW = 'hasRegularReviews'
    FIRST_THREADED_REVIEW = 'firstThreadedReview'
    FIRST_REGULAR_REVIEW = 'firstRegularReview'
    COLLABORATORS = 'collaborators'
    ORGANIZATIONS = 'organizations'


class Files:
    BACKUP_FILE = 'backup.json'
