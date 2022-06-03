from typing import Dict, Any

from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport

from . import log_utils, utils
from .constants import constants

LOG = log_utils.configure_logger()


class GraphQLClient:
    def __init__(self, default_variables: Dict[str, Any] = None):
        LOG.debug("initialize")
        token = utils.load_secret()
        transport = RequestsHTTPTransport(
            url=constants.URL_GITHUB_GRAPHQL, headers={"Authorization": "bearer " + token}, verify=True, retries=3,
        )
        self._default_variables = default_variables if default_variables else {}
        self._client = Client(transport=transport, fetch_schema_from_transport=True)

    def send_graphql_query(self, query: str, variable_values: dict = None, *args, **kwargs) -> dict:
        LOG.debug("send query %s", query)
        variable_values.update(self._default_variables)
        LOG.debug(f"variable values {variable_values}")
        return self._client.execute(gql(query), variable_values=variable_values, *args, **kwargs)
