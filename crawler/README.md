# Github Crawler

The Github crawler is used to download and normalize github data.

## Setup

You need to create the file `secret` with the following structure:

```
OAUTH_TOKEN: <your-token>
```

You can get a token here: https://github.com/settings/tokens

## Usage

Download last 300 PRs for Apache Flink:

Notes:
- Without `-l <int>`, this downloads all data
- The output path is automatically created for easier data lineage

Normalize the data into a flat JSON format:

    python normalize.py extract-pr-flat data/raw_20220602-13h35m41s_apache_flink_master_prs-brief.txt

See the help options of the tools for more information.

## Dependencies

- gql
- anytree
- pandas
- click
- requests
- requests_toolbelt

## Future work

Currently we collect the full data set. This can take several minutes. For
future work we would prefer to collect only the delta of new information.

For this we need to either store GraphQL cursors or match ids (commit ids or PR
numbers).

- unify the order of crawling from most recent to oldest
- combine both normalization and downloading into a single process
- store cursor, last id and filepath into a description file
- move execution code into library (so that we can call it from the ipython
    notebook)
