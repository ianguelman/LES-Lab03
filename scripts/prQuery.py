from math import ceil
import os
import sys
from types import NoneType
from dateutil.parser import parse
from utils.graphql import GraphQL
from utils.mongo import Mongo

PER_PAGE = 50


def run():
    repos_count = Mongo().get_documents_count("repo")
    processed_count = Mongo().get_processed_documents_count("repo")

    repos = Mongo().get_all_documents("repo")

    if processed_count < repos_count:

        graphql = GraphQL(os.environ["API_URL"])
        last_cursor = None

        for repo in repos:
            if repo["processed"] is False:

                prAnalysed = 0
                query_ended = False

                while not query_ended:
                    
                    response = graphql.post(
                        """
                        query pullRequests($name: String!, $owner: String!, $perPage: Int, $lastCursor: String) {
                            repository(name: $name, owner: $owner) {
                                pullRequests(first: $perPage, after: $lastCursor) {
                                totalCount
                                nodes {
                                    reviews(first: 1) {
                                        totalCount
                                    }
                                    merged
                                    closed
                                    createdAt
                                    mergedAt
                                    closedAt
                                    files(first: 1) {
                                        totalCount
                                    }
                                    bodyText
                                    changedFiles
                                    participants(first: 1) {
                                        totalCount
                                    }
                                    comments(first: 1) {
                                        totalCount
                                    }
                                }
                                pageInfo {
                                    hasNextPage
                                    endCursor
                                }
                                }
                            }
                            }
                        """,
                        {
                            "name": repo["name"],
                            "owner": repo["owner"],
                            "lastCursor": last_cursor,
                            "perPage": PER_PAGE,
                        },
                    )

                    last_cursor = response["data"]["repository"]["pullRequests"]["pageInfo"]["endCursor"]
                    prCount = response["data"]["repository"]["pullRequests"]["totalCount"]

                    def formatter(node): return {
                        "reviews": node["reviews"]["totalCount"],
                        "merged": node["merged"],
                        "closed": node["closed"],
                        "createdAt": node["createdAt"],
                        "mergedAt": node["mergedAt"],
                        "mergeTime": (parse(node["mergedAt"]).replace(tzinfo=None) - parse(node["createdAt"]).replace(tzinfo=None)).seconds * 60 if node["merged"] else None,
                        "closedAt": node["closedAt"],
                        "closeTime": (parse(node["closedAt"]).replace(tzinfo=None) - parse(node["createdAt"]).replace(tzinfo=None)).seconds * 60 if node["closed"] else None,
                        "files": node["files"]["totalCount"],
                        "bodySize": len(node["bodyText"]),
                        "changedFiles": node["changedFiles"],
                        "participants": node["participants"],
                        "comments": node["comments"],
                    }

                    nodes = list(
                        map(formatter, response["data"]["repository"]["pullRequests"]["nodes"]))

                    for node in nodes:
                        prAnalysed += 1
                        if int(node["reviews"]) >= 1 and (node["closed"] or node["merged"]):
                            if min(node["mergeTime"] if node["merged"] else sys.maxsize, node["closeTime"] if node["closed"] else sys.maxsize,) >= 60:
                                Mongo().insert_one(node, "pr")

                                print(
                                    f'{repo["name"]} - Pull request data added to DB - {prAnalysed} pull requests analysed out of {prCount} ({prCount - prAnalysed} left)')

                    if not response["data"]["repository"]["pullRequests"]["pageInfo"]["hasNextPage"]:
                        reposLen = repos.len()
                        repoIndex = repos.index(repo)
                        print(f'{repo["name"]} - All pull requests analysed - {repoIndex} repositories analysed out of {reposLen} ({reposLen - repoIndex} left)')
                        Mongo().update_one({'url': repo["url"]}, {
                            '$set': {'processed': True}})
                        query_ended = True
                        break
            else:
                print(
                    f'{repo["name"]} - Repository had already been processed')
    else:
        print(f'All {repos_count} repositories had already been processed')
