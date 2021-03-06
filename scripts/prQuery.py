from math import ceil
import os
import sys
from types import NoneType
from dateutil.parser import parse
from utils.graphql import GraphQL
from utils.mongo import Mongo

PER_PAGE = 10


def run():
    repos_count = Mongo().get_documents_count("repo")
    processed_count = Mongo().get_processed_documents_count("repo")

    repos = Mongo().get_all_documents("repo")

    if processed_count < repos_count:

        graphql = GraphQL(os.environ["API_URL"])
        last_cursor = Mongo().get_document({'_id': 'lastCursor'}, "config")['value']

        for repo in repos:
            if repo["processed"] is False:

                prAnalysed = 0
                query_ended = False

                while not query_ended:
                    
                    response = graphql.post(
                        """
                        query pullRequests($name: String!, $owner: String!, $perPage: Int, $lastCursor: String) {
                            repository(name: $name, owner: $owner) {
                                pullRequests(first: $perPage, after: $lastCursor, states: [MERGED,CLOSED]) {
                                totalCount
                                nodes {
                                    id
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
                                    body
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
                        }
                    )
                    if response != None and "data" in response:
                        last_cursor = response["data"]["repository"]["pullRequests"]["pageInfo"]["endCursor"]
                        Mongo().update_one({'_id': 'lastCursor'}, {'$set': {'value': last_cursor}}, "config")
                        prCount = response["data"]["repository"]["pullRequests"]["totalCount"]

                        def formatter(node): return {
                            "_id": node["id"],
                            "repoName": repo["name"],
                            "reviews": node["reviews"]["totalCount"],
                            "merged": node["merged"],
                            "closed": node["closed"],
                            "createdAt": node["createdAt"],
                            "mergedAt": node["mergedAt"],
                            "mergeTimeHours": (parse(node["mergedAt"]).replace(tzinfo=None) - parse(node["createdAt"]).replace(tzinfo=None)).total_seconds() / 60 / 60 if node["merged"] else None, 
                            "closedAt": node["closedAt"],
                            "closeTimeHours": (parse(node["closedAt"]).replace(tzinfo=None) - parse(node["createdAt"]).replace(tzinfo=None)).total_seconds() / 60 / 60 if node["closed"] else None,
                            "files": node["files"]["totalCount"],
                            "bodySize": len(node["body"]),
                            "changedFiles": node["changedFiles"],
                            "participants": node["participants"]["totalCount"],
                            "comments": node["comments"]["totalCount"],
                        }

                        nodes = list(
                            map(formatter, response["data"]["repository"]["pullRequests"]["nodes"]))

                        for node in nodes:
                            prAnalysed += 1
                            if int(node["reviews"]) >= 1 and (node["closed"] or node["merged"]):
                                if min(node["mergeTimeHours"] if node["merged"] else sys.maxsize, node["closeTimeHours"] if node["closed"] else sys.maxsize,) >= 1:
                                    Mongo().update_one({'_id': node['_id']}, {'$set': node}, "pr")

                                    print(
                                        f'{repo["name"]} - Pull request data added to DB - {prAnalysed} pull requests analysed out of {prCount} ({prCount - prAnalysed} left)')

                        if not response["data"]["repository"]["pullRequests"]["pageInfo"]["hasNextPage"]:
                            reposLen = len(repos)
                            repoIndex = repos.index(repo)
                            print(f'{repo["name"]} - All pull requests analysed - {repoIndex} repositories analysed out of {reposLen} ({reposLen - repoIndex} left)')
                            Mongo().update_one({'url': repo["url"]}, {
                                '$set': {'processed': True}},
                                "repo"
                                )
                            Mongo().update_one({'_id': 'lastCursor'}, {'$set': {'value': None}}, "config")
                            query_ended = True
                            break
            else:
                print(
                    f'{repo["name"]} - Repository had already been processed')
    else:
        print(f'All {repos_count} repositories had already been processed')
