from math import ceil
import os
from types import NoneType
from dateutil.parser import parse
from utils.graphql import GraphQL
from utils.mongo import Mongo

PER_PAGE = 50

def run():
    repos_count = Mongo().get_documents_count("repo")
    processed_count = Mongo().get_processed_documents_count("repo")

    repos = [Mongo().get_all_documents("repo")[0]]

    if processed_count < repos_count:

        graphql = GraphQL(os.environ["API_URL"])
        last_cursor = None

        for repo in repos:
            if repo["processed"] is False:

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
                                
                    formatter = lambda node : {
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

                    nodes = list(map(formatter, response["data"]["repository"]["pullRequests"]["nodes"]))

                    for node in nodes:
                        if int(node["reviews"]) >= 1 and (node["closed"] or node["merged"]):
                            if min(node["mergeTime"], node["closeTime"]) >= 60:
                                Mongo().insert_one(node, "pr")

                                print(f'{repo["name"]} - Pull request data added to DB')


                    if not response["data"]["repository"]["pullRequests"]["pageInfo"]["hasNextPage"]:
                        print(f'{repo["name"]} - All pull requests analysed')
                        Mongo().update_one({'url': repo["url"]}, {'$set' : { 'processed': True } })
                        query_ended = True
                        break
            else:
                print(f'{repo["name"]} - Repositories had already been processed')
    else:
        print(f'All {repos_count} repositories had already been processed')