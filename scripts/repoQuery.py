from math import ceil
import os
from types import NoneType
from utils.graphql import GraphQL
from utils.mongo import Mongo

TOTAL_ITEMS = 100
PER_PAGE = 25

def run():
    items_count = Mongo().get_documents_count("repo")

    if items_count < TOTAL_ITEMS:

        graphql = GraphQL(os.environ["API_URL"])
        last_cursor = None

        while items_count < TOTAL_ITEMS:
            response = graphql.post(
                """
                query popularRepositories($lastCursor: String, $perPage: Int) {
                    search(
                        query: "stars:>100"
                        type: REPOSITORY
                        after: $lastCursor
                        first: $perPage
                    ) {
                        nodes {
                        ... on Repository {
                            url
                            name
                            owner {
                                login
                            }
                            stargazerCount
                            mergedPr: pullRequests(first: 1, states: MERGED) {
                                totalCount
                            }
                            closedPr: pullRequests(first: 1, states: CLOSED) {
                                totalCount
                            }
                        }
                        }
                        pageInfo {
                        endCursor
                        hasNextPage
                        }
                    }
                    }
                """,
                {
                    "lastCursor": last_cursor,
                    "perPage": PER_PAGE,
                },
            )
            
            last_cursor = response["data"]["search"]["pageInfo"]["endCursor"]
                        
            formatter = lambda node : {
                "url" : node["url"],
                "name": node["name"],
                "owner": node["owner"]["login"],
                "stargazerCount": node["stargazerCount"],
                "mergedPr": node["mergedPr"]["totalCount"],
                "closedPr": node["closedPr"]["totalCount"],
                "processed": False
            }

            nodes = list(map(formatter, response["data"]["search"]["nodes"]))

            for node in nodes:
                if int(node["mergedPr"]) + int(node["closedPr"]) >= 100:
                    Mongo().insert_one(node, "repo")

                    items_count = Mongo().get_documents_count("repo")
                    print(f'{items_count} nodes of {TOTAL_ITEMS}')

                    if items_count >= TOTAL_ITEMS:
                        break

            if not response["data"]["search"]["pageInfo"]["hasNextPage"]:
                break

    else:
        print(f"DB already populated with {items_count} repos")