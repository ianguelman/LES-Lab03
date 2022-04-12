import os
import requests
import json

class GraphQL:
    
    api: str
    tokens = []
    
    def __init__(self, api):
        self.api = api
        self.tokens = os.environ.get('TOKENS').split(',')
        
    def post(self, query, variables, retries):
        token = self.tokens[retries % len(self.tokens)]
        request = requests.post(
            self.api, json={'query': query, "variables": variables}, 
            headers={'Authorization': f'bearer {token}'}
        )
        if request.status_code == 200:
            return json.loads(request.text)
        elif request.status_code == 502:
            token
            retries += 1
            self.post(query, variables, retries)
        else:
            raise Exception(f'Query failed, with status code {request.status_code}')


        