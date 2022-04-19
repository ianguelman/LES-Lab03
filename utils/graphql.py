import os
import requests
import json
import time

class GraphQL:
    
    api: str
    tokens = []
    retries: int
    
    def __init__(self, api):
        self.api = api
        self.tokens = os.environ.get('TOKENS').split(',')
        self.retries = 0
        
    def post(self, query, variables):
        token = self.tokens[self.retries]
        request = requests.post(
            self.api, json={'query': query, "variables": variables}, 
            headers={'Authorization': f'bearer {token}'}
        )
        time.sleep(0.1)
        if request.status_code == 200:
            return json.loads(request.text)
        elif request.status_code == 502:
            self.retries = self.retries + 1 if self.retries + 1 < len(self.tokens) else 0 
            print("Retrying query...")
            self.post(query, variables)
        else:
            raise Exception(f'Query failed, with status code {request.status_code}')
        


        