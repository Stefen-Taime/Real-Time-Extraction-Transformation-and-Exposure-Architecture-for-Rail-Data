from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from elasticsearch import Elasticsearch
from typing import Optional

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],  
    allow_headers=["*"],  
)

es = Elasticsearch("http://localhost:9200")

@app.get("/data/")
async def get_data(pagination_token: Optional[str] = None):
    body = {
        "query": {"match_all": {}},
        "sort": [{"_id": "asc"}],  
        "size": 100
    }
    if pagination_token:
        body["search_after"] = [pagination_token]

    response = es.search(index="enriched_update", body=body)
    results = [hit["_source"] for hit in response['hits']['hits']]
    last_hit = response['hits']['hits'][-1] if response['hits']['hits'] else None
    next_pagination_token = last_hit["sort"][0] if last_hit else None
    return {"data": results, "next_pagination_token": next_pagination_token}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
