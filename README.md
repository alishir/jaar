# Env. Setup
```
$ python3 -m venv .venv
$ . .venv/bin/activate.fish
$ pip install requirments.txt
```

# Crawl
```
$ scrapy crawl divar -o divar_posts.jsonlines
```

# Index
```
# download and run meilisearch

$ curl -X POST 'http://localhost:7700/indexes/posts/documents' \
     -H 'Content-Type: application/x-ndjson' \
     --data-binary @divar_posts.jsonlines
```
