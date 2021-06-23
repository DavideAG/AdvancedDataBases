# Artificial Intelligence & Cloud
ElasticSearch project  
Davide Antonino Giorgio  
s291466

## Data Ingestion


```python
import json 
from tqdm.notebook import tqdm
import threading
from elasticsearch import Elasticsearch
from multiprocessing.pool import ThreadPool
```


```python
es = Elasticsearch('127.0.0.1', port=9200)

''' Reading JSON file '''
fr = open("imdb.json", "r", encoding="utf-8")
text = fr.read()
fr.close()

''' Creating required index '''
try:
    es.indices.delete(index='project_3', ignore=[400, 404])
except Exception as e:
    pass

create_index = es.indices.create(index='project_3', ignore=400)

jd = json.loads(text)
```


```python
def parse_and_load(jd, pos):
    for item in tqdm(jd, position=pos):
        try:
            ''' Retrieving required info '''
            d = {}

            for field_str in ['title', 'plot', 'fullplot']:
                try: d[field_str] = item[field_str]
                except Exception as e: d[field_str] = ''

            try: d['year'] = int(item['year'])
            except Exception as e: d['year'] = 0

            try: d['imdb_rating'] = float(item['imdb']['rating'])
            except Exception as e: d['imdb_rating'] = 0.0

            try: d['tomatoes_rating'] = float(item['tomatoes']['viewer']['rating'])
            except Exception as e: d['tomatoes_rating'] = 0.0

            for lst in ['directors', 'countries', 'genres']:
                try: d[lst] = item[lst]
                except Exception as e: d[lst] = []

            res = es.index(index="project_3", body=d)

        except Exception as e:
            print (e)
```


```python
def chunk_list(in_lst, chunk_size):
    for i in range(0, len(in_lst), chunk_size):
        yield in_lst[i:i + chunk_size]
```


```python
N_THREADS = 2
chunks = chunk_list(jd, len(jd)//N_THREADS)
threads = list()

for th_i, chunk in zip(range(N_THREADS), chunks):
    threads.append(threading.Thread(target=parse_and_load, args=(chunk,th_i,)))
    threads[th_i].start()

for thread in threads:
    thread.join() 
```


      0%|          | 0/22969 [00:00<?, ?it/s]



      0%|          | 0/22969 [00:00<?, ?it/s]



```python
es.indices.refresh('project_3')
es.cat.count('project_3', params={"format": "json"})
```




    [{'epoch': '1616795961', 'timestamp': '21:59:21', 'count': '45938'}]



## Interest queries


```python
# Get the list of the first 10 movies that have been published
# between 1939 and 1945 and best matches the word “war” in the plot.
res = es.search(index="project_3", body={
"query": {
    "bool":{
        "must":[{
            "match":{
                "plot": "war" 
            }},{
            "range": {
                "year": {
                    "gte": 1939,
                    "lte": 1945
                }
            }}
        ]
    }
},
"size": 10
})

for doc in res['hits']['hits']:
    print (doc["_score"], doc["_source"]["title"])
```

    5.96776 The Life and Death of Colonel Blimp
    5.8774576 Story of G.I. Joe
    5.803811 The Battle of Britain
    5.7515407 The Nazis Strike
    5.5533576 Divide and Conquer
    5.5533576 Divide and Conquer
    5.371045 Random Harvest
    5.3277254 Ichiban utsukushiku
    5.238765 The Well-Digger's Daughter
    5.2027693 Listen to Britain
    


```python
# Get the list of the first 20 movies that have an average IMDB
# rating higher than 6.0 and best matches “Iron Man” in the full plot.
res = es.search(index="project_3", body={
"query": {
    "bool":{
        "must":[{
            "match":{
                "fullplot": "Iron Man" 
            }},{
            "range": {
                "imdb_rating": {
                    "gt": 6.0
                }
            }}
        ]
    }
},
"size": 20
})

for doc in res['hits']['hits']:
    print (doc["_score"], doc["_source"]["title"])
```

    11.599296 Iron Man 3
    11.04736 With Great Power: The Stan Lee Story
    10.932037 Tetsuo II: Body Hammer
    10.29641 Drunken Tai Chi
    10.228993 Marvel One-Shot: All Hail the King
    10.050875 Iron Man
    9.71492 The Avengers
    9.71492 Iron Man 2
    9.402206 Iron Monkey
    9.165083 The Iron Ministry
    9.05147 Generation Iron
    9.01878 Deadly Advice
    9.01878 Deadly Advice
    9.001775 Avengers: Age of Ultron
    8.898079 An American Romance
    8.84136 The Sessions
    8.72884 Iron Maiden: Flight 666
    8.587389 Revansa
    8.53908 Iron Man
    8.53908 The Mysterious Geographic Explorations of Jasper Morello
    


```python
# Search for the movies that best match the text query “matrix” in the title. Boost the results
#by multiplying the standard score with the IMDB rating score. Repeat the same query
#considering the Rotten Tomatoes score instead of IMDB. Does the order of the results change?
res = es.search(index="project_3", body={
"query": {
    "function_score": {
        "query": {
            "match":{
                "title": "matrix",
            },
        },
        "script_score": {
            "script": {
                "source": "doc.containsKey('imdb_rating') ? doc['imdb_rating'].value : 1"
            }
        }
    }
}})

for doc in res['hits']['hits']:
    print (doc["_score"], doc["_source"]["title"]+',', 'score:', doc["_source"]['imdb_rating'])
    
# "boost_mode": "multiply" is the default so we can avoid to write it
```

    89.26549 The Matrix, score: 8.7
    65.56894 The Matrix Revisited, score: 7.4
    63.796803 The Matrix Reloaded, score: 7.2
    59.36647 The Matrix Revolutions, score: 6.7
    57.594337 Armitage: Dual Matrix, score: 6.5
    50.299583 Return to Source: Philosophy & 'The Matrix', score: 8.0
    


```python
# using Rotten Tomatoes

res = es.search(index="project_3", body={
"query": {
    "function_score": {
        "query": {
            "match":{
                "title": "matrix",
            },
        },
        "script_score": {
            "script": {
                "source": "doc.containsKey('tomatoes_rating') ? doc['tomatoes_rating'].value : 1"
            }
        }
    }
}})

for doc in res['hits']['hits']:
    print (doc["_score"], doc["_source"]["title"]+',', 'score:', doc["_source"]['tomatoes_rating'])
```

    36.937443 The Matrix, score: 3.6
    31.012335 Armitage: Dual Matrix, score: 3.5
    30.12627 The Matrix Revisited, score: 3.4
    30.12627 The Matrix Reloaded, score: 3.4
    30.12627 The Matrix Revolutions, score: 3.4
    27.664772 Return to Source: Philosophy & 'The Matrix', score: 4.4
    

## Additional search request


```python
res = es.search(index="project_3", body={
"query": {
    "function_score": {
        "query": {
            "bool":{
                "must":[{
                    "match":{
                        "fullplot": "mafia"
                    }},{
                    "range": {
                        "imdb_rating": {
                            "gt": 7.0
                        }
                    }},{
                    "range": {
                        "year": {
                            "lt": 2000
                        }
                    }}
                ]
            }
        },
        "script_score": {
            "script": {
                "source": "doc.containsKey('imdb_rating') ? doc['imdb_rating'].value : 1"
            }
        }
    }
},
"size": 5
})

for doc in res['hits']['hits']:
    print (doc["_score"], doc["_source"]["title"])
```

    80.38469 Donnie Brasco
    77.17743 The Boondock Saints
    75.18646 Nayakan
    75.18646 Nayakan
    74.71821 Confessions of a Police Captain
    
