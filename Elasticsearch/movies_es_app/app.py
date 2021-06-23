from flask import Flask, render_template, request
from elasticsearch import Elasticsearch

app = Flask(__name__)
es = Elasticsearch('127.0.0.1', port=9200)

@app.route('/')
def home():
    return render_template('search.html')

@app.route('/search/results', methods=['GET', 'POST'])
def search_request():
    search_term = request.form["input"]
    res = es.search(
        index="project_3", 
        size=20, 
        body={
            "query": {
                "function_score": {
                    "query": {
                        "multi_match" : {
                            "query": search_term, 
                            "fields": [
                                "fullplot"
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
            "highlight": {
                "fields": {
                    "fullplot": {}
                },
                "pre_tags" : ["<b>"],
                "post_tags" : ["</b>"],
            }
        }
    )
    return render_template('results.html', res=res )

if __name__ == '__main__':
    app.secret_key = 'mysecret'
    app.run(host='0.0.0.0', port=5000)