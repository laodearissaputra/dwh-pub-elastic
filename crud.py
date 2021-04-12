from datetime import datetime
from flask import Flask, jsonify, request, Response
from elasticsearch import Elasticsearch
import time
import random

app = Flask(__name__)

es = Elasticsearch("http://34.101.246.247:9200")

@app.route('/index', methods=['POST'])
def create():

    index_name = "test-bithealth"

    request_body = {
        "settings" :{
        "number_of_shards": 1, 
        "number_of_replicas": 0}
    }

    res = es.indices.exists(index=index_name)

    if res:
        data = {
            'index': 'index exists'
        }
    else:
        res = es.indices.create(index=index_name, body=request_body)
        print(res)
        data = {
            'index': 'index create'
        }

    return jsonify(data)

@app.route('/create', methods=['POST'])
def insert():

    name = str(request.form['name'])
    no_hp = int(request.form['no_hp'])
    address = str(request.form['address'])

    doc = {
        'nama': name,
        'no handphone': no_hp,
        'alamat': address,
        'timestamp': datetime.now(),
    }

    # index_name = 'guest-book'
    res = es.index(index = 'test-bithealth', doc_type='test', body = doc)
    data = {
        'result' : res['result']
    }
    print('Data insert!')

    return jsonify(data)

@app.route('/read', methods=['GET'])
def read():

    name = str(request.form['name'])

    q = {"query": {"match": {"nama.keyword": name}}}
    search_data = es.search(index='guest-book', doc_type='guest', body=q)

    for resp in search_data['hits']['hits']:
        res_title = {
            "nama" : resp['_source'].get('nama'),
            "no handphone" : resp['_source'].get('no handphone'),
            "address" : resp['_source'].get('alamat')
        }

    print('Data find!')

    data = {
        'name' : res_title['nama']
    }

    return jsonify(data)

@app.route('/update', methods=['POST'])
def update():

    guest_id = str(request.form['_id'])
    name = str(request.form['name'])
    no_hp = int(request.form['no_hp'])
    address = str(request.form['address'])

    doc = {
        'nama': name,
        'no handphone': no_hp,
        'alamat': address,
    }

    res = es.update(index='guest-book', doc_type="guest", id=guest_id, body={"doc": doc})
    print('Data update!')
    data = {
        '_id' : res['_id']
    }

    return jsonify(data)

@app.route('/delete', methods=['POST'])
def delete():

    name = str(request.form['name']).lower()
    data = {"query": {"match": {"nama.keyword": name}}}
    res = es.delete_by_query(index='guest-book', body=data)
    data = {
        'delete' : res['deleted']
    }
    print('Document Delete')

    return jsonify(data)


if __name__ == '__main__':
    app.run(debug=True)