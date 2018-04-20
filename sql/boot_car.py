import hashlib
import sqlite3
import requests
import json
from kafka import KafkaProducer
from elasticsearch import Elasticsearch
import datetime
from urllib.parse import urlencode

API_URL = "https://maps.googleapis.com/maps/api/geocode/json"


def get_position(address):
    param = {'address': address}
    res = json.loads(requests.get("{}?{}".format(API_URL, urlencode(param))).text)
    try:
        location = res['results'][0]["geometry"]["location"]
        return {'lat': float(location['lat']), 'lon': float(location['lng'])}
    except Exception:
        return None


def parser(es, table, value, index=None, doc_type=None):
    mapper = {
        'carburant': fuel_parser,
        'entretien': maintain_parser
    }
    mapper[table](es, value, index=index, doc_type=doc_type)


def fuel_parser(es, value, index=None, doc_type=None):
    tmp = {
        'attr': doc_type,
        '@timestamp': str(datetime.datetime.now()).replace(' ', 'T')
    }
    if value[1]:
        tmp['compteur'] = value[1]
    if value[2]:
        tmp['eventDate'] = value[2]
    if value[3]:
        tmp['kilometre'] = value[3]
    if value[4]:
        tmp['lieu'] = value[4]
        geo_point = get_position(value[4])
        if geo_point:
            tmp['location'] = geo_point
        else:
            if value[4] == 'Oyonnax':
                tmp['location'] = {'lon': 5.655335, 'lat': 46.257773}
            if value[4] == 'Amberieu':
                tmp['location'] = {'lon': 5.359556, 'lat': 45.95843600000001}
            if value[4] == 'Nantua':
                tmp['location'] = {'lon': 5.607762, 'lat': 46.153405}
    if value[5]:
        tmp['litre'] = value[5]
    if value[6]:
        tmp['prix'] = value[6]
    if value[7]:
        tmp['station'] = value[7]
    if value[8]:
        tmp['type'] = value[8]
    if value[9]:
        tmp['voiture'] = 'celica' if value[9] == 1 else 'laguna'
    print(tmp)
    es.create(
        index=index,
        doc_type='_doc',
        id=hashlib.md5('{0}{1}'.format(doc_type, value[0]).encode('utf-8')).hexdigest(),
        body=tmp
    )


def maintain_parser(es, value, index=None, doc_type=None):
    tmp = {
        'attr': doc_type,
        '@timestamp': str(datetime.datetime.now()).replace(' ', 'T')
    }
    if value[1]:
        tmp['compteur'] = value[1]
    if value[2]:
        tmp['eventDate'] = value[2]
    if value[3]:
        tmp['garage'] = value[3]
        if value[3] == 'Norauto':
            tmp['location'] = {'lon': 4.867663, 'lat': 45.806236}
        if value[3] == 'Point S':
            tmp['location'] = {'lon': 5.650765, 'lat': 46.263112}
        if value[3] == 'Feuvert':
            tmp['location'] = {'lon': 5.661825, 'lat': 46.281100}
        if value[3] == 'Dominguez':
            tmp['location'] = {'lon': 5.641535, 'lat': 46.260083}
    if value[4]:
        tmp['libelle'] = value[4]
    if value[5]:
        tmp['lieu'] = value[5]
    if value[6]:
        tmp['prix'] = value[6]
    if value[7]:
        tmp['voiture'] = 'celica' if value[7] == 1 else 'laguna'
    print(tmp)
    es.create(
        index=index,
        doc_type='_doc',
        id=hashlib.md5('{0}{1}'.format(doc_type, value[0]).encode('utf-8')).hexdigest(),
        body=tmp
    )


def load_to_es(table=None, doc=None):
    es = Elasticsearch()
    db = sqlite3.connect('../resources/oursCars.db')
    cursor = db.cursor()
    cursor.execute("SELECT * FROM {0}".format(table))
    for val in cursor.fetchall():
        parser(es, table, val, index='car', doc_type=doc)
    print(str(datetime.datetime.now()).replace(' ', 'T')+'Z+0200')
    return 0


if __name__ == "__main__":
    load_to_es(table='carburant', doc='fuel')
    load_to_es(table='entretien', doc='maintains')
