#!/home/samuel/venv3.5/bin/python3
import hashlib
import sqlite3
import requests
import json
from kafka import KafkaProducer
from elasticsearch import Elasticsearch
import datetime
from urllib.parse import urlencode
import piexif
import os
import sys
import yaml
import subprocess

API_URL = "https://maps.googleapis.com/maps/api/geocode/json"


def get_position(address):
    param = {'address': address}
    res = json.loads(requests.get("{}?{}".format(API_URL, urlencode(param))).text)
    try:
        location = res['results'][0]["geometry"]["location"]
        return {'lat': float(location['lat']), 'lon': float(location['lng'])}
    except Exception:
        return None


def load_jpg_to_es(filename):
    proc = subprocess.Popen(["exiv2 {0}".format(filename)], stdout=subprocess.PIPE, shell=True)
    (out, err) = proc.communicate()
    out = out.decode('utf-8').split('\n')
    temp = {
        'attr': 'jpeg',
        '@timestamp': str(datetime.datetime.now()).replace(' ', 'T'),
        'status': True
    }
    y = yaml.load('\n'.join([l for l in out[:-4] if l.count(': ') == 1]))
    for key, value in y.items():
        if value:
            if key == 'File size':
                value = float(int(value[:-6]))
            if key == 'File name':
                value = os.path.basename(value)
            if key == 'Image timestamp':
                event_date = datetime.datetime.strptime(value, '%Y:%m:%d %H:%M:%S')
                # temp['year'] = event_date.year
                # temp['month'] = event_date.month
                temp['eventDate'] = event_date.strftime('%Y-%m-%dT%H:%M:%S')
            else:
                temp[to_camel_case(key)] = value
    print(temp)
    es.create(
        index='image',
        doc_type='_doc',
        id=hashlib.md5('{0}{1}'.format('jpeg', os.path.basename(filename)).encode('utf-8')).hexdigest(),
        body=temp
    )


def resize(filename, target='/home/ansaoo'):
    base = os.path.basename(filename)
    if not os.path.exists('{0}/{1}'.format(target, base[:7])):
        os.mkdir('{0}/{1}'.format(target, base[:7]))
    proc = subprocess.Popen(
        ["convert -resize 600 {0} {1}/{2}/{3}_thumb.jpg".format(filename, target, base[:7], base)],
        stdout=subprocess.PIPE,
        shell=True)
    (out, err) = proc.communicate()
    print('{0}: {1}'.format(filename, err))


def to_camel_case(word):
    tmp = ''
    for index, token in enumerate(word.lower().split(' ')):
        if index == 0:
            tmp += token
        else:
            tmp += token.capitalize()
    return tmp


if __name__ == "__main__":
    es = Elasticsearch()
    files = os.popen('find /home/ansaoo/Images/20* -iname "2018-*.jpg"').readlines()
    tot = len(files)
    for index, file in enumerate(files):
        print('{0} / {1}'.format(index, tot))
        resize(file.strip(), target='/home/ansaoo/Images/thumbnail')
        load_jpg_to_es(file.strip())
    # resize(sys.argv[1], target='/home/ansaoo/Images/thumbnail')
    # load_jpg_to_es(sys.argv[1])
