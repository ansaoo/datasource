#!/home/samuel/venv3.5/bin/python3
import hashlib
import sqlite3
import requests
import json
from kafka import KafkaProducer
from elasticsearch import Elasticsearch
import datetime
from urllib.parse import urlencode
#import piexif
import os
import sys
import yaml
import subprocess
import argparse

API_URL = "https://maps.googleapis.com/maps/api/geocode/json"


def bulk(cmd, target, **kwargs):
    files = os.popen(cmd).readlines()
    tot = len(files)
    f = open('{0}.log'.format(datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%S')), 'a')
    print('0/{0}'.format(tot))
    err_count = 0
    for index, file in enumerate(files):
        sys.stdout.flush()
        print('{0}/{1}'.format(index+1, tot), end='\r')
        try:
            first = resize(file.strip(), target=target)
            second = load_jpg_to_es(file.strip())
            f.write('{0}:\n'.format(hashlib.md5(file.strip().encode('utf-8')).hexdigest()))
            f.write('  filename: {0}\n'.format(file.strip()))
            f.write('  resize: {0}\n'.format(first))
            f.write('  es: {0}\n'.format(second))
        except Exception as e:
            err_count += 1
            f.write('{0}:\n'.format(hashlib.md5(file.strip().encode('utf-8')).hexdigest()))
            f.write('  filename: {0}\n'.format(file.strip()))
            f.write('  error: "{0}"\n'.format(e.__str__()))
    print('nb error: ', err_count)


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
    # print(temp)
    es.create(
        index='image',
        doc_type='_doc',
        id=hashlib.md5('{0}{1}'.format('jpeg', os.path.basename(filename)).encode('utf-8')).hexdigest(),
        body=temp
    )
    return temp


def oneshot(file, target, **kwargs):
    try:
        resize(file, target=target)
        load_jpg_to_es(file)
        print('success')
    except Exception:
        raise Exception('Error: already exist')


def resize(filename, target='/home/ansaoo'):
    base = os.path.basename(filename)
    if not os.path.exists('{0}/{1}'.format(target, base[:7])):
        os.mkdir('{0}/{1}'.format(target, base[:7]))
    proc = subprocess.Popen(
        ["convert -resize 600 {0} {1}/{2}/{3}_thumb.jpg".format(filename, target, base[:7], base)],
        stdout=subprocess.PIPE,
        shell=True)
    (out, err) = proc.communicate()
    return err


def to_camel_case(word):
    tmp = ''
    for index, token in enumerate(word.lower().split(' ')):
        if index == 0:
            tmp += token
        else:
            tmp += token.capitalize()
    return tmp


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode',
                        # nargs=1,
                        default='oneshot',
                        type=str,
                        help='mode [oneshot|bulk]. Default=oneshot')
    parser.add_argument('--type',
                        # nargs=1,
                        default='image',
                        type=str,
                        help='mode [image|audio|video]. Default=image')
    parser.add_argument('--file',
                        # nargs=1,
                        default=None,
                        type=str,
                        help='default=None')
    parser.add_argument('--target',
                        # nargs=1,
                        default='/home/ansaoo/Images/thumbnail',
                        type=str,
                        help='default=/home/ansaoo/Images/thumbnail')
    parser.add_argument('--cmd',
                        # nargs=1,
                        default='find /home/ansaoo/Images/20* -iname "*.jpg"',
                        type=str,
                        help='default=\'find /home/ansaoo/Images/20* -iname "*.jpg"\'')
    args = parser.parse_args()
    es = Elasticsearch()

    fct = {
        'bulk': bulk,
        'oneshot': oneshot
    }

    fct[args.mode](**vars(args))
