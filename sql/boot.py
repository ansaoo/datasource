#!/home/ansaoo/Documents/08-myProject/pyenv/bin/python3
import hashlib
import sqlite3
import requests
import json
from kafka import KafkaProducer
from elasticsearch import Elasticsearch
import datetime
from dateutil import parser
from urllib.parse import urlencode
from xmljson import badgerfish as bf
#import piexif
import re
import os
import sys
import yaml
import subprocess
import argparse
import xml.etree.ElementTree as ET
import collections

API_URL = "https://maps.googleapis.com/maps/api/geocode/json"

winter_date = parser.parse('{0}-03-25 02:00:00'.format(datetime.datetime.now().year))
summer_date = parser.parse('{0}-10-28 03:00:00'.format(datetime.datetime.now().year))


class Info:
    def __init__(self, filename):
        self.media_info = media_info(filename)


class MediaInfoError(BaseException):
    pass


class ThumbnailError(BaseException):
    pass


def bulk(cmd, target, index, **kwargs):
    files = os.popen(cmd).readlines()
    tot = len(files)
    f = open('{0}.log'.format(datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%S.%f')), 'a')
    f.write('# {0}\n# target: {1}\n# index: {2}\n'.format(cmd, target, index))
    print('0/{0}'.format(tot), end='\r')
    err_count = 0
    for ind, file in enumerate(files):
        sys.stdout.flush()
        print('{0}/{1}'.format(ind+1, tot), end='\r')
        try:
            data = media_info(file.strip())
            second = load_to_es(file.strip(), data=data, index=index, target=target, renamed=False)
            f.write('{0}:\n'.format(hashlib.md5(file.strip().encode('utf-8')).hexdigest()))
            f.write('  filename: {0}\n'.format(file.strip()))
            f.write('  es: {0}\n'.format(second))
        except Exception as e:
            err_count += 1
            f.write('{0}:\n'.format(hashlib.md5(file.strip().encode('utf-8')).hexdigest()))
            f.write('  filename: {0}\n'.format(file.strip()))
            f.write('  error: "{0}"\n'.format(e.__str__()))
    f.write('# nb error: {0}'.format(err_count))
    print('nb error: ', err_count)


def exiv2(filename):
    proc = subprocess.Popen(["exiv2 {0}".format(filename)], stdout=subprocess.PIPE, shell=True)
    (out, err) = proc.communicate()
    if err:
        return None
    out = out.decode('utf-8').split('\n')
    temp = {}
    y = yaml.load('\n'.join([l for l in out[:-4] if l.count(': ') == 1]))
    for key, value in y.items():
        if value:
            if key == 'File size':
                value = float(int(value[:-6]))
            if key == 'File name':
                value = os.path.basename(value)
            if key == 'Image timestamp':
                event_date = datetime.datetime.strptime(value, '%Y:%m:%d %H:%M:%S')
                # event_date = parser.parse(value)
                temp['eventDate'] = event_date.strftime('%Y-%m-%dT%H:%M:%S')
            else:
                temp[to_camel_case(key)] = value
    return temp


def extract_date_filename(filename):
    matchs = re.match(
       '(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})_(?P<hour>\d{2})h(?P<min>\d{2})m(?P<sec>\d{2})_(.*)',
        filename
    )
    if matchs:
        match = matchs.groupdict()
        if all([
            int(match.get('year')) < 2100,
            int(match.get('year')) > 1970,
            int(match.get('month')) < 13,
            int(match.get('day')) < 32,
            int(match.get('hour')) < 24,
            int(match.get('min')) < 60,
            int(match.get('sec')) < 60
        ]):
            return parser.parse('{year}-{month}-{day}T{hour}:{min}:{sec}'.format(**match))
    return None


def get_position(address):
    param = {'address': address}
    res = json.loads(requests.get("{}?{}".format(API_URL, urlencode(param))).text)
    try:
        location = res['results'][0]["geometry"]["location"]
        return {'lat': float(location['lat']), 'lon': float(location['lng'])}
    except Exception:
        return None


def is_valid_date(date_time):
    return all([
        date_time.year < 2100,
        date_time.year > 1980
    ])


def load_table_to_es(cursor, table, index=None):
    cursor.execute('SELECT * FROM {0} limit 1'.format(table))
    name = [d[0] for d in cursor.description]
    data = [{k: v for k, v in zip(name, value) if v} for value in cursor.fetchall()]
    print(data)
    # try:
    #     es.create(
    #         index=table,
    #         doc_type='_doc',
    #         id=hashlib.md5('{0}{1}'.format(table, os.path.basename(filename)).encode('utf-8')).hexdigest(),
    #         body=data
    #     )
    # except Exception:
    #     es.update(
    #         index=table,
    #         doc_type='_doc',
    #         id=hashlib.md5('{0}{1}'.format(table, os.path.basename(filename)).encode('utf-8')).hexdigest(),
    #         body={'doc': data}
    #     )
    # return data


def load_to_es(filename, data, index, target, renamed=True, **kwargs):
    if 'Image' in data.keys():
        attr = 'image'
    elif 'Video' in data.keys():
        attr = 'video'
    elif 'Audio' in data.keys():
        attr = 'audio'
    else:
        attr = 'other'
    temp = {
        'attr': attr,
        '@timestamp': str(datetime.datetime.now()).replace(' ', 'T'),
        'status': True,
        'originalFileName': os.path.basename(filename),
        'mediainfo': data,
        'path': 'image_tmp_path' if 'TEMP' in filename else 'image_path'
    }

    if data['General'][0].get('Recorded_Date'):
        date_tmp = parser.parse(
            data['General'][0].get('Recorded_Date')
        )
        if is_valid_date(date_tmp):
            temp['eventDate'] = date_tmp.strftime('%Y-%m-%dT%H:%M:%S')
    elif data['General'][0].get('Encoded_Date'):
        date_tmp = data['General'][0].get('Encoded_Date')
        if date_tmp.startswith('UTC'):
            date_tmp = parser.parse(date_tmp.replace('UTC', ''))
            if all([
                        (winter_date - date_tmp).total_seconds() < 0,
                        (summer_date - date_tmp).total_seconds() > 0,
            ]):
                hour_delta = 2
            else:
                hour_delta = 1
            if date_tmp.hour + hour_delta < 24:
                date_tmp = date_tmp.replace(hour=date_tmp.hour + hour_delta)
            else:
                date_tmp = date_tmp.replace(
                    hour=date_tmp.hour + hour_delta - 24
                ).replace(
                    day=date_tmp.day + 1
                )
        if is_valid_date(date_tmp):
            temp['eventDate'] = date_tmp.strftime('%Y-%m-%dT%H:%M:%S')
    if temp.get('eventDate') is None and extract_date_filename(filename):
        date_tmp = extract_date_filename(filename)
        if is_valid_date(date_tmp):
            temp['eventDate'] = date_tmp.strftime('%Y-%m-%dT%H:%M:%S')
    if temp.get('eventDate') is None:
        date_tmp = parser.parse(
            data['General'][0].get('File_Modified_Date_Local')
        )
        if is_valid_date(date_tmp):
            temp['eventDate'] = date_tmp.strftime('%Y-%m-%dT%H:%M:%S')

    match = re.match('.*=(?P<tag>.*)\..*', os.path.basename(filename))
    if match and match.groupdict().get('tag'):
        temp['tag'] = [e for e in match.groupdict()['tag'].split('+')]

    if attr == 'image':
        exiv = exiv2(filename)
        if exiv:
            # if exiv.get('eventDate'):
            #     if temp.get('eventDate'):
            #         temp_date = parser.parse(temp['eventDate'])
            #         exiv_date = parser.parse(exiv['eventDate'])
            #         diff = temp_date-exiv_date
            #         if temp_date.year == exiv_date.year:
            #             temp['eventDate'] = exiv['eventDate']
            #         elif abs(diff.total_seconds()) < 86400:
            #             temp['eventDate'] = exiv_date
            #     else:
            #         temp['eventDate'] = exiv['eventDate']
            temp['exiv2'] = exiv

    if temp.get('eventDate') is None:
        temp['eventDate'] = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

    if renamed:
        root, file = os.path.split(filename)
        final_date = parser.parse(temp['eventDate'])
        new_name = '{0}/{1}_{2}'.format(root, final_date.strftime('%Y-%m-%d_%Hh%Mm%S'), file)
        os.rename(filename, new_name)
    else:
        new_name = filename

    temp['fileName'] = os.path.basename(new_name)

    create_thumbnail(new_name, target=target, attr=attr)

    try:
        es.create(
            index=index,
            doc_type='_doc',
            id=hashlib.md5('{0}'.format(os.path.basename(new_name)).encode('utf-8')).hexdigest(),
            body=temp
        )
    except Exception:
        es.update(
            index=index,
            doc_type='_doc',
            id=hashlib.md5('{0}'.format(os.path.basename(new_name)).encode('utf-8')).hexdigest(),
            body={'doc': temp}
        )
    return temp


def media_info(filename, ns='{https://mediaarea.net/mediainfo}'):
    proc = subprocess.Popen(["mediainfo --Output=XML {0}".format(filename)], stdout=subprocess.PIPE, shell=True)
    (out, err) = proc.communicate()
    if err:
        raise MediaInfoError('mediainfo error on {0}'.format(filename))
    data = bf.data(ET.fromstring(out.decode('utf-8')))
    result = collections.OrderedDict()
    for e in my_get(data, 'MediaInfo.media.track'):
        temp = collections.OrderedDict()
        for k, v in e.items():
            match = re.match('\{https://mediaarea\.net/mediainfo\}(?P<name>.+)', k)
            if match:
                temp[match.groupdict()['name']] = v.get('$')
        if result.get(e['@type']):
            result[e['@type']].append(temp)
        else:
            result[e['@type']] = [temp]
    return result


def my_get(data, key, ns='{https://mediaarea.net/mediainfo}'):
    for e in key.split('.'):
        data = data['{0}{1}'.format(ns, e)]
    return data


def oneshot(file, target, index, **kwargs):
    try:
        data = media_info(file)
        load_to_es(file, data, index=index, target=target)
        print('success')
    except Exception as e:
        raise Exception(e.__str__())


def create_thumbnail(filename, target='/home/ansaoo', attr='image'):
    img_script = "convert {0} -auto-orient -resize 600 {1}/{2}/{3}_thumb.jpg"
    video_script = "ffmpeg -i {0} -v 0 -f -ss 00:00:00.435 -vframes 1 {1}/{2}/{3}_thumb.jpg"
    if attr == 'image':
        cmd = img_script
    elif attr == 'video':
        cmd = video_script
    else:
        return None
    base = os.path.basename(filename)
    if not os.path.exists('{0}/{1}'.format(target, base[:7])):
        os.mkdir('{0}/{1}'.format(target, base[:7]))
    proc = subprocess.Popen(
        [cmd.format(filename, target, base[:7], base)],
        stdout=subprocess.PIPE,
        shell=True)
    (out, err) = proc.communicate()
    if err:
        raise ThumbnailError(err)
    return None


def sql(db, table=None, **kwargs):
    database = sqlite3.connect(db)
    cursor = database.cursor()
    if table:
        load_table_to_es(cursor, table)
    else:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [e[0] for e in cursor.fetchall()]
        [load_table_to_es(cursor, t) for t in tables if t != 'sqlite_sequence']


def to_camel_case(word):
    tmp = ''
    for index, token in enumerate(word.lower().split(' ')):
        if index == 0:
            tmp += token
        else:
            tmp += token.capitalize()
    return tmp


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--mode',
                            # nargs=1,
                            default='oneshot',
                            type=str,
                            required=True,
                            choices=['oneshot', 'bulk', 'sql'],
                            help='mode [oneshot|bulk|sql]. Default=oneshot')
    arg_parser.add_argument('--index',
                            # nargs=1,
                            default='image',
                            type=str,
                            help='label index to use. Default=image')
    arg_parser.add_argument('--file',
                            # nargs=1,
                            default=None,
                            type=str,
                            help='file to load. Only use with mode=oneshot. Default=None')
    arg_parser.add_argument('--target',
                            # nargs=1,
                            default='~/Images/thumbnail',
                            type=str,
                            help='loading img process create thumbnails.'
                                 ' This define repository to store thumbnails.'
                                 ' default=\'~/Images/thumbnail\'')
    arg_parser.add_argument('--cmd',
                            # nargs=1,
                            default='find ~/Images/20* -iname "*.jpg"',
                            type=str,
                            help='command line to get multiple file to load. Only use with mode=bulk.'
                                 ' Default=\'find ~/Images/20* -iname "*.jpg"\'')
    arg_parser.add_argument('--db',
                            # nargs=1,
                            default=None,
                            type=str,
                            help='sqlite database to load (all table exist)')
    arg_parser.add_argument('--table',
                            # nargs=1,
                            default=None,
                            type=str,
                            help='sqlite specific table to load. Needs define db.')
    args = arg_parser.parse_args()
    es = Elasticsearch()

    fct = {
        'bulk': bulk,
        'oneshot': oneshot,
        'sql': sql
    }

    fct[args.mode](**vars(args))
