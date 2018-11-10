#!/usr/bin/env python3

import argparse
import os
import re
import subprocess
import sys
import xmltodict
import xml.etree.ElementTree as ET
from functools import reduce


class MovieInfo:
    def __init__(self, filename):
        self.name = os.path.splitext(filename)[0]
        self.chapters = "{0}.chapters.txt"
        self.filename = filename
        matchs = re.match(
            "(?P<movie_name>.+)\.(?P<year_of_release>\d{4})(\.(?P<original_source_medium>\w+))?(-(?P<encoder>.+))?\.\w{3}$",
            os.path.basename(filename)
        )
        self.parsed = matchs.groupdict() if matchs else None
        self.output = None
        if self.parsed:
            self.output = "{0}.{1}.mkv".format(self.parsed.get("movie_name"), self.parsed.get("year_of_release"))
        else:
            self.output = "{0}.mkv".format(self.name)
        self.mediainfo = media_info(filename)
        self.tags = None

    def get_info(self):
        return get_object(self.mediainfo, selector='track.1.Height')

    def merge(self):
        global args
        proc = subprocess.run(
            ["mkvmerge --output {0}/{1} --title \"{2}\" --global-tags {3} {4}".format(
                args.target,
                self.output,
                self.parsed["movie_name"].replace('.', ' '),
                self.tags,
                self.filename
            )],
            shell=True)
        if proc.returncode > 0:
            print("\x1b[0;30;41m Error \x1b[0m")
            raise MkvPropEditError('mkvmerge file error on {0}'.format(self.filename))

    def set_chapter(self):
        if os.path.exists(self.chapters.format(self.name)):
            chapter_file = self.chapters.format(self.name)
        elif self.parsed and os.path.exists(self.chapters.format(
                "{0}.{1}".format(self.parsed.get("movie_name"), self.parsed.get("year_of_release"))
        )):
            chapter_file = self.chapters.format(
                "{0}.{1}".format(self.parsed.get("movie_name"), self.parsed.get("year_of_release"))
            )
        else:
            return None
        set_chapter(self.filename, chapter_file)

    def set_tag(self):
        if self.tags and os.path.exists(self.tags):
            set_tag(self.filename, self.tags)

    def set_title(self):
        if self.parsed:
            set_title(
                self.filename,
                title=self.parsed.get("movie_name").replace('.', ' ')
            )

    def to_xml(self):
        header = '<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE Tags SYSTEM "matroskatags.dtd">'
        tags = ET.Element('Tags')
        tag = ET.SubElement(tags, 'Tag')
        if self.parsed:
            [tag.append(create_simple(k, v)) for k, v in self.parsed.items() if create_simple(k, v)]
        self.tags = '{0}.xml'.format(self.name)
        with open(self.tags, mode='w') as f:
            f.write(header)
            f.write(ET.tostring(tags, encoding='utf-8', method='xml').decode('utf-8'))


class MediaInfoError(BaseException):
    pass


class MkvPropEditError(BaseException):
    pass


def media_info(filename):
    proc = subprocess.Popen(
        ["mediainfo --Output=XML {0}".format(filename)],
        stdout=subprocess.PIPE,
        shell=True)
    (out, err) = proc.communicate()
    if err:
        raise MediaInfoError('mediainfo error on {0}'.format(filename))
    return get_object(xmltodict.parse(out), selector='MediaInfo.media')


def int_or_string(value):
    try:
        return int(value)
    except ValueError:
        return value


def get_object(doc, selector=None, **kwargs):
    if selector is None:
        return doc
    selects = selector.split(".")
    selects.insert(0, doc)
    try:
        return reduce((lambda x, y: x[int_or_string(y)]), selects)
    except Exception:
        return None


def create_simple(key, val):
    if str(key).lower() == 'movie_name' or val is None:
        return None
    simple = ET.Element('Simple')
    name = ET.Element('Name')
    name.text = str(key).replace('_', ' ').capitalize()
    if str(key).lower() == 'original_source_medium':
        global trans
        if trans.get(str(val).lower()):
            value = ET.Element('String')
            value.text = str(trans[str(val).lower()])
            simple.append(name)
            simple.append(value)
        else:
            return None
    elif str(key).lower() == 'movie_name':
        value = ET.Element('String')
        value.text = str(val).replace('.', ' ')
        simple.append(name)
        simple.append(value)
    else:
        value = ET.Element('String')
        value.text = str(val)
        simple.append(name)
        simple.append(value)
    return simple


def set_chapter(filename, chapter_file):
    print("update chapter ... ", end="\r")
    proc = subprocess.Popen(
        ["mkvpropedit {0} --chapters {1}".format(filename, chapter_file)],
        stdout=subprocess.PIPE,
        shell=True)
    (out, err) = proc.communicate()
    if err:
        print("update chapter ... \x1b[0;30;41m Error \x1b[0m")
        print(err)
        raise MkvPropEditError('mkvpropedit set chapters error on {0}'.format(filename))
    print("update chapter ... \33[32m Ok \33[0m")


def set_tag(filename, tag_xml_file):
    print("update tag ... ", end="\r")
    proc = subprocess.Popen(
        ["mkvpropedit {0} --tags global:{1}".format(filename, tag_xml_file)],
        stdout=subprocess.PIPE,
        shell=True)
    (out, err) = proc.communicate()
    if err:
        print("update tag ... \x1b[0;30;41m Error \x1b[0m")
        print(err)
        raise MkvPropEditError('mkvpropedit set tags error on {0}'.format(filename))
    print("update tag ... \33[32m Ok \33[0m")


def set_title(filename, title, **kwargs):
    print("update title ... ", end="\r")
    proc = subprocess.Popen(
        ["mkvpropedit {0} --edit info --set \"title={1}\"".format(filename, title)],
        stdout=subprocess.PIPE,
        shell=True)
    (out, err) = proc.communicate()
    if err:
        print("update title ... \x1b[0;30;41m Error \x1b[0m")
        print(err)
        raise MkvPropEditError('mkvpropedit set title error on {0}'.format(filename))
    print("update title ... \33[32m Ok \33[0m")


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--mode',
                            # nargs=1,
                            default='update',
                            type=str,
                            required=True,
                            choices=['merge', 'update'],
                            help='mode [merge|update]. Default=merge')
    arg_parser.add_argument('--file',
                            # nargs=1,
                            type=str,
                            required=True,
                            help='file to load.')
    arg_parser.add_argument('--target',
                            # nargs=1,
                            default='/tmp',
                            type=str,
                            help='repository to store new generated file in mode "merge".'
                                 ' default=/tmp')
    args = arg_parser.parse_args()
    trans = {
        '720p': 'Blu-ray',
        '1080p': 'Blu-ray',
        'bluray': 'Blu-ray',
        'webdl': 'WebDL',
        'dvd': 'DVD-Video'
    }
    if args.mode == 'update':
        print("Work: {0}".format(args.file))
        movie = MovieInfo(args.file)
        movie.to_xml()
        movie.set_title()
        movie.set_chapter()
        movie.set_tag()
    if args.mode == 'merge':
        print("Work: {0}".format(args.file))
        movie = MovieInfo(args.file)
        movie.to_xml()
        movie.set_chapter()
        movie.merge()
    print("\x1b[6;30;42m Success! \x1b[0m")

