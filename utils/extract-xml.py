#!/usr/bin/env python3

import argparse
import json
import os
import re
import subprocess
import xmltodict
import xml.etree.ElementTree as ET
from functools import reduce


class MovieInfo:
    def __init__(self, filename):
        self.name = os.path.splitext(filename)[0]
        self.chapters = "{0}.chapters.txt"
        self.chapters_file = None
        self.cmd = None
        self.filename = filename
        self.mediainfo = media_info(filename)
        if self.get_title():
            matchs = re.match(
                "(?P<movie_name>.+)\.(?P<year_of_release>\d{4})(\.(?P<original_source_medium>[^-]+))?(-(?P<encoder>.+))?$",
                str(self.get_title())
            )
        elif self.get_movie():
            matchs = re.match(
                "(?P<movie_name>.+)\.(?P<year_of_release>\d{4})(\.(?P<original_source_medium>[^-]+))?(-(?P<encoder>.+))?$",
                str(self.get_movie())
            )
        elif self.get_movie_name():
            matchs = re.match(
                "(?P<movie_name>.+)\.(?P<year_of_release>\d{4})(\.(?P<original_source_medium>[^-]+))?(-(?P<encoder>.+))?$",
                str(self.get_movie_name())
            )
        else:
            matchs = re.match(
                "(?P<movie_name>.+)\.(?P<year_of_release>\d{4})(\.(?P<original_source_medium>[^-]+))?(-(?P<encoder>.+))?\.\w{3}$",
                str(os.path.basename(filename))
            )
        self.parsed = matchs.groupdict() if matchs else None
        self.output = None
        if self.parsed:
            self.output = "{0}.{1}.mkv".format(self.parsed.get("movie_name"), self.parsed.get("year_of_release"))
        else:
            self.output = "{0}.mkv".format(self.name)
        self.tags = None

    def check_chapter(self):
        if os.path.exists(self.chapters.format(self.name)):
            self.chapters_file = self.chapters.format(self.name)
        elif self.parsed and os.path.exists(self.chapters.format(
                "{0}.{1}".format(self.parsed.get("movie_name"), self.parsed.get("year_of_release"))
        )):
            self.chapters_file = self.chapters.format(
                "{0}.{1}".format(self.parsed.get("movie_name"), self.parsed.get("year_of_release"))
            )
        return self.chapters_file

    def get_cmd(self):
        if self.mediainfo:
            global lang
            track_list = get_object(self.mediainfo, 'track')[1:]
            for track in track_list:
                if track['@type'] == 'Video':
                    self.cmd.append("--compression {0}:none".format(track['ID']))
                    self.cmd.append("--track-name {0}:".format(track['ID']))
                elif track['@type'] == 'Audio':
                    self.cmd.append("--compression {0}:none".format(track['ID']))
                    self.cmd.append("--track-name {0}:".format(track['ID']))
                elif track['@type'] == 'Text':
                    self.cmd.append("--compression {0}:none".format(track['ID']))
                    if 'force' in str(track['Title']).lower():
                        self.cmd.append("--track-name {0}:'Forced'".format(track['ID']))
                        self.cmd.append("--forced-track {0}:yes".format(track['ID']))
                    else:
                        self.cmd.append("--track-name {0}:".format(track['ID']))
        return None

    def get_height(self):
        return get_object(self.mediainfo, selector='track.1.Height')

    def get_movie(self):
        return get_object(self.mediainfo, selector='track.0.Movie')

    def get_movie_name(self):
        return get_object(self.mediainfo, selector='track.0.Movie_name')

    def get_title(self):
        return get_object(self.mediainfo, selector='track.0.Title')

    def merge(self):
        global args
        self.cmd = [
            "mkvmerge",
            "--output {0}/{1}".format(args.target, self.output),
            "--title \"{0}\"".format(self.parsed["movie_name"].replace('.', ' '))
        ]
        self.get_cmd()
        if self.tags:
            self.cmd.append("--global-tags {0}".format(self.tags))
        if self.check_chapter():
            self.cmd.append("--chapters {0}".format(self.chapters_file))

        self.cmd.append(self.filename)
        proc = subprocess.run(
            [" ".join(self.cmd)],
            shell=True)
        if os.path.exists("{0}/{1}".format(args.target, self.output)):
            new_file = media_info("{0}/{1}".format(args.target, self.output))
            print(json.dumps(
                get_object(new_file, selector='track.0'),
                indent=2
            ))
        if proc.returncode > 0:
            print("\x1b[0;30;41m Error \x1b[0m")
            print(proc.returncode)
            print(proc.stderr)
            raise MkvPropEditError('mkvmerge file error on {0}'.format(self.filename))

    def set_chapter(self):
        if self.check_chapter():
            set_chapter(self.filename, self.chapters_file)
        return None

    def set_tag(self):
        if self.tags and os.path.exists(self.tags):
            set_tag(self.filename, self.tags)

    def set_title(self):
        if self.parsed:
            new = self.parsed.get("movie_name").replace('.', ' ')
            set_title(
                self.filename,
                title=new
            )
            self.update_mediainfo()
            if self.get_movie() == new:
                print("update title ... \33[32m Ok \33[0m")
            else:
                print("update title ... \x1b[0;30;41m Error \x1b[0m")
            print("--> new title: \"{0}\"".format(new))

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

    def update_mediainfo(self):
        self.mediainfo = media_info(self.filename)


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
        for el in trans.keys():
            if el in str(val).lower():
                value = ET.Element('String')
                value.text = str(trans[el])
                simple.append(name)
                simple.append(value)
                return simple
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
    if proc.returncode > 0:
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
    if proc.returncode > 0:
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
    if proc.returncode > 0:
        print("update title ... \x1b[0;30;41m Error \x1b[0m")
        print(err)
        raise MkvPropEditError('mkvpropedit set title error on {0}'.format(filename))
    # print("update title ... \33[32m Ok \33[0m")


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('mode',
                            # nargs=1,
                            default='update',
                            type=str,
                            # required=True,
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
    lang = {
        'en': 'eng',
        'fr': 'fre'
    }
    trans = {
        'Blu-ray': ['720p', '1080p', 'bluray'],
        'WebDL': ['webdl', 'web-dl'],
        'DVD-Video': ['dvd']
    }
    trans = {e: k for k, v in trans.items() for e in v}
    try:
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
            movie.merge()
        print("\x1b[6;30;42m Success! \x1b[0m")
    except Exception as e:
        print("\x1b[0;30;41m Failed \x1b[0m")
        print(e.__str__())

