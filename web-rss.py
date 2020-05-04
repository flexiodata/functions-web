
# ---
# name: web-rss
# deployed: true
# title: RSS Reader
# description: Returns the articles from the RSS feed given by the URLs
# params:
# - name: url
#   type: array
#   description: Urls for which to get the info
#   required: true
# returns:
# - name: channel_title
#   type: string
#   description: The feed channel title
# - name: channel_link
#   type: string
#   description: The feed channel link
# - name: item_title
#   type: string
#   description: The article title
# - name: item_author
#   type: string
#   description: The article author
# - name: item_link
#   type: string
#   description: The article link
# - name: item_published
#   type: string
#   description: The date/time the article was published
# - name: item_description
#   type: string
#   description: A description for the article
# examples:
#   - '"http://feeds.arstechnica.com/arstechnica/technology-lab"'
#   - '"http://feeds.arstechnica.com/arstechnica/technology-lab,https://www.technologyreview.com/feed/"'
#   - '"https://www.technologyreview.com/feed/","channel_title,item_title,item_author,item_link"'
# notes:
# ---

import json
import time
import urllib
import tempfile
import aiohttp
import asyncio
import itertools
import feedparser
from cerberus import Validator
from collections import OrderedDict

def flexio_handler(flex):

    # get the input
    input = flex.input.read()
    input = json.loads(input)
    if not isinstance(input, list):
        raise ValueError

    # define the expected parameters and map the values to the parameter names
    # based on the positions of the keys/values
    params = OrderedDict()
    params['urls'] = {'required': True, 'validator': validator_list, 'coerce': to_list}
    params['properties'] = {'required': False, 'validator': validator_list, 'coerce': to_list, 'default': '*'}
    params['config'] = {'required': False, 'type': 'string', 'default': ''} # index-styled config string
    input = dict(zip(params.keys(), input))

    # validate the mapped input against the validator
    v = Validator(params, allow_unknown = True)
    input = v.validated(input)
    if input is None:
        raise ValueError

    # map this function's property names to the API's property names
    property_map = OrderedDict()
    property_map['channel_title'] = 'channel_title'
    property_map['channel_link'] = 'channel_link'
    property_map['item_title'] = 'item_title'
    property_map['item_author'] = 'item_author'
    property_map['item_link'] = 'item_link'
    property_map['item_published'] = 'item_published'
    property_map['item_description'] = 'item_description'

    # get the properties to return and the property map;
    # if we have a wildcard, get all the properties
    properties = [p.lower().strip() for p in input['properties']]
    if len(properties) == 1 and (properties[0] == '' or properties[0] == '*'):
        properties = list(property_map.keys())

    # get any configuration settings
    config = urllib.parse.parse_qs(input['config'])
    config = {k: v[0] for k, v in config.items()}
    limit = int(config.get('limit', 10000))
    headers = config.get('headers', 'true').lower()
    if headers == 'true':
        headers = True
    else:
        headers = False

    # get the feeds
    urls = input['urls']
    loop = asyncio.get_event_loop()
    temp_fp_all = loop.run_until_complete(fetch_all(urls))

    # write the output
    flex.output.content_type = 'application/json'
    flex.output.write('[')

    if headers is True:
        flex.output.write(json.dumps(properties))

    idx = 0
    for temp_fp in temp_fp_all:
        while True:
            row = temp_fp.readline()
            if not row:
                break
            if idx >= limit:
                break
            row = json.loads(row)
            content = ''
            if headers is True or idx > 0:
                content = ','
            content = content + json.dumps([(row.get(p) or '') for p in properties])
            flex.output.write(content)
            idx = idx + 1

    flex.output.write(']')

async def fetch_all(urls):
    tasks = []
    async with aiohttp.ClientSession(raise_for_status=True) as session:
        for url in urls:
            tasks.append(fetch(session, url))
        temp_fp_all = await asyncio.gather(*tasks)
        return temp_fp_all

async def fetch(session, url):
    # get the data, process it and put the results in a temporary
    # file for aggregating with other results
    temp_fp = tempfile.TemporaryFile(mode='w+t')
    try:
        async with session.get(url) as response:
            content = await response.text()
            for item in getFeedItem(content):
                data = json.dumps(item) + "\n" # application/x-ndjson
                temp_fp.write(data)
    except Exception:
        pass
    temp_fp.seek(0)
    return temp_fp

def getFeedItem(content):
    # see: https://pythonhosted.org/feedparser/
    parser = feedparser.parse(content)
    channel = parser.get('channel',{})
    items = parser.get('entries',[])
    for i in items:
        yield {
            'id': i.get('id'),
            'channel_title': channel.get('title'),
            'channel_link': channel.get('link'),
            'item_title': i.get('title'),
            'item_author': i.get('author'),
            'item_link': i.get('link'),
            'item_published': string_from_time(i.get('published_parsed')),
            'item_description': i.get('description')
        }

def validator_list(field, value, error):
    if isinstance(value, str):
        return
    if isinstance(value, list):
        for item in value:
            if not isinstance(item, str):
                error(field, 'Must be a list with only string values')
        return
    error(field, 'Must be a string or a list of strings')

def to_list(value):
    # if we have a list of strings, create a list from them; if we have
    # a list of lists, flatten it into a single list of strings
    if isinstance(value, str):
        return value.split(",")
    if isinstance(value, list):
        return list(itertools.chain.from_iterable(value))
    return None

def string_from_time(value):
    try:
        return time.strftime('%Y-%m-%d %H:%M:%S', value)
    except:
        return ''