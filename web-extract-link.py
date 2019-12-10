
# ---
# name: web-extract-link-multiple
# deployed: true
# title: Website Link Extraction
# description: Returns the domain and/or url for all links on one-or-more webpages matching a search string.
# params:
# - name: url
#   type: array
#   description: Urls of webpage to search; parameter can be an array or urls or a comma-delimited list of urls
#   required: true
# - name: search
#   type: string
#   description: String
#   required: true
# - name: properties
#   type: array
#   description: The properties to return (defaults to all properties). See "Notes" for a listing of the available properties.
#   required: false
# examples:
# - '"https://www.flex.io", "Contact Us"'
# - '"https://news.ycombinator.com/", "Contact", "link"'
# notes: |
#   The following properties are allowed:
#     * `link`: the link corresponding to the matched item
#     * `domain`: the domain corresponding to the matched item
# ---

import json
import aiohttp
import asyncio
import urllib
import itertools
from cerberus import Validator
from collections import OrderedDict
from bs4 import BeautifulSoup

def flexio_handler(flex):

    # get the input
    input = flex.input.read()
    try:
        input = json.loads(input)
        if not isinstance(input, list): raise ValueError
    except ValueError:
        raise ValueError

    # define the expected parameters and map the values to the parameter names
    # based on the positions of the keys/values
    params = OrderedDict()
    params['urls'] = {'required': True, 'validator': validator_list, 'coerce': to_list}
    params['search'] = {'required': True, 'type': 'string'}
    params['properties'] = {'required': False, 'validator': validator_list, 'coerce': to_list, 'default': '*'}
    input = dict(zip(params.keys(), input))

    # validate the mapped input against the validator
    v = Validator(params, allow_unknown = True)
    input = v.validated(input)
    if input is None:
        raise ValueError

    # get the urls to process
    search_urls = input['urls']

    # get the search term to use to find the corresponding links
    search_text = input['search']

    # get the properties to return and the property map
    property_map = OrderedDict()
    property_map['domain'] = 'domain'
    property_map['link'] = 'link'
    properties = [p.lower().strip() for p in input['properties']]

    # if we have a wildcard, get all the properties
    if len(properties) == 1 and properties[0] == '*':
        properties = list(property_map.keys())

    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(fetch_all(search_urls, search_text, properties))
    flex.output.write(result)

async def fetch_all(search_urls, search_text, properties):
    tasks = []
    async with aiohttp.ClientSession() as session:
        for search_url in search_urls:
            tasks.append(fetch(session, search_url, search_text, properties))
        content = await asyncio.gather(*tasks)
        return list(itertools.chain.from_iterable(content))

async def fetch(session, search_url, search_text, properties):
    async with session.get(search_url) as response:
        content = await response.text()
        return parseContent(content, search_url, search_text, properties)

def parseContent(content, search_url, search_text, properties):

    # extract the info and build up the result
    soup = BeautifulSoup(content, "lxml")

    result = []
    for item in soup.findAll(True, text=search_text):
        link, domain = '',''
        if item is not None and item.name == 'a':
            link = item.get('href','')
        else:
            parent = item.find_parent('a')
            if parent is not None and parent.name == 'a':
                link = parent.get('href','')
        if len(link) == 0:
            continue

        # if we don't have a complete url, use the search url as the base;
        # see here for info on urllib.parse: https://docs.python.org/3/library/urllib.parse.html
        link = urllib.parse.urljoin(search_url, link)
        domain = urllib.parse.urlparse(link)[1] # second item is the network location part of the url
        available_properties = {'domain': domain, 'link': link}

        # append the row to the result
        row = [available_properties.get(p,'') for p in properties]
        result.append(row)

    return result

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
