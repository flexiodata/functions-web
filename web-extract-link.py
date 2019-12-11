
# ---
# name: web-extract-link
# deployed: true
# title: Website Link Extraction
# description: Returns information for all hyperlinks on one-or-more web pages matching a search string; information includes domain, link, and matching text.
# params:
# - name: url
#   type: array
#   description: Urls of web pages to search; parameter can be a single url or a comma-delimited list of urls.
#   required: true
# - name: search
#   type: string
#   description: The search string to use to find the corresponding links.
#   required: true
# - name: properties
#   type: array
#   description: The properties to return (defaults to all properties). See "Notes" for a listing of the available properties.
#   required: false
# examples:
# - '"https://www.flex.io", "Contact Us"'
# - '"https://news.ycombinator.com/news?p=1,https://news.ycombinator.com/news?p=2,https://news.ycombinator.com/news?p=3","Show HN"'
# notes: |
#   The following properties are allowed:
#     * `domain`: the domain of the link for the matched item
#     * `link`: the link of the matched item
#     * `text`: the text of the matched item
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
    search_urls = [s.strip() for s in search_urls]

    # get the search term to use to find the corresponding links
    search_text = input['search']
    search_text = " ".join(search_text.split()).lower().strip() # remove leading/trailing/duplicate spaces and convert to lowercase

    # get the properties to return and the property map
    property_map = OrderedDict()
    property_map['domain'] = 'domain'
    property_map['link'] = 'link'
    property_map['text'] = 'text'
    properties = [p.lower().strip() for p in input['properties']]

    # if we have a wildcard, get all the properties
    if len(properties) == 1 and properties[0] == '*':
        properties = list(property_map.keys())

    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(fetch_all(search_urls, search_text, properties))

    # return the results
    result = json.dumps(result, default=to_string)
    flex.output.content_type = "application/json"
    flex.output.write(result)

async def fetch_all(search_urls, search_text, properties):
    tasks = []
    async with aiohttp.ClientSession() as session:
        for search_url in search_urls:
            tasks.append(fetch(session, search_url, search_text, properties))
        content = await asyncio.gather(*tasks)
        return list(itertools.chain.from_iterable(content))

async def fetch(session, search_url, search_text, properties):
    try:
        async with session.get(search_url) as response:
            content = await response.text()
            return parseContent(content, search_url, search_text, properties)
    except Exception:
        return []

def parseContent(content, search_url, search_text, properties):

    # extract the info and build up the result
    result = []

    # remove leading/trailing/duplicate spaces and convert to lowercase
    cleaned_search_text = " ".join(search_text.split()).lower().strip()

    # parse the content and look for anchors
    soup = BeautifulSoup(content, "lxml")
    for item in soup.findAll('a'):

        # get the anchor and item text
        anchor_href = item.get('href')
        anchor_text = item.text

        # remove leading/trailing/duplicate spaces and convert to lowercase
        # if the cleaned search text is in the cleaned anchor text, add the item to the result
        cleaned_anchor_text = " ".join(anchor_text.split()).lower().strip()
        if cleaned_search_text in cleaned_anchor_text:
            link = urllib.parse.urljoin(search_url, anchor_href)
            domain = urllib.parse.urlparse(link)[1] # second item is the network location part of the url
            row = [{'domain': domain, 'link': link, 'text': anchor_text}.get(p,'') for p in properties]
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

def to_string(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (Decimal)):
        return str(value)
    return value

def to_list(value):
    # if we have a list of strings, create a list from them; if we have
    # a list of lists, flatten it into a single list of strings
    if isinstance(value, str):
        return value.split(",")
    if isinstance(value, list):
        return list(itertools.chain.from_iterable(value))
    return None
