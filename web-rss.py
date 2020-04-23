
# ---
# name: rss-reader-multiple
# deployed: true
# title: RSS Reader
# description: Returns the articles from the RSS feed given by the URLs
# params:
# - name: url
#   type: array
#   description: Urls for which to get the info
#   required: true
# examples:
# - '"https://news.ycombinator.com/rss"'
# - '"https://news.ycombinator.com/rss,http://feeds.arstechnica.com/arstechnica/index/"'
# - 'A1:A3'
# notes:
# ---

import json
import aiohttp
import asyncio
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
    #params['columns'] = {'required': True, 'validator': validator_list, 'coerce': to_list}
    input = dict(zip(params.keys(), input))

    # validate the mapped input against the validator
    v = Validator(params, allow_unknown = True)
    input = v.validated(input)
    if input is None:
        raise ValueError

    urls = input['urls']
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(fetch_all(urls))
    flex.output.write(result)

async def fetch_all(urls):
    tasks = []
    async with aiohttp.ClientSession() as session:
        for url in urls:
            tasks.append(fetch(session, url))
        content = await asyncio.gather(*tasks)
        return list(itertools.chain.from_iterable(content))

async def fetch(session, url):
    async with session.get(url) as response:
        result = await response.text()
        return parseFeed(result)

def parseFeed(content):
    result = []
    soup = BeautifulSoup(content, "xml")
    items = soup.findAll("item")
    for i in items:
        result.append([i.title.text, i.link.text, i.pubDate.text, i.description.text])
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
