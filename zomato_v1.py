#!/usr/bin/python

###############################################
# Program: PRogram to scrape Zomato data
# Author: Ankur Pal
# Date: 17-12-2015
# Version 1.0
###############################################

from bs4 import BeautifulSoup as bs
import urllib2
import urllib
import multiprocessing as mp
import pandas as pd
import os
import re


def main():
    city = 'hyderabad'
    url = base_url + city + '/delivery'
    stpage = 56
    pages = 3
    print pages, 'pages to scrape'
    getRestaurants(city, stpage, pages)


def scrapePage(url, log=None):
    soup = None
    err = 0
    while ((soup == None) and (err < 5)):
        try:
            page = urllib2.urlopen(url).read()
            soup = bs(page, 'html.parser')
        except Exception, e:
            if log == None:
                print e
                print url
            else:
                log.write('Error: ' + str(e) + '\n')
                log.write('Url: ' + url + '\n')
            err += 1
            url = urllib.quote(url, safe=':/')
    return soup


def getRestaurants(city, stpage, pages):
    # seg = 10
    # n = max(1, pages/10)
    seg = 0
    # n = pages
    procs = []
    for i in xrange(seg + 1):
        p = mp.Process(target=scrapeRestaurants, args=(city, i, stpage, pages))
        p.start()
        procs.append(p)
    for p in procs:
        p.join()
    appendOutput(city, seg)
    deduplicate(city)


def scrapeRestaurants(city, i, st, pages):
    opf = open('data' + os.sep + city + '_' + str(i) + '.csv', 'w')
    log = open('logs' + os.sep + city + '_' + str(i) + '.log', 'w')
    page_url = base_url + city + '/delivery?page='
    end = st + pages - 1
    for p in xrange(st, end+1):
        url = page_url + str(p)
        soup = scrapePage(url, log)
        if soup:
            restaurants = pd.Series(list(soup.find_all('article', 'search-result')))
            restaurants = restaurants.apply(checkChain)
            data = restaurants.loc[restaurants['chain'] == 0, 'soup'].apply(getFields)
            data['chain'] = 0
            if (restaurants['chain'] == 1).sum():
                chainResult = getChainFields(
                    restaurants.loc[restaurants['chain'] == 1, 'soup'], log)
                chainResult['chain'] = 1
                data = pd.concat([data, chainResult], ignore_index=True)
            data.columns = ['Name', 'Locality', 'Rating', 'Votes', 'Address', 'Cuisines',
                            'CostFor2', 'Reviews', 'DelTime', 'MinOrder', 'Link', 'Chain']
            data['PageNumber'] = p

            data[['Latitude', 'Longitude']] = data['Link'].apply(lambda x: getLocation(x, log))
            log.write('Successfully scraped page ' + str(p) + ' of ' + str(pages) + '\n')
            data.to_csv(opf, index=False, header=False, encoding='utf-8')
        opf.flush()
        log.flush()
    opf.close()
    log.close()


def checkChain(restaurant):
    chain = (1 if restaurant.find('a', 'search-collapse') else 0)
    return pd.Series([restaurant, chain], index=['soup', 'chain'])


def getChainFields(chains, log=None):
    chainResult = None
    for chain in chains:
        url = base_url + chain.find('a', 'search-collapse')['href'].encode('utf-8').strip()
        soup = scrapePage(url, log)
        if soup:
            restaurants = pd.Series(list(soup.find_all('article', 'search-result')))
            result = restaurants.apply(getFields)
            chainResult = pd.concat([chainResult, result])
    return chainResult


def getFields(row):
    title = row.find('a', 'result-title')
    Name = text(title)
    Locality = text(row.find('a', 'search_result_subzone'))
    Rating = text(row.find('div', 'res-rating-nf'))
    Votes = text(row.find('div', 'rating-rank'), rep='votes')
    Address = text(row.find('div', 'search-result-address'))
    Cuisines = text(row.find('div', 'res-snippet-small-cuisine'), rep='Cuisines:')
    CostFor2 = text(row.find('div', 'res-cost'), spl=-1)
    Reviews = text(row.find('a', 'result-reviews'), rep='Reviews')
    DelTime = text(row.find('div', 'del-time'), rep='Delivery Time')
    MinOrder = text(row.find('div', 'del-min-order'), spl=-1)
    Link = title['href'].encode('utf-8').strip()
    return pd.Series([Name, Locality, Rating, Votes, Address, Cuisines, CostFor2,
                      Reviews, DelTime, MinOrder, Link])


def getLocation(url, log=None):
    soup = scrapePage(url, log)
    loc = ['', '']
    if soup:
        soup = soup('meta', property=re.compile('place:location:'))
        loc = [l['content'].encode('utf-8').strip() for l in soup]
    return pd.Series(loc)


def text(x, rep='', spl=None):
    x = x.text.encode('utf-8').replace(rep, '').strip() if x else ''
    return x.split()[spl] if (len(x) and spl) else x


def appendOutput(city, seg):
    opf = open('data' + os.sep + city + '_data.csv', 'a')
    # opf.write(
    #     'PageNumber,Name,Locality,Rating,Votes,Address,Cuisines,CostFor2,Reviews,DelTime,MinOrder,Link\n')
    for i in xrange(seg+1):
        ipf = open('data' + os.sep + city + '_' + str(i) + '.csv')
        for line in ipf:
            opf.write(line)
        ipf.close()
    opf.close()


def checkDir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def deduplicate(city):
    df = pd.read_csv('data' + os.sep + city + '_data.csv')
    df.drop_duplicates(['Link'], inplace=True)
    df.to_csv('data' + os.sep + city + '_data.csv', index=False)


base_url = 'https://www.zomato.com/'

if __name__ == "__main__":
    main()
    # checkDir('data')
    # checkDir('logs')
    # scrapeRestaurants('ncr', 0, 50, 50)
    # print getLocation('https://www.zomato.com/mumbai/cafe-zoe-lower-parel')
