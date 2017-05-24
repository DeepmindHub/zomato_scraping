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
import math as m


def main():
    city = 'ncr'
    url = base_url + city + '/delivery'
    soup = scrapePage(url)
    pages = soup.find(
        'div', 'col-l-3 mtop0 alpha tmargin pagination-number').text.encode(
        'utf-8').strip().split()[-1]
    pages = int(pages)
    print pages, 'pages to scrape'
    checkDir('data')
    checkDir('logs')
    getRestaurants(city, pages)


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


def getRestaurants(city, pages):
    seg = 20
    n = int(m.ceil(1.0 * pages / 10))
    procs = []
    for i in xrange(seg + 1):
        p = mp.Process(target=scrapeRestaurants, args=(city, i, n, pages))
        p.start()
        procs.append(p)
    for p in procs:
        p.join()
    appendOutput(city, seg)
    deduplicate(city)


def scrapeRestaurants(city, i, n, pages):
    opf = open('data' + os.sep + city + '_' + str(i) + '.csv', 'w')
    log = open('logs' + os.sep + city + '_' + str(i) + '.log', 'w')
    page_url = base_url + city + '/delivery?page='
    st = i * n + 1
    end = min((i + 1) * n, pages)
    chain_id_st = 1
    for p in xrange(st, end+1):
        url = page_url + str(p)
        soup = scrapePage(url, log)
        if soup:
            restaurants = pd.Series(list(soup.find_all('article', 'search-result')))
            restaurants = restaurants.apply(checkChain)
            data = restaurants.loc[restaurants['chain'] == 0, 'soup'].apply(getFields)
            data['chain'] = 0
            chain_ind = (restaurants['chain'] == 1)
            if chain_ind.sum():
                restaurants.loc[chain_ind, 'chain'] = range(chain_id_st, chain_ind.sum()+1)
                chain_id_st += (chain_ind.sum()+1)
                chainResult = getChainFields(
                    restaurants.loc[chain_ind], log)
                # chainResult['chain'] = 1
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
    # for chain in chains:
    for ind in chains.index:
        chain = chains.loc[ind, 'soup']
        url = base_url + chain.find('a', 'search-collapse')['href'].encode('utf-8').strip()
        soup = scrapePage(url, log)
        if soup:
            restaurants = pd.Series(list(soup.find_all('article', 'search-result')))
            result = restaurants.apply(getFields)
            result['chain'] = chains.loc[ind, 'chain']
            chainResult = pd.concat([chainResult, result])
    return chainResult


def getFields(row):
    title = row.find('a', 'result-title')
    Name = text(title)
    Locality = text(row.find('a', 'search_result_subzone'))
    Rating = text(row.find('div', 'res-rating-nf'))
    Votes = text(row.find('div', 'rating-rank'), rep='votes')
    Address = text(row.find('div', 'search-result-address'))
    # Cuisines = text(row.find('div', 'res-snippet-small-cuisine'), rep='Cuisines:')
    Cuisines = row.find('div', 'res-cuisine mt2 clearfix')['title'].encode('utf-8').strip()
    CostFor2 = text(row.find('div', 'res-cost'), spl=-1)
    Reviews = text(row.find('a', 'result-reviews'), rep='Reviews')
    DelTime = text(row.find('div', 'del-time'), rep='Delivery Time:')
    MinOrder = text(row.find('div', 'del-min-order'), spl=-1)
    Link = title['href'].encode('utf-8').strip()
    return pd.Series([Name, Locality, Rating, Votes, Address, Cuisines, CostFor2,
                      Reviews, DelTime, MinOrder, Link])


def getLocation(url, log=None):
    soup = scrapePage(url + '/info', log)
    loc = ['', '']
    if soup:
        soup = soup('meta', property=re.compile('place:location:'))
        loc = [l['content'].encode('utf-8').strip() for l in soup]
    return pd.Series(loc)


def text(x, rep='', spl=None):
    x = x.text.encode('utf-8').replace(rep, '').strip() if x else ''
    return x.split()[spl] if (len(x) and spl) else x


def appendOutput(city, seg):
    opf = open('data' + os.sep + city + '_data.csv', 'w')
    opf.write(
        'Name,Locality,Rating,Votes,Address,Cuisines,CostFor2,Reviews,DelTime,MinOrder,Link,Chain,PageNumber,Latitude,Longitude\n')
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
    # deduplicate('hyderabad')
    # checkDir('data')
    # checkDir('logs')
    # scrapeRestaurants('mumbai', 0, 5, 1)
    # print getLocation('https://www.zomato.com/mumbai/cafe-zoe-lower-parel')
    # chains = pd.Series([
    #     bs("""<article class="search-result ">\n<div class="row">\n<div class="search_result_info col-s-12 pr0">\n<div class="search-name clearfix">\n<h3 class="top-res-box-name left">\n<a class="result-title" data-result-type="ResCard_Name" href="https://www.zomato.com/mumbai/theobroma-powai" title="theobroma Restaurant, Powai">Theobroma </a>\n</h3>\n<div class="left chain_link"><a class="search-collapse" href="/mumbai/delivery?cid=34407"> 10 locations\xa0\u203a</a></div>\n</div>\n<a class="search-page-text zblack search_result_subzone" href="https://www.zomato.com/mumbai/powai-restaurants" title="Restaurants in Powai">Powai</a>\n</div>\n<div class="search_result_rating col-s-4 clearfix">\n<div class="tooltip rating-for-45707 res-rating-nf right level-8" data-res-id="45707" title="Excellent">\n                4.2\n            </div>\n<div class="clear"></div>\n<div class="rating-rank right">\n<!-- show the vote count only if there's a rating -->\n<span class="rating-votes-div-45707">1499 votes</span>\n</div>\n</div>\n</div>\n<div class="search_grid_100">\n<div class="search-result-address zdark" title="G 8, City Park Building, Central Avenue, Hiranadani Gardens, Powai, Mumbai"> G 8, City Park Building, Central Avenue, Hiranadani Gardens, Powai, Mumbai</div>\n<div class="search-page-text clearfix mtop0 mbot0">\n<div class="res-cuisine truncate"><div class="res-snippet-small-cuisine truncate search-page-text" title="Bakery, Cafe"><span class="zdark ttupper fontsize5">Cuisines: </span>Bakery, Cafe</div></div>\n<div class="res-type truncate"><div class="res-snippet-small-establishment" title="Caf\xe9s in Mumbai"><span class="zdark ttupper fontsize5">Type:</span> <a class="cblack" href="https://www.zomato.com/mumbai/caf\xe9">Caf\xe9</a></div></div>\n<div class="res-cost"><span class="upc cblack sml">Cost for 2</span> \u203a Rs. 600 </div>\n</div>\n<div class="search-collection-details clearfix mb10">\n<div class="highlight-heading">Featured in Collections </div>\n<div class="srp-collections"><a href="/mumbai/dessert-places">Sweet Tooth</a></div>\n</div>\n<div class="clear"></div>\n</div>\n<div class="photosContainer mt15 mb5" data-res_id="45707" data-result-type="ResCard_thumbnails">\n<div class="res-photo-thumbnails clearfix">\n<a class="res-image-view res-image-view--thumbs mr5 mb5" href="#"><img alt="Theobroma, Powai Photos" class="res-photo-thumbnail lazy" data-index="0" data-original="https://d.zmtcdn.com/data/pictures/7/45707/0c787422ce3b6c4fb7114497e242a635_200_thumb.jpg" data-photo_id="r_OTM0OTk2NjE2Nj" data-type="res" src="https://b.zmtcdn.com/images/photoback.png"/></a>\n<a class="res-image-view res-image-view--thumbs mr5 mb5" href="#"><img alt="Theobroma, Powai Photos" class="res-photo-thumbnail lazy" data-index="1" data-original="https://b.zmtcdn.com/data/pictures/7/45707/4399521d0554396f2fa0e5632ca0d0d3_200_thumb.jpg" data-photo_id="r_NzIyNTg5MDg3Mz" data-type="res" src="https://b.zmtcdn.com/images/photoback.png"/></a>\n<a aria-label="+1249 Photos" class="res-image-view res-image-view--thumbs mr5 mb5 last" href="#"><div class=" left-photo-count photomore user-info-thumbs-load-more res-photo-thumbnail" data-photo_id="r_MjQ1NjQ1NjQyMz" data-type="res">+1249</div></a>\n</div>\n</div>\n<div class="search_result_links pos-relative clearfix">\n<div class="search-result-links box-sizing-content">\n<div class="left mr5 mt5">\n<a class="result-menu btn btn--medium" data-icon="M" data-result-type="ResCard_Menu" href="https://www.zomato.com/mumbai/theobroma-powai/menu#tabtop" role="button" title="theobroma Menu">\n                    Menu\n                </a>\n<div class="clear"></div>\n</div>\n<div class="left mr5 mt5">\n<a class="result-reviews btn btn--medium search-result-reviews" data-icon="r" data-result-type="ResCard_Reviews" href="https://www.zomato.com/mumbai/theobroma-powai/reviews#tabtop" role="button" title="User reviews for Theobroma, Powai">\n                    742 Reviews\n                </a>\n<div class="clear"></div>\n</div>\n<div class="clear"></div>\n</div>\n</div>\n<div class="clear ieclear"></div>\n<div class="clear ieclear"></div>\n<div class="search_result_online_del row">\n<div class="col-s-8">\n<div class="btn btn--green o2_link white-text ttupper mt5" data-app_link="" data-href="https://www.zomato.com/restaurant?tab=order&amp;res_id=45707" data-iconr="\u01a7" data-res_id="45707" role="button">Order Now</div>\n</div>\n<div class="right pr10">\n<div class="del-time mb5">\n<span class="del-text">Delivery Time</span> 40 min\n                    </div>\n<div class="del-min-order">\n<span class="del-text">Minimum Order</span> Rs. 300\n                    </div>\n</div>\n<div class="clear"></div>\n</div>\n</article>""", 'html.parser')])
    # result = getChainFields(chains)
    # print result
