# Proxy and TOR
from stem.control import Controller
from stem import Signal
import socket
import socks
import time
# HTTP requests
from fake_useragent import UserAgent
import requests
# HTML parsing
from bs4 import BeautifulSoup
# DataFrames
import pandas as pd
# Directory checking/creating
import os
# Command line arguments
import argparse


# Constants
RETRY_TIMES = 10
ROTATION_T = 10*60

# UserAgent object
ua = UserAgent()
# Header for requests
header = {'User-Agent':str(ua.chrome)}


def get_links(page):
    '''
    Returns list of links to the memes on the page
        page: int
            number of the page for parsing
        header: dict
            headers distionary that will be passed in requests.get()
        memes_links: [str]
            list of links
    '''
    link = 'http://knowyourmeme.com/memes/all/page/{}'.format(page)
    # Sending GET request
    try:
        response = requests.get(link, headers=header)
    except:
        return []
    if not response.ok:
        print('get_links', response.status_code)
        return []
    # Brewing soup
    soup = BeautifulSoup(response.text, features='lxml')
    # Lambda to filter all <a> tags that have 'class' attribute set to 'photo'
    a_photo_filter = lambda tag: tag.name == 'a' and tag.get('class') == ['photo']
    # Creating list of links to the memes
    memes_links = ['http://knowyourmeme.com' + a.get('href') for a in soup.find_all(a_photo_filter)]
    return memes_links


def get_stats(soup, stat):
    '''
    Return requested stat of a meme
        soup: bs4 soup
            soup of a meme
        stats: string
            stat name string: views/videos/photos/comments
        value: int
            stat of a meme
    '''
    dd = soup.find('dd', attrs={'class':stat})
    value = dd.find_next('a').text.replace(',', '') if dd else 0
    return int(value)


def get_properties(soup):
    '''
    Return dictionary containing properties of a meme
        soup: bs4 soup
            soup of a meme
        properties_dict: {str:[str/int]}
            dict of all useful properties
    '''
    # Creating return dict
    properties_dict = {}

    # Name
    m_name = soup.find('h1')
    m_name = m_name.text.strip() if m_name else ''
    properties_dict['name'] = m_name

    # Finding <aside> tag contaning needed information
    properties = soup.find('aside', attrs={'class':'left'})
    if properties:
        # Category
        m_category = properties.find('dl')
        m_category = m_category.find_next()
        m_category = m_category.text.strip() if m_category else ''
        properties_dict['category'] = m_category.lower()

        m_status = properties.find_next('dd')
        m_status = m_status.text.strip() if m_status else ''
        properties_dict['status'] = m_status.lower()

        m_type = properties.find('a', attrs={'class':'entry-type-link'})
        m_type = m_type.text.strip() if m_type else ''
        properties_dict['type'] = m_type.lower()

        m_year = properties.find(text='\nYear\n')
        m_year = m_year.find_next() if m_year else ''
        if m_year:
            m_year = int(m_year.text.strip()) if m_year.text.strip().isdigit() else m_year.text.strip().lower()
        properties_dict['year'] = m_year

        m_origin = properties.find('dd', attrs={'entry_origin_link'})
        m_origin = m_origin.text.strip() if m_origin else ''
        properties_dict['origin'] = m_origin.lower()

        m_tags = properties.find(text='\nTags\n')
        m_tags = m_tags.find_next() if m_tags else ''
        m_tags = m_tags.text.strip() if m_tags else ''
        properties_dict['tags'] = m_tags.lower()

    # Fetching dates
    times = soup.find_all('abbr', attrs={'class':'timeago'})
    for t in times:
        # Split parent's tag text
        time = t.parent.text.split('\n')
        # If added or updated found in list, get
        if 'Added' in time:
            properties_dict['added'] = t.get('title')
        elif 'Updated' in time:
            properties_dict['updated'] = t.get('title')
    return properties_dict


def get_text(soup):
    '''
    Returns dictionary of text info
        soup: bs4 soup
            soup of a meme
        text_dict: {str:str}
            dictionary of text info
    '''
    def remove_brackets(string, brackets):
        opening, closing = brackets
        # If deleting brackets
        if brackets == '[]':
            start = string.find(opening)
            end = string.find(closing)
            while start != -1 and end != -1:
                string = string.replace(string[start:end+1], '')
                start = string.find(opening)
                end = string.find(closing)
        # Else, deleting '(shown below)'
        else:
            substring = '(shown below)'
            index = string.find(substring)
            while index != -1:
                string = string.replace(substring, '')
                index = string.find(substring)
        return string

    text_dict = {}
    bodycopy = soup.find('section', attrs={'class':'bodycopy'})
    if bodycopy:
        # About text
        about = bodycopy.find('p')
        about = about.text.strip() if about else ''
        about = remove_brackets(about, '()')
        #about = remove_brackets(about, '[]')
        text_dict['about'] = about

        # Origin or history
        next_h = bodycopy.find(text='Origin') or bodycopy.find(text='History')
        next_h = next_h.find_next('p') if next_h else ''
        history = ''
        while next_h and next_h.name != 'h2':
            if next_h.name == 'p':
                history += next_h.text.strip()
            next_h = next_h.find_next()
        history = remove_brackets(history, '()')
        #history = remove_brackets(history, '[]')
        text_dict['history'] = history

        # Every other text
        next_o = next_h.find_next('p') if next_h else ''
        other = ''
        while next_o and (next_o.name != 'h2' and next_o.text != 'Search Interest'):
            if next_o.name == 'p':
                other += next_o.text.strip()
            next_o = next_o.find_next()
        other = remove_brackets(other, '()')
        #other = remove_brackets(other, '[]')
        text_dict['other'] = other

    return text_dict


def get_pic_link(soup):
    '''
        soup: bs4 soup
            soup of a meme page
        link: str
            link to a pic
    '''
    a = soup.find('a', attrs={'class':'photo left wide'})
    if not a:
        for a in soup.find_all('a', attrs={'class':'photo left'}):
            if a.get('href')[:6] != '/memes':
                break
    link = a.get('href') if a else ''
    return link


def get_data(link):
    '''
    Scraps the data from link
        link: str
            link to the meme
        data: {str:str/int}
            Dict contaning row of dataframe
    '''
    try:
        response = requests.get(link, headers=header)
    except:
        return {}
    if not response.ok:
        print("Error code in get_data:", response.status_code)
        return {}
    soup = BeautifulSoup(response.text, features='lxml')
    # Creating return dict
    data = {}
    # Updating with dicts returned by previously written functions
    for stat in ['views', 'videos', 'photos', 'comments']:
        data[stat] = get_stats(soup, stat)
    data.update(get_properties(soup))
    data.update(get_text(soup))
    # Adding pic link
    data['picture'] = get_pic_link(soup)

    return data


def check_ip():
    ip_url = 'https://api.ipify.org'
    r = requests.get(ip_url)
    if r.ok:
        return r.text


def get_last_page():
    url = 'https://knowyourmeme.com/memes/all'
    request = requests.get(url, headers=header)
    soup = BeautifulSoup(request.text, features='lxml')
    h1 = soup.find(text=' All Entries ')
    if h1:
        p = h1.find_next('p')
    # Get number of memes on the site
    meme_quantity = int(''.join([s for s in p.text if s.isnumeric()]))
    # Get last page index
    end = meme_quantity/16 + 1
    if end % 1 != 0:
        end += 1
    return int(end)


if __name__ == '__main__':
    # Command line argument parser
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--all', '-a', action='store_true', default=False)
    argparser.add_argument('--start', '-s', type=int, help='Parsing starting page', required=False)
    argparser.add_argument('--end', '-e', type=int, help='Parsing ending page', required=False)
    args = argparser.parse_args()

    # Bool to scrape all pages
    all = args.all
    if all:
        start = 1
        end = get_last_page()
    else:
        if not args.start or not args.end:
            parser.error('If --all is not set, start and end page must be provided')
        # Staring page
        start = args.start
        # Ending page
        end = args.end + 1

    def save(dataframe, fail=False):
        dirname = 'data'.format(start, end)
        # Creating a directory if it doesn't exist yet
        if not os.path.exists(dirname):
            os.mkdir(dirname)
        # Saving dataframe to csv overriding existing file
        dataframe.to_csv(dirname + '/data.csv'.format(start, page), encoding='utf-8', index=False)
        # Creating file with skipped pages numbers if some pages were skipped
        if skipped_pages:
            with open(dirname + '/skippedpages.txt', 'w') as f:
                f.write(' '.join([str(s) for s in skipped_pages]))
        # Creating file with skipped memes if some memes were skipped
        if skipped_links:
            with open(dirname + '/skippedlinks.txt', 'w') as f:
                for link in skipped_links:
                    f.write(link+'\n')
        # If function called after some exception has risen, save pagenumber
        if fail:
            with open(dirname + '/last.txt', 'w') as f:
                f.write(str(page))

    # Creating desired dataframe
    df = pd.DataFrame(columns=['name', 'category', 'status','year', 'added', 'updated',
                           'views', 'videos', 'photos', 'comments', 'tags', 'type',
                          'about', 'history', 'other'])

    skipped_pages = []
    skipped_links = []

    rotation_timer = time.time()

    # WARNING: UGLY CODE AHEAD
    try:
        with Controller.from_port(port=9051) as controller:
            print('Your actual IP-address:', check_ip())
            controller.authenticate()
            socks.set_default_proxy(socks.SOCKS5, 'localhost', 9150)
            socket.socket = socks.socksocket
            for page in range(start, end):
                # Saving each 10th page
                if (page - start + 1) % 10 == 0:
                    save(df)
                print("Page {}/{}:".format(page, end-1))
                print('Current IP:', check_ip())
                # Trying to load page multiple times just in case
                for i in range(RETRY_TIMES):
                    links = get_links(page)
                    # If everything is okay, break
                    if links:
                        break
                    else:
                        # If it's the last iteration and unsuccessful, add page to the list
                        if i == RETRY_TIMES-1:
                            skipped_pages.append(page)
                        # Rotate ip
                        controller.signal(Signal.NEWNYM)
                        # Wait until new tor-connection is established
                        time.sleep(controller.get_newnym_wait())
                        print('Changed IP to:', check_ip())
                        # Creating a header with new user-agent
                        header = {'User-Agent':str(ua.chrome)}
                for n, link in enumerate(links):
                    meme_name = link[link.rfind('/')+1:].replace('-', ' ').capitalize()
                    print("    Meme {}/{}: {}".format(n+1, len(links), meme_name))
                    # Trying to load meme multiple times just in case
                    for i in range(RETRY_TIMES):
                        # Getting the data
                        data_row = get_data(link)
                        if time.time() - rotation_timer > ROTATION_T:
                            # Each 10 minutes by default tor rotates your ip
                            # This code checking current time
                            # and creating new user-agent when it happens
                            rotation_timer = time.time()
                            header = {'User-Agent':str(ua.chrome)}
                        # If everyting is great, append a row and break
                        if data_row:
                            df = df.append(data_row, ignore_index=True)
                            break
                        else:
                            # If it's the last try and it was unsuccessful, add meme to the list
                            if i == RETRY_TIMES-1:
                                skipped_links.append(link)
                            # Rotate ip
                            controller.signal(Signal.NEWNYM)
                            time.sleep(controller.get_newnym_wait())
                            print('Changed IP to:', check_ip())
                            # Creating a header with new user-agent
                            header = {'User-Agent':str(ua.chrome)}
            # Saving after everything went okay
            save(df)
            # Loading skipped links if there are some
            if skipped_links:
                for link in skipped_links:
                    # Again, trying loading multiple times
                    for i in range(RETRY_TIMES):
                        data_row = get_data(link, header)
                        if time.time() - rotation_timer > ROTATION_T:
                            rotation_timer = time.time()
                            header = {'User-Agent':str(ua.chrome)}
                        # If everyting is great, append a row and break
                        if data_row:
                            df = df.append(data_row, ignore_index=True)
                            break
                        else:
                            # Rotate ip
                            controller.signal(Signal.NEWNYM)
                            time.sleep(controller.get_newnym_wait())
                            print('Changed IP to:', check_ip())
                            header = {'User-Agent':str(ua.chrome)}
            # Printing summary
            print('Successfully scraped {} memes from {} pages!'.format(len(df), end-start))
    except:
        # If something went wrong, save the progress and raise an exception
        save(df, fail=True)
        raise
