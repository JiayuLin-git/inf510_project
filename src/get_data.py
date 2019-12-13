import getopt
import os
import sys
import time

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tmdbv3api import Movie
from tmdbv3api import TMDb

tmdb = TMDb()
tmdb.api_key = '375d1add201d23995ae7f5ea7213cd7d'


def cmdargs(args):
    try:
        opts, args = getopt.getopt(args, '-s:-h', ['source=', 'help'])
    except getopt.GetoptError as error:
        print('Error:',error)
        exit()
    if not opts:
        print("usage: python <thisfilename>.py --source=<source> ('remote' or 'local' or 'test')")
        exit()
    for opt_name, opt_value in opts:
        if opt_name in ('-h', '--help'):
            print("usage: <thisfilename>.py -source=<source('remote' or 'local' or 'test')>")
            exit()
        if opt_name in ('-s', '--source'):
            if opt_value == 'local':
                return 'local'
            elif opt_value == 'remote':
                return 'remote'
            elif opt_value == 'test':
                return 'test'
            else:
                print("invalid source! the valid source is 'local' or 'remote' or 'test'")
                exit()


def get_movie_details(tmdbId, df_MovieLens):
    country, genres, budget, revenue, keywords, MLens_rating = '', '', np.NaN, np.NaN, '', np.NaN
    if tmdbId:
        movie_api = Movie()
        movie_api.wait_on_rate_limit = True
        movie_dict = movie_api.details(tmdbId).__dict__
        country_list = movie_dict['entries']['production_countries']
        if country_list:
            country = movie_dict['entries']['production_countries'][0]['iso_3166_1']
        genres_list = movie_dict['entries']['genres']
        genres = '/'.join([i['name'] for i in genres_list])
        budget = movie_dict['entries']['budget']
        revenue = movie_dict['entries']['revenue']
        keywords_list = movie_dict['entries']['keywords']['keywords']
        keywords = '/'.join([i['name'] for i in keywords_list])
        MLens_rating_Series = df_MovieLens[df_MovieLens['tmdbId'] == tmdbId]['rating']
        if len(MLens_rating_Series) == 1:
            MLens_rating = float(MLens_rating_Series)
        #time.sleep(0.5) # to ensure not to exceed the rate limit of API

    return [country, genres, budget, revenue, keywords, MLens_rating,tmdbId]


def proc_MovieLens(save=True):
    df = pd.read_csv('./data/MovieLens/movies.csv')
    year = []
    title = []
    for row in df['title']:
        year.append(row[-5:-1])
        title.append(row[:-7])
    df['year'] = year
    df.iloc[:, 1] = title

    iddf = pd.read_csv('./data/MovieLens/links.csv')
    df2 = pd.merge(df, iddf, how='left', on='movieId')

    df3 = pd.read_csv('./data/MovieLens/ratings.csv')
    df_merge = df3['rating'].groupby(df3['movieId']).mean()

    wholedf = pd.merge(df2, df_merge, how='left', on='movieId')
    wholedf[wholedf['tmdbId'].isna()] = 0
    wholedf['tmdbId'] = wholedf['tmdbId'].astype('int')

    if save:
        wholedf.to_csv('./data/MovieLens.csv')
    print('MovieLens dataset processed')
    return wholedf


def get_IMDB(source='',save=True):
    r = requests.get('https://www.imdb.com/chart/top')
    soup = BeautifulSoup(r.content, 'html.parser')
    main_table = soup.findAll('table')[0]
    main_body = main_table.find('tbody')
    movies = main_body.findAll('tr')
    if source == 'test':
        movies=movies[:25]
    movies_list = list()
    for movie in movies:
        movie_attrs = movie.findAll('td')
        attrs_list = movie_attrs[1].get_text().split()
        rank, movie_name, rel_year = int(attrs_list[0][:-1]), ' '.join(attrs_list[1:-1]), int(attrs_list[-1][1:-1])
        rating = float(movie_attrs[2].find('strong').get_text())
        a_movie_list = [rank, movie_name, rel_year, rating]
        movies_list.append(a_movie_list)

    df_movies = pd.DataFrame(np.array(movies_list), columns=['rank', 'name', 'year', 'IMDB_rating'])
    if save:
        if not os.path.exists('./data/raw_data'):
            os.mkdir('./data/raw_data')
        if source == 'test':
            df_movies.to_csv('./data/raw_data/IMDB_Top_250_raw_test.csv')
            print('Raw testing IMDB dataset got')
        else:
            df_movies.to_csv('./data/raw_data/IMDB_Top_250_raw.csv' )
            print('Raw IMDB dataset got')

    return df_movies


def get_Douban(source='',save=True):
    headers = {'user-agent': 'Mozilla/5.0'}

    movies_list = list()
    nums_list = [i for i in range(0, 250, 25)]
    if source == 'test':
        nums_list = [0]
    for num in nums_list:  # iterate through different web pages
        url = 'https://movie.douban.com/top250?start=%d&filter=' % num
        r = requests.get(url, headers=headers)
        soup = BeautifulSoup(r.content, 'html.parser')
        main_body = soup.findAll('ol')[0]
        movies = main_body.findAll('li')
        for movie in movies:  # iterate through all 25 movies on single page
            name = ''.join(movie.findAll('span', attrs={'class': 'title'})[0].get_text())  # the name here is in Chinese
            rank = int(movie.find('em').get_text())
            rating = float(movie.find('span', attrs={'class': 'rating_num'}).get_text())
            year = ''.join(list(filter(str.isdigit, movie.find('p').get_text())))
            # print(rank, '/ 250', name)
            a_movie_list = [rank, name, year, rating]
            movies_list.append(a_movie_list)


    df_movies = pd.DataFrame(movies_list, columns=['rank', 'name', 'year', 'douban_rating'])
    if save:
        if not os.path.exists('./data/raw_data'):
            os.mkdir('./data/raw_data')
        if source == 'test':
            df_movies.to_csv('./data/raw_data/Douban_Top_250_raw_test.csv')
            print('Raw testing Douban dataset got')
        else:
            df_movies.to_csv('./data/raw_data/Douban_Top_250_raw.csv')
            print('Raw Douban dataset got')

    return df_movies


def get_Yahoo(source='',save=True):
    file = open('./data/YahooMovies.txt', 'r', encoding='utf-8-sig')
    f = file.readlines()
    pd_data = pd.DataFrame(columns=['rank', 'name', 'year', 'Yahoo_rating'])
    rank = 1
    if source == 'test':
        f=f[:25]
    for line in f:
        wordslist = {}
        eachrowlist = line.split()
        wordslist['Yahoo_rating'] = eachrowlist[0]
        wordslist['rank'] = rank
        wordslist['name'] = ' '.join(eachrowlist[2:-1])
        wordslist['year'] = eachrowlist[-1][1:-1]
        pd_data = pd_data.append(wordslist, ignore_index='True')
        rank += 1

    if save:
        if not os.path.exists('./data/raw_data'):
            os.mkdir('./data/raw_data')
        if source == 'test':
            pd_data.to_csv('./data/raw_data/Yahoo_Top_500_raw_test.csv')
            print('Raw testing Yahoo dataset processed')
        else:
            pd_data.to_csv('./data/raw_data/Yahoo_Top_500_raw.csv')
            print('Raw Yahoo dataset processed')

    return pd_data


def proc_IMDB(df_IMDB_raw, df_MovieLens,source=''):
    lists_IMDB = df_IMDB_raw.values.tolist()
    for a_movie_list in lists_IMDB:
        movie_api = Movie()
        movie_api.wait_on_rate_limit = True
        while True:
            try:
                search = movie_api.search(a_movie_list[1])
                break
            except BaseException as error:
                print(error)
                time.sleep(0.25)
        tmdbId = 0
        for res in search:
            if ('release_date' not in res.__dict__) or (res.release_date.split('-')[0] != a_movie_list[2]):
                continue
            else:
                tmdbId = res.id
                break
        if tmdbId == 0 and len(search):
            tmdbId = search[0].id
        a_movie_list_extension = get_movie_details(tmdbId, df_MovieLens)
        a_movie_list += a_movie_list_extension
    df_IMDB = pd.DataFrame(lists_IMDB,
                           columns=['rank', 'name', 'year', 'IMDB_rating', 'country', 'genres', 'budget', 'revenue',
                                    'keywords', 'MLens_rating','tmdb_id'])
    if source == 'test':
        df_IMDB.to_csv('./data/IMDB_top_250_test.csv')
        print('IMDB testing dataset processed')
    else:
        df_IMDB.to_csv('./data/IMDB_top_250.csv')
        print('Complete IMDB dataset processed')
    return df_IMDB


def proc_Douban(df_Douban_raw, df_MovieLens,source=''):
    lists_Douban = df_Douban_raw.values.tolist()
    n=1
    for a_movie_list in lists_Douban:
        movie_api = Movie()
        movie_api.wait_on_rate_limit = True
        while True:
            try:
                search = movie_api.search(a_movie_list[1])
                break
            except BaseException as error:
                print(error)
                time.sleep(0.25)
        print(n)
        tmdbId = 0
        for res in search:
            if ('release_date' not in res.__dict__) or (res.release_date.split('-')[0] != a_movie_list[2]):
                continue
            else:
                tmdbId = res.id
                name = res.title
                break
        if tmdbId == 0 and len(search):
            tmdbId = search[0].id
            name = search[0].title
        a_movie_list[1] = name
        a_movie_list_extension = get_movie_details(tmdbId, df_MovieLens)
        a_movie_list += a_movie_list_extension
    df_Douban = pd.DataFrame(lists_Douban,
                             columns=['rank', 'name', 'year', 'Douban_rating', 'country', 'genres', 'budget', 'revenue',
                                      'keywords', 'MLens_rating','tmdb_id'])
    if source == 'test':
        df_Douban.to_csv('./data/Douban_top_250_test.csv')
        print('Douban testing dataset processed')
    else:
        df_Douban.to_csv('./data/Douban_top_250.csv')
        print('Complete Douban dataset processed')
    return df_Douban


def proc_Yahoo(df_Yahoo_raw, df_MovieLens,source=''):
    lists_Yahoo = df_Yahoo_raw.values.tolist()
    for a_movie_list in lists_Yahoo:
        movie_api = Movie()
        movie_api.wait_on_rate_limit = True
        while True:
            try:
                search = movie_api.search(a_movie_list[1])
                break
            except BaseException as error:
                print(error)
                time.sleep(0.25)
        tmdbId = 0
        for res in search:
            if ('release_date' not in res.__dict__) or (res.release_date.split('-')[0] != a_movie_list[2]):
                continue
            else:
                tmdbId = res.id
                break
        if tmdbId == 0 and len(search):
            tmdbId = search[0].id
        a_movie_list_extension = get_movie_details(tmdbId, df_MovieLens)
        a_movie_list += a_movie_list_extension
    df_Yahoo = pd.DataFrame(lists_Yahoo,
                            columns=['rank', 'name', 'year', 'Yahoo_rating', 'country', 'genres', 'budget', 'revenue',
                                     'keywords', 'MLens_rating','tmdb_id'])
    for i in range(510):
        df_Yahoo['year'][i] = re.sub('[()]', '', df_Yahoo['year'][i])
    df_Yahoo['year']=df_Yahoo['year'].astype(int)
    if source=='test':
        df_Yahoo.to_csv('./data/Yahoo_top_500_test.csv')
        print('Yahoo testing dataset processed')
    else:
        df_Yahoo.to_csv('./data/Yahoo_top_500.csv')
        print('Complete Yahoo dataset processed')
    return df_Yahoo


def main(source):
    if source == 'local':
        try:
            df_MovieLens = pd.read_csv('./data/MovieLens.csv')
            print('MovieLens Dataset loaded successfully')
        except Exception as error:
            print(error)
            df_MovieLens = proc_MovieLens()

        try:
            df_IMDB = pd.read_csv('./data/IMDB_top_250.csv')
            print('IMDB Dataset loaded successfully')
        except Exception as error:
            print(error)
            df_IMDB_raw = get_IMDB()
            df_IMDB = proc_IMDB(df_IMDB_raw, df_MovieLens)

        try:
            df_Douban = pd.read_csv('./data/Douban_top_250.csv')
            print('Douban Dataset loaded successfully')
        except Exception as error:
            print(error)
            df_Douban_raw = get_Douban()
            df_Douban = proc_Douban(df_Douban_raw, df_MovieLens)

        try:
            df_Yahoo = pd.read_csv('./data/Yahoo_top_500.csv')
            print('Yahoo Dataset loaded successfully')
        except Exception as error:
            print(error)
            df_Yahoo_raw = get_Yahoo()
            df_Yahoo = proc_Yahoo(df_Yahoo_raw, df_MovieLens)

        for column_name in df_Yahoo:
            if df_Yahoo[column_name].dtype == 'object':
                df_Yahoo[column_name] = df_Yahoo[column_name].fillna('')
            else:
                df_Yahoo[column_name] = df_Yahoo[column_name].fillna(0)

        for column_name in df_IMDB:
            if df_IMDB[column_name].dtype == 'object':
                df_IMDB[column_name] = df_IMDB[column_name].fillna('')
            else:
                df_IMDB[column_name] = df_IMDB[column_name].fillna(0)

        for column_name in df_Douban:
            if df_Douban[column_name].dtype == 'object':
                df_Douban[column_name] = df_Douban[column_name].fillna('')
            else:
                df_Douban[column_name] = df_Douban[column_name].fillna(0)
    else:
        df_MovieLens = proc_MovieLens()  # this file is needed in the following methods
        df_IMDB_raw = get_IMDB(source=source)
        df_Douban_raw = get_Douban(source=source)
        df_Yahoo_raw = get_Yahoo(source=source)
        df_IMDB = proc_IMDB(df_IMDB_raw, df_MovieLens,source=source)
        df_Douban = proc_Douban(df_Douban_raw, df_MovieLens,source=source)
        df_Yahoo = proc_Yahoo(df_Yahoo_raw, df_MovieLens,source=source)

    df_MovieLens, df_Yahoo, df_Douban, df_IMDB  # This line just shows the program works correctly....


if __name__ == "__main__":
    main(cmdargs(sys.argv[1:]))
