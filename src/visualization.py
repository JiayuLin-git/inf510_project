from collections import Counter

import matplotlib.pyplot as plt
from matplotlib import cm
import numpy as np
import pandas as pd
import wordcloud
from PIL import Image
from src.read_data import *


def word_count(df, col_name):
    '''
    calculates the appearance time of each words
    each element of the df[col_name] is a string like 'aaa/bbb/ccc', which will be processed to 'aaa', 'bbb' and 'ccc'

    Args:
        df (pd.DataFrame): Pandas Dataframe containing movies data
        col_name (string): one column name of given DataFrame

    Returns:
        dict: # of appearance of each word's in df[col_name]
    '''
    dic = {}
    for words in list(df[col_name]):
        words_list = words.split('/')
        for word in words_list:
            if word:
                dic[word] = dic.get(word, 0) + 1
    return dic


def year_count(df, col_name='year'):
    '''
    calculates the # of movies in each decades
    each element of df[col_name] is an int object
    abcd belongs to abc0 decade, such as 1989 in 1980s, 2001 in 2000s.

    Args:
        df (pd.DataFrame): Pandas Dataframe containing movies data
        col_name (string,optional): one column name of given DataFrame, Default to 'year'

    Returns:
        list: a list of tuples, # of movies in each decades, each tuple is like ('decade',# of movies)
    '''
    dic = dict()
    years_list = list(df[col_name][:250])
    for year in years_list:
        dic[str(year)[-4:-1] + '0'] = dic.get(str(year)[-4:-1] + '0', 0) + 1
    return sorted(list(dic.items()))


def create_cloud(df, image_path, title, col_name='keywords'):
    '''
    make word cloud figure to show the keywords frequency on different movies rating websites

    Args:
        df (pd.DataFrame): Pandas Dataframe containing movies data
        image_path (string): the relative path of mask image used for creating the word cloud
        title (string): figure title
        col_name (string, optional): column of given DataFrame to create the word cloud, Default to 'keywords')

    '''
    wordcount = word_count(df, col_name)
    mask = np.array(Image.open(image_path))
    wc = wordcloud.WordCloud(scale=5, mask=mask, max_words=200, max_font_size=100, background_color="white")
    wc.generate_from_frequencies(wordcount)
    plt.figure(figsize=(20, 20))
    plt.title(title, fontsize=32, y=0.96)
    plt.imshow(wc)
    plt.axis('off')
    plt.show()


def plot_year(df1=df_IMDB, df2=df_Douban, df3=df_Yahoo, col_name_1='IMDB', col_name_2='Douban', col_name_3='Yahoo'):
    '''
    plot line chart to indicate the release year distribution of top 250 movies on three different move rating websites

    Args:
        df1 (pd.DataFrame, optional): Pandas Dataframe containing movies data, Default to df_IMDB
        df2 (pd.DataFrame, optional): Pandas Dataframe containing movies data, Default to df_Douban
        df3 (pd.DataFrame, optional): Pandas Dataframe containing movies data, Default to df_Yahoo
        col_name_1 (string, optional): legend name of 1st line, Default to 'IMDB'
        col_name_2 (string, optional): legend name of 2nd line, Default to 'Douban'
        col_name_3 (string, optional): legend name of 3rd line, Default to 'Yahoo'

    '''
    df1 = pd.DataFrame(year_count(df1), columns=['Year', col_name_1])
    df2 = pd.DataFrame(year_count(df2), columns=['Year', col_name_2])
    df3 = pd.DataFrame(year_count(df3), columns=['Year', col_name_3])
    dfall = pd.merge(df1, df2, how='outer')
    dfall = pd.merge(dfall, df3, how='outer')
    dfall = dfall.fillna(0)
    dfall.set_index(['Year'], inplace=True)
    dfall.plot()
    plt.ylabel('# of movies')
    plt.title('Time distribution of favourited movies on different websites')


def same_movie(df1=df_IMDB, df2=df_Douban, df3=df_Yahoo, title1='IMDB', title2='Douban', title3='Yahoo'):
    '''
    Make the line chart to show the different ratings on the same movie from three different movie rating websites

    Args:
        df1 (pd.DataFrame, optional): Pandas Dataframe containing movies data, Default to df_IMDB
        df2 (pd.DataFrame, optional): Pandas Dataframe containing movies data, Default to df_Douban
        df3 (pd.DataFrame, optional): Pandas Dataframe containing movies data, Default to df_Yahoo
        title1 (string, optional): legend name of 1st line, Default to 'IMDB'
        title2 (string, optional): legend name of 2nd line, Default to 'Douban'
        title3 (string, optional): legend name of 3rd line, Default to 'Yahoo'

    '''
    overlap = pd.merge(df1[['tmdb_id', '%s_rating' % title1]], df2[['tmdb_id', '%s_rating' % title2]], on='tmdb_id')
    overlap = pd.merge(overlap, df3[['tmdb_id', '%s_rating' % title3]], on='tmdb_id')
    y1 = overlap['%s_rating' % title1]
    y2 = overlap['%s_rating' % title2]
    y3 = overlap['%s_rating' % title3] * 2

    IMDB_mean, IMDB_std = overlap['IMDB_rating'].mean(), overlap['IMDB_rating'].std()
    Douban_mean, Douban_std = overlap['Douban_rating'].mean(), overlap['Douban_rating'].std()
    Yahoo_mean, Yahoo_std = overlap['Yahoo_rating'].mean()*2, overlap['Yahoo_rating'].std()

    len = overlap.shape[0]
    x = range(0, len)
    plt.plot(x, y1, label='%s, mean=%.2f, std=%.2f' % (title1, IMDB_mean, IMDB_std))
    plt.plot(x, y2, label='%s, mean=%.2f, std=%.2f' % (title2, Douban_mean, Douban_std))
    plt.plot(x, y3, label='%s, mean=%.2f, std=%.2f' % (title3, Yahoo_mean, Yahoo_std))
    plt.xticks(np.arange(0, 19))
    plt.title('ratings on the same movie from three different websites')
    plt.legend()
    plt.show()


def time_series_genres(df, title):
    '''
    make a bar charts to show the audience's preference of movies genres in different time.

    Args:
        df (pd.DataFrame): Pandas Dataframe containing movies data
        title: name of the bar chart

    '''
    ninty_dic = word_count(df[(df['year'] < 2000) & (df['year'] > 1989)], 'genres')
    zero_dic = word_count(df[(df['year'] < 2010) & (df['year'] > 1999)], 'genres')
    ten_dic = word_count(df[(df['year'] < 2020) & (df['year'] > 2009)], 'genres')
    ninty_top3 = sorted(ninty_dic.items(), key=lambda x: x[1], reverse=True)[:3]
    zero_top3 = sorted(zero_dic.items(), key=lambda x: x[1], reverse=True)[:3]
    ten_top3 = sorted(ten_dic.items(), key=lambda x: x[1], reverse=True)[:3]

    top3 = ninty_top3 + zero_top3 + ten_top3

    plt.bar([0, 1, 2], [i[1] for i in ninty_top3], label='1990s')
    plt.bar([3, 4, 5], [i[1] for i in zero_top3], label='2000s')
    plt.bar([6, 7, 8], [i[1] for i in ten_top3], label='2010s')
    plt.title('%s Top 3 Genres from 1990' % title)
    plt.xticks(range(9), [i[0] for i in top3], rotation=60)

    plt.legend()
    plt.show()


def region_distribute(df, title):
    '''
    make a pie chart indicating the spatial distribution of where top rated movies are produced on different movie
    rating websites. Only top 6 popular regions are listed.

    Args:
        df (pd.DataFrame): Pandas Dataframe containing movies data
        title: name of the pie chart

    '''
    dic = {}
    for word in df['country']:
        if word:
            dic[word] = dic.get(word, 0) + 1

    dic = Counter(dic).most_common(6)
    country = []
    count = []
    for key, value in dic:
        country.append(key)
        count.append(value)
    percentage = []
    for i in count:
        j = i / sum(count)
        percentage.append(j)

    colors = ['lightcoral', 'peachpuff', 'bisque', 'khaki', 'turquoise',
              'lightskyblue', 'lightsteelblue', 'mediumpurple', 'thistle', 'pink']

    plt.title(title)
    plt.pie(percentage, labels=country, colors=colors, autopct='%1.lf%%', startangle=90, radius=1)
    plt.show()
