# -*- coding: utf-8 -*-
"""Geography of Usage for Open Access Books"""

import geopandas
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pydata_google_auth
import seaborn as sns
import matplotlib.gridspec as gs
import matplotlib.colors as colors
import json
from PIL import Image

project_id = 'coki-scratch-space'
HDF5_CANONICAL_FILENAME = 'data_cache.h5'
sns.set_context('paper', font_scale=1.4)

def get_continuous_cmap(hex_list, float_list=None):
    rgb_list = [rgb_to_dec(hex_to_rgb(i)) for i in hex_list]
    if float_list:
        pass
    else:
        float_list = list(np.linspace(0, 1, len(rgb_list)))

    cdict = dict()
    for num, col in enumerate(['red', 'green', 'blue']):
        col_list = [[float_list[i], rgb_list[i][num], rgb_list[i][num]] for i in range(len(float_list))]
        cdict[col] = col_list
    cmp = colors.LinearSegmentedColormap('my_cmp', segmentdata=cdict, N=256)
    return cmp


def hex_to_rgb(value):
    '''
    Converts hex to rgb colours
    value: string of 6 characters representing a hex colour.
    Returns: list length 3 of RGB values'''
    value = value.strip("#")  # removes hash symbol if present
    lv = len(value)
    return tuple(int(value[i:i + lv // 3], 16) for i in range(0, lv, lv // 3))


def rgb_to_dec(value):
    '''
    Converts rgb to decimal colours (i.e. divides each value by 256)
    value: list (length 3) of RGB values
    Returns: list (length 3) of decimal values'''
    return [v / 256 for v in value]

lilacs = get_continuous_cmap(['#ffffff', '#937cb9'])


################
# Collect Data #
################

def get_data(af,
             project_id=project_id):
    scopes = [
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/drive',
    ]

    credentials = pydata_google_auth.get_user_credentials(
        scopes,
    )

    # Collect the data from various tables
    cites = get_citation_data()
    webo = get_webometrics_data()
    continents = get_continents_data()
    normal = get_normalisation_data()
    chapters = get_chapterspages()
    tld = get_tld_data()
    usage = get_usage_data()

    with pd.HDFStore(HDF5_CANONICAL_FILENAME) as store:
        store['usage'] = usage
        store['cites'] = cites
        store['webo'] = webo
        store['continents'] = continents
        store['normal'] = normal
        store['chapters'] = chapters
        store['tld'] = tld
    af.add_existing_file(HDF5_CANONICAL_FILENAME, remove=True)

    # with pd.HDFStore('data_cache.h5') as store:
    #     store['usage'] = usage
    #     store['cites'] = cites
    #     store['webo'] = webo
    #     store['continents'] = continents
    #     store['normal'] = normal
    #     store['chapters'] = chapters
    #     store['tld'] = tld


def get_usage_data():
    sql = '''
    SELECT *
    FROM `coki-scratch-space.SpringerNature.rawv4`
    ORDER BY year, cluster, title 
    '''

    df = pd.read_gbq(sql, project_id=project_id)
    return df


def get_chapterspages():
    sql = '''
    SELECT *
    FROM `coki-scratch-space.SpringerNature.chapter_pagenumbers`
    '''

    df = pd.read_gbq(sql, project_id=project_id)
    return df


def get_citation_data():
    sql = '''
    SELECT 
        isbn, 
        citations as Citations
    FROM `coki-scratch-space.SpringerNature.citations`
    '''

    df = pd.read_gbq(sql, project_id=project_id)
    return df


def get_webometrics_data():
    sql = '''
    SELECT 
        isbn, 
        tld as TLD, 
        domain as Domains, 
        url
    FROM `coki-scratch-space.SpringerNature.webometrics`
    '''

    df = pd.read_gbq(sql, project_id=project_id)
    return df


def get_continents_data():
    sql = '''
SELECT *
FROM `coki-scratch-space.SpringerNature.springerTitleNames_Continents`
ORDER BY year, cluster, title
    '''
    df = pd.read_gbq(sql, project_id=project_id)
    return df


def get_normalisation_data():
    sql = '''
SELECT 
  Country, 
  Publication
FROM `coki-scratch-space.SpringerNature.normalization`
'''
    df = pd.read_gbq(sql, project_id=project_id)
    return df


def get_tld_data():
    sql = '''
SELECT *
FROM `coki-scratch-space.SpringerNature.tld`
'''
    df = pd.read_gbq(sql, project_id=project_id)
    return df


###################
# Data Processing #
###################

def process_usage_data(usage):
    # Minor data manipulations for date-time and number of months published
    usage['publication_month'] = usage.pubdate.dt.to_period('M')
    usage['month_only'] = pd.to_datetime(usage.month).dt.to_period('M')
    usage['months_published'] = usage.month_only - usage.publication_month
    usage['Months After Publication'] = usage.months_published.apply(lambda x: x.n)

    # Some renaming of columns for pretty graphs
    usage['Open Access'] = usage['is_oa']
    usage['short_cluster'] = usage.cluster.map(
        {'Social Sciences': 'Social Sci',
         'Humanities': 'Humanities',
         'Business & Economics': 'Business & Econ',
         'Medical, Biomedical and Life Sciences': 'Biomed',
         'Physical Sciences, Engineering, Math & Computer Science': 'Phys & Com Sci'
         }
    )

    return usage


def process_mapdata(usage):
    world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
    world.at[world.name == 'Norway', 'iso_a3'] = 'NOR'
    world.at[world.name == 'France', 'iso_a3'] = 'FRA'
    world.at[world.name == 'United States of America', 'name'] = 'United States'

    geogroupoa = usage[usage.is_oa == True].groupby('iso_a3').sum()['downloads']
    geogroupnoa = usage[usage.is_oa == False].groupby('iso_a3').sum()['downloads']
    world = world.set_index('iso_a3')
    mapdata = world.join(geogroupoa)
    mapdata = mapdata.join(geogroupnoa, rsuffix='_noa')
    mapdata['downloads'] = mapdata.downloads.fillna(1)
    mapdata['downloads_noa'] = mapdata.downloads_noa.fillna(1)
    mapdata['Total OA Book Downloads'] = mapdata.downloads
    mapdata['Total Non-OA Book Downloads'] = mapdata.downloads_noa

    num_oa_books = usage[usage.is_oa == True]['isbn'].nunique()
    num_noa_books = usage[usage.is_oa == False]['isbn'].nunique()
    mapdata['Average downloads per OA book'] = mapdata.downloads / num_oa_books
    mapdata['Average downloads per non-OA book'] = mapdata.downloads_noa / num_noa_books

    return mapdata, world


###############################
# Processing and Figures Main #
###############################

def plot_figures(af):
    """Main plotting and processing function"""
    store_filepath = af.path_to_cached_file(
        HDF5_CANONICAL_FILENAME, "get_data")

    print('Loading data from:', store_filepath)

    with pd.HDFStore(store_filepath) as store:
        usage = store['usage']
        cites = store['cites']
        webo = store['webo']
        continents = store['continents']
        normal = store['normal']
        chapters = store['chapters']
        tld = store['tld']

    usage = process_usage_data(usage)
    mapdata, world = process_mapdata(usage)

    in_text_data(af, usage, cites, mapdata, tld)
    figure1(af, usage)
    figure2(af, usage, cites, webo)
    figure_gini(af, usage)
    figure_oa_advantage(af, usage, cites, webo)
    scatter_chapters(af, usage, chapters)
    tld_bar(af, tld, usage)
    tld_table(af, tld)


    map_oa_noa(af, mapdata)
    av_downloads(af, usage, world)
    anonymous_where_no_logged(af, usage, world)
    anon_v_logged(af, usage, world)
    africa_title_effect(af, usage, continents, world)
    latam_title_effect(af, usage, continents, world)
    usage_normal_by_pubs(af, usage, world, normal)

    case_study('978-1-137-57878-5', usage, world, af)


################
# In text data #
################

def in_text_data(af, usage, cites, mapdata, tld):
    d = {}

    d['num_oa'] = usage[usage.is_oa==True].isbn.nunique()
    d['num_closed'] = usage[usage.is_oa==False].isbn.nunique()
    ratio = d['num_closed'] / d['num_oa']

    oa_downloads = usage[usage.is_oa==True].downloads.sum()
    closed_downloads = usage[usage.is_oa==False].downloads.sum()

    d['oa_times_more_downloads'] = int(np.round((oa_downloads / closed_downloads * ratio), decimals=0))

    downloads = usage.groupby(['is_oa', 'isbn'])
    downloads = downloads.agg(
        downloads=pd.NamedAgg(column='downloads', aggfunc='sum')
    )
    downloads.reset_index(inplace=True)
    tempdata = pd.merge(downloads, cites, on='isbn')

    d['oa_cites'] = int(tempdata[tempdata.is_oa==True].Citations.sum())
    d['closed_cites'] = int(tempdata[tempdata.is_oa==False].Citations.sum())

    d['oa_times_more_citations'] = np.round(d['oa_cites'] / d['closed_cites'] * ratio,
                                            decimals=1)

    # bycountry = usage.groupby(['iso_a3', 'is_oa']).agg(
    #     downloads = pd.NamedAgg(column='downloads', aggfunc='sum')
    # )
    # bycountry.reset_index(inplace=True)
    # only_oa = bycountry[(bycountry.loc[is_oa==False, 'downloads'] == 0) &
    #                     (bycountry.loc[(,True), 'downloads'] > 0)]
    # d['countries_with_oa_but_not_nonoa_usage'] = len(only_oa)
    # d['new_countries_in_africa'] = len(only_oa[only_oa.continent == "AFRICA"])
    # d['new_countries_total_downloads'] = only_oa.downloads.sum()
    # d['new_countries_downloads_pc'] = int(np.round(d['new_countries_total_downloads'] /
    #                                       usage[
    #                                           usage.logged==True
    #                                       ].downloads.sum() * 100,
    #                                       decimals=0))

    top_ten_tlds = tld[tld.rankTotal < 11]
    top_ten_tld_sites = top_ten_tlds.Total.sum()
    total_sites = tld.Total.sum()
    d['top_10_tld_pc_all'] = int(np.round((top_ten_tld_sites / total_sites * 100), decimals=0))

    d['oa_pc_increase_sites'] = int(np.round((tld.OATotal.sum() / tld.nonOATotal.sum() * 100 * ratio), decimals=0))

    d['num_countries_oa_books'] = int(usage[usage.is_oa==True].iso_a3.nunique())
    d['num_countries_noa_books'] = int(usage[usage.is_oa == False].iso_a3.nunique())

    for f in af.generate_file('text_data.json'):
        json.dump(d, f)


######################
# Individual Figures #
######################

def figure1(af, usage):
    # Group and calculate summary statistics
    groups = usage.groupby(['short_cluster', 'category', 'Months After Publication', 'Open Access'])
    grouped = groups.agg(
        downloads=pd.NamedAgg(column='downloads', aggfunc='sum'),
        num_books=pd.NamedAgg(column='isbn', aggfunc='nunique')
    )
    grouped['Downloads per Book'] = grouped.downloads / grouped.num_books
    grouped.reset_index(inplace=True)

    # Summary Panel
    panela = top_panel(sns.lineplot,
                       grouped,
                       'Months After Publication', 'Downloads per Book',
                       xlim=(-2, 40), ylim=(10, 20000),
                       linewidth=3)
    panela.set(yscale='log')
    panela.savefig('figure1a.png', bbox_inches='tight', dpi=300)
    af.add_existing_file('figure1a.png', remove=True)
    plt.close()

    # Main Panel
    panelb = grid_panel(sns.lineplot,
                        grouped,
                        'Months After Publication', 'Downloads per Book',
                        xlim=(-2, 40), ylim=(10, 20000),
                        linewidth=3)
    panelb.set(yscale='log')
    panelb.savefig('figure1b.png', bbox_inches='tight', dpi=300)
    af.add_existing_file('figure1b.png', remove=True)
    plt.close()
    combine_panels(af, 'figure1a.png', 'figure1b.png', 'figure1full.png')


def figure2(af, usage, cites, webo):
    # Data Processing
    downloads = usage.groupby(['Open Access', 'isbn'])
    downloads = downloads.agg(
        downloads=pd.NamedAgg(column='downloads', aggfunc='sum'),
        short_cluster=pd.NamedAgg(column='short_cluster', aggfunc='max'),
        category=pd.NamedAgg(column='category', aggfunc='max'))
    downloads.reset_index(inplace=True)

    # To make the y-scales comparable between metrics
    downloads['Downloads'] = downloads.downloads / 1000

    # Merging and reshaping the datasets
    figdata = pd.merge(downloads, cites, on='isbn')
    figdata = pd.merge(figdata, webo, on='isbn')
    figmelt = figdata.melt(id_vars=['Open Access', 'isbn', 'short_cluster', 'category'],
                           value_vars=['Downloads', 'Citations', 'Domains'],
                           value_name='Metric')

    # Plot the panels
    panela = top_panel(sns.barplot,
                       figmelt,
                       'variable', 'Metric')
    panela.set_axis_labels(x_var='', y_var='Number (Downloads in 1000s)')
    panela.savefig('figure2a.png', bbox_inches='tight', dpi=300)
    af.add_existing_file('figure2a.png', remove=True)
    plt.close()

    panelb = grid_panel(sns.barplot,
                        figmelt,
                        'variable', 'Metric', ci=None)
    panelb.set_axis_labels(x_var='', y_var='')
    panelb.savefig('figure2b.png', bbox_inches='tight', dpi=300)
    af.add_existing_file('figure2b.png', remove=True)
    plt.close()
    combine_panels(af, 'figure2a.png', 'figure2b.png', 'figure2full.png')


def figure_oa_advantage(af, usage, cites, webo):
    # Data Processing
    downloads = usage.groupby(['Open Access', 'isbn'])
    downloads = downloads.agg(
        downloads=pd.NamedAgg(column='downloads', aggfunc='sum'),
        short_cluster=pd.NamedAgg(column='short_cluster', aggfunc='max'),
        category=pd.NamedAgg(column='category', aggfunc='max'))
    downloads.reset_index(inplace=True)

    # To make the y-scales comparable between metrics
    downloads['Downloads'] = downloads.downloads / 1000

    # Merging and reshaping the datasets
    figdata = pd.merge(downloads, cites, on='isbn')
    figdata = pd.merge(figdata, webo, on='isbn')

    # Calculating the advantage
    adv_grouping = figdata.groupby(['short_cluster', 'category', 'Open Access'])
    adv_grouping = adv_grouping.agg(
        Downloads=pd.NamedAgg(column='downloads', aggfunc='sum'),
        Citations=pd.NamedAgg(column='Citations', aggfunc='sum'),
        Domains=pd.NamedAgg(column='Domains', aggfunc='sum'),
        numbooks=pd.NamedAgg(column='isbn', aggfunc='nunique'))
    adv_grouping.reset_index(inplace=True)

    advantage_list = []
    for cluster in adv_grouping.short_cluster.unique():
        for category in adv_grouping.category.unique():
            num_oa = adv_grouping[(adv_grouping.short_cluster == cluster) &
                                  (adv_grouping.category == category) &
                                  (adv_grouping['Open Access'] == True)]['numbooks']
            num_noa = adv_grouping[(adv_grouping.short_cluster == cluster) &
                                   (adv_grouping.category == category) &
                                   (adv_grouping['Open Access'] == False)]['numbooks']
            if len(num_oa) != 1:
                continue
            num_oa = num_oa.iloc[0]
            num_noa = num_noa.iloc[0]

            for metric in ['Downloads', 'Citations', 'Domains']:
                oa = adv_grouping[(adv_grouping.short_cluster == cluster) &
                                  (adv_grouping.category == category) &
                                  (adv_grouping['Open Access'] == True)][metric]
                noa = adv_grouping[(adv_grouping.short_cluster == cluster) &
                                   (adv_grouping.category == category) &
                                   (adv_grouping['Open Access'] == False)][metric]

                oa = float(oa.iloc[0]) / num_oa
                noa = float(noa.iloc[0]) / num_noa
                oa_adv = oa / noa
                advantage_list.append({'short_cluster': cluster,
                                       'category': category,
                                       'variable': metric,
                                       'Metric': oa_adv})
    advantage = pd.DataFrame(advantage_list)

    # Plot the panels
    panela = top_panel(sns.barplot,
                       advantage,
                       'variable', 'Metric',
                       hue=None, color='green')
    panela.set_axis_labels(x_var='', y_var='Open Access Advantage (times)')
    panela.savefig('figure_adv_a.png', bbox_inches='tight', dpi=300)
    af.add_existing_file('figure_adv_a.png', remove=True)
    plt.close()

    panelb = grid_panel(sns.barplot,
                        advantage,
                        'variable', 'Metric',
                        hue=None, color='green')
    panelb.set_axis_labels(x_var='', y_var='Open Access Advantage (times)')
    panelb.savefig('figure_adv_b.png', bbox_inches='tight', dpi=300)
    af.add_existing_file('figure_adv_b.png', remove=True)
    plt.close()
    combine_panels(af, 'figure_adv_a.png', 'figure_adv_b.png', 'figure_adv_full.png')


def figure_gini(af, usage):
    # Process Data
    countries = usage.groupby(['isbn', 'Open Access', 'iso_a3']).sum()['downloads']
    countries = countries.unstack('iso_a3', fill_value=0)
    countries = countries.sort_index(level=0)
    gini = usage[['isbn', 'short_cluster', 'category', 'Open Access']]
    gini = gini.groupby('isbn').first()
    gini.reset_index(inplace=True)
    gini.sort_values('isbn')
    cort = countries.reset_index().sort_values('isbn')
    del cort['Open Access']
    cort = cort.set_index(cort.isbn)
    del cort['isbn']
    cort = cort.values.astype(int)
    gini['Gini Coefficient'] = np.array([ineq(x) for x in cort])

    # Plot the Panels
    panela = top_panel(sns.barplot,
                       gini,
                       'Open Access', 'Gini Coefficient',
                       ylim=(0.70, 0.95),
                       hue=None, order=[True, False])
    panela.savefig('figure_gini_a.png', bbox_inches='tight', dpi=300)
    af.add_existing_file('figure_gini_a.png', remove=True)
    plt.close()

    panelb = grid_panel(sns.barplot,
                        gini,
                        'Open Access', 'Gini Coefficient',
                        ylim=(0.80, 0.95),
                        hue=None, order=[True, False])
    panelb.savefig('figure_gini_b.png', bbox_inches='tight', dpi=300)
    af.add_existing_file('figure_gini_b.png', remove=True)
    plt.close()
    combine_panels(af, 'figure_gini_a.png', 'figure_gini_b.png', 'figure_gini_full.png')


def ineq(arr):
    ## first sort
    sorted_arr = arr.copy()
    sorted_arr.sort()
    n = arr.size
    coef_ = 2. / n
    const_ = (n + 1.) / n
    weighted_sum = sum([(i + 1) * yi for i, yi in enumerate(sorted_arr)])
    return coef_ * weighted_sum / (sorted_arr.sum()) - const_


def scatter_chapters(af, usage, chapters):
    downloads = usage.groupby(['Open Access', 'isbn'])
    downloads = downloads.agg(
        downloads=pd.NamedAgg(column='downloads', aggfunc='sum'),
        months_published=pd.NamedAgg(column='month', aggfunc='nunique')
    )
    downloads['Downloads per Month'] = downloads.downloads / downloads.months_published
    downloads = downloads.reset_index()

    figdata = downloads.merge(chapters, on='isbn')
    figdata['Number of Chapters'] = figdata['nr_of_chapters']
    downloadsvchapters = sns.lmplot(x='Number of Chapters',
                                    y='Downloads per Month',
                                    hue='Open Access',
                                    data=figdata,
                                    )
    downloadsvchapters.set(xscale='log', yscale='log')
    downloadsvchapters.savefig('chapters_usage_scatter.png')
    af.add_existing_file('chapters_usage_scatter.png', remove=True)
    plt.close()

    figdata['Number of Pages'] = figdata['nr_of_arabic_pages']
    downloadsvpages = sns.lmplot(x='Number of Pages',
                                 y='Downloads per Month',
                                 hue='Open Access',
                                 data=figdata,
                                 )

    downloadsvpages.set(xscale='log', yscale='log')
    downloadsvpages.savefig('pages_usage_scatter.png')
    af.add_existing_file('pages_usage_scatter.png', remove=True)
    plt.close()


def tld_table(af, tld):
    table_headers = [
        'TLD',
        'Total Sites',
        'Total %',
        'Non-OA',
        'Non-OA %',
        'OA',
        'OA %'
    ]

    figdata = tld[tld.rankOA < 11].sort_values('OATotal')
    rows = []
    for row in figdata.to_dict('records'):
        d = {}
        d['TLD'] = row.get('Top_level_domains')
        d['Total Sites'] = row.get('OATotal')
        d['Total %'] = row.get('TotalPerc')
        d['Non-OA'] = row.get('nonOATotal')
        d['Non-OA %'] = row.get('nonOAPerc')
        d['OA'] = row.get('OATotal')
        d['OA %'] = row.get('OAPerc')
        rows.append(d)

    table = {
        'title': '',
        'columns': [{'name': column} for column in table_headers],
        'rows': rows
    }

    for f in af.generate_file('tld_table.json'):
        json.dump(table, f)


def tld_bar(af, tld, usage):
    tempdata = tld[tld.rankOA < 11].sort_values('OATotal', ascending=False)
    tempdata = tempdata.to_dict('records')

    num_oabooks = usage[usage.is_oa == True].isbn.nunique()
    num_nonoabooks = usage[usage.is_oa == False].isbn.nunique()
    dflist = []
    for row in tempdata:
        da = {}
        da['Top Level Domain'] = row['Top_level_domains']
        da['Open Access'] = True
        da['Number of sites per book'] = row['OATotal'] / num_oabooks
        dflist.append(da)

        db = {}
        db['Top Level Domain'] = row['Top_level_domains']
        db['Open Access'] = False
        db['Number of sites per book'] = row['nonOATotal'] / num_nonoabooks
        dflist.append(db)

    figdata = pd.DataFrame(dflist)
    panel = sns.barplot(x='Top Level Domain', y='Number of sites per book',
                        hue='Open Access', hue_order=[True, False], palette=['orange', 'blue'], data=figdata)
    panel.get_figure().savefig('tld_bar.png')
    af.add_existing_file('tld_bar.png', remove=True)
    plt.close()


###############
# Map Figures #
###############

def map_oa_noa(af, mapdata):
    panel = map_compare(mapdata, ['Total OA Book Downloads', 'Total Non-OA Book Downloads'],
                        vmin=1, vmax=10000000,
                        panel_titles=True,
                        legend_kwds={'label': 'Downloads',
                                     'orientation': "horizontal"})
    panel.savefig('map_oa_noa.png')
    af.add_existing_file('map_oa_noa.png', remove=True)
    plt.close()

    panel = map_compare(mapdata, ['Total OA Book Downloads', 'Total Non-OA Book Downloads'],
                        vmin=1, vmax=10000000,
                        panel_titles=True, cmap=lilacs,
                        legend_kwds={'label': 'Downloads',
                                     'orientation': "horizontal"})
    panel.savefig('map_oa_noa_lilac.png')
    af.add_existing_file('map_oa_noa_lilac.png', remove=True)
    plt.close()


def av_downloads(af, usage, world):
    oa_download = usage[usage.is_oa == True].groupby(['iso_a3']).sum()['downloads'].to_frame().reset_index().set_index(
        'iso_a3')
    noa_download = usage[usage.is_oa == False].groupby(['iso_a3']).sum()['downloads'].to_frame().reset_index().set_index(
        'iso_a3')

    oa_download_perbook = oa_download.div(usage[usage['is_oa'] == True]['isbn'].nunique())
    noa_download_perbook = noa_download.div(usage[usage['is_oa'] == False]['isbn'].nunique())

    mapdata = world.join(oa_download_perbook)
    mapdata = mapdata.join(noa_download_perbook, rsuffix='_noa')

    figdata = mapdata
    figdata['Average downloads per OA book'] = figdata.downloads.fillna(0)
    figdata['Average downloads per non-OA book'] = figdata.downloads_noa.fillna(0)

    panel = map_compare(figdata, ['Average downloads per OA book', 'Average downloads per non-OA book'],
                'fig3-download_perbook',
                # title = 'Average Downloads per Book',
                figsize=(12, 12),
                vmin=1, vmax=mapdata['downloads'].max(),
                panel_titles=True, cmap=lilacs,
                legend_kwds={'label': 'Downloads',
                             'orientation': "horizontal"})
    # panel = map_compare(mapdata, ['Average downloads per OA book', 'Average downloads per non-OA book'],
    #                     figsize=(12, 12),
    #                     vmin=1, vmax=mapdata['downloads'].max(),
    #                     panel_titles=True,
    #                     legend_kwds={'label': 'Downloads',
    #                                  'orientation': "horizontal"})
    panel.savefig('av_downloads.png')
    af.add_existing_file('av_downloads.png', remove=True)
    plt.close()

    panel = map_compare(mapdata, ['Average downloads per OA book', 'Average downloads per non-OA book'],
                        figsize=(12, 12),
                        vmin=1, vmax=mapdata['downloads'].max(),
                        panel_titles=True, cmap=lilacs,
                        legend_kwds={'label': 'Downloads',
                                     'orientation': "horizontal"})
    panel.savefig('av_downloads_lilac.png')
    af.add_existing_file('av_downloads_lilac.png', remove=True)
    plt.close()


def anonymous_where_no_logged(af, usage, world):
    country_logged = usage[['is_oa', 'logged', 'iso_a3', 'downloads']]  # logged downloads
    country_loggeda = country_logged.dropna()  # remove no downloads
    country_log = country_loggeda.groupby(['is_oa', 'logged', 'iso_a3']).sum()['downloads']  # grouping
    colog = country_log.to_frame().reset_index()  # reset index
    testlogged = colog.loc[colog['logged'] == True].groupby(['iso_a3']).sum()['downloads']  # extract logged
    testanon = colog.loc[colog['logged'] == False].groupby(['iso_a3']).sum()['downloads']  # extract anonymous
    nologged = testanon[~testanon.index.isin(testlogged.index)]  # extract only countries with logged usage
    figdata = world.join(nologged)
    figdata['Anonymous downloads from countries having no logged downloads'] = figdata.downloads.fillna(1)

    panel = map_compare(figdata, ['Anonymous downloads from countries having no logged downloads'],
                        figsize=(12, 6),
                        panel_titles=False,
                        legend_kwds={'label': 'Anonymous downloads',
                                     'orientation': "horizontal"})
    panel.savefig('anon_where_no_logged.png')
    af.add_existing_file('anon_where_no_logged.png', remove=True)
    plt.close()

    panel = map_compare(figdata, ['Anonymous downloads from countries having no logged downloads'],
                        figsize=(12, 6),
                        panel_titles=False, cmap=lilacs,
                        legend_kwds={'label': 'Anonymous downloads',
                                     'orientation': "horizontal"})
    panel.savefig('anon_where_no_logged_lilac.png')
    af.add_existing_file('anon_where_no_logged_lilac.png', remove=True)
    plt.close()


def anon_v_logged(af, usage, world):
    oalogged = usage[(usage.is_oa == True) &
                     (usage.logged == True)]
    oaanon = usage[(usage.is_oa == True) &
                   (usage.logged == False)]
    geooalogged = oalogged.groupby('iso_a3').sum()['downloads']
    geooaanon = oaanon.groupby('iso_a3').sum()['downloads']
    mapdata = world.join(geooalogged)
    mapdata = mapdata.join(geooaanon, rsuffix='_anon')

    mapdata['downloads'] = mapdata.downloads.fillna(1)
    mapdata['downloads_anon'] = mapdata.downloads_anon.fillna(1)
    mapdata['Logged access downloads'] = mapdata.downloads
    mapdata['Anonymous access downloads'] = mapdata.downloads_anon

    panel = map_compare(mapdata, ['Logged access downloads',
                                  'Anonymous access downloads'],
                        figsize=(12, 12),
                        vmin=1, vmax=mapdata['downloads_anon'].max(),
                        panel_titles=True,
                        legend_kwds={'label': 'Downloads',
                                     'orientation': "horizontal"})
    panel.savefig('anon_v_logged.png')
    af.add_existing_file('anon_v_logged.png', remove=True)
    plt.close()

    panel = map_compare(mapdata, ['Logged access downloads',
                                  'Anonymous access downloads'],
                        figsize=(12, 12),
                        vmin=1, vmax=mapdata['downloads_anon'].max(),
                        panel_titles=True, cmap=lilacs,
                        legend_kwds={'label': 'Downloads',
                                     'orientation': "horizontal"})
    panel.savefig('anon_v_logged_lilac.png')
    af.add_existing_file('anon_v_logged_lilac.png', remove=True)
    plt.close()


def africa_title_effect(af, usage, continents, world):
    panel = regional_effect("AFRICA",
                            ['Increase in downloads of all books with Africa in the title',
                             'Increase in downloads of OA books with Africa in the title',
                             'Increase in downloads of Non-OA books with Africa in the title'],
                            continents, usage, world, colornorm=colors.Normalize)

    panel.savefig('africa_title_effect.png')
    af.add_existing_file('africa_title_effect.png', remove=True)
    plt.close()

    panel = regional_effect("AFRICA",
                            ['Increase in downloads of all books with Africa in the title',
                             'Increase in downloads of OA books with Africa in the title',
                             'Increase in downloads of Non-OA books with Africa in the title'],
                            continents, usage, world, colornorm=colors.Normalize, cmap=lilacs)

    panel.savefig('africa_title_effect_lilac.png')
    af.add_existing_file('africa_title_effect_lilac.png', remove=True)
    plt.close()


def latam_title_effect(af, usage, continents, world):
    panel = regional_effect("LATIN_AMERICA",
                            ['Increase in downloads of all books with Latin America in the title',
                             'Increase in downloads of OA books with Latin America in the title',
                             'Increase in downloads of Non-OA books with Latin America in the title'],
                            continents, usage, world)

    panel.savefig('latam_title_effect.png')
    af.add_existing_file('latam_title_effect.png', remove=True)
    plt.close()

    panel = regional_effect("LATIN_AMERICA",
                            ['Increase in downloads of all books with Latin America in the title',
                             'Increase in downloads of OA books with Latin America in the title',
                             'Increase in downloads of Non-OA books with Latin America in the title'],
                            continents, usage, world, cmap=lilacs)

    panel.savefig('latam_title_effect_lilac.png')
    af.add_existing_file('latam_title_effect_lilac.png', remove=True)
    plt.close()

def usage_normal_by_pubs(af, usage, world, normal):
    # figdata = mapdata.merge(normal, on='name')
    # # figdata.dropna(subset = 'publications')
    # figdata['OA Downloads Normalized by Publication'] = figdata['Average downloads per OA book'] / \
    #                                                     figdata.publications
    # figdata['Non-OA Downloads Normalized by Publication'] = figdata['Average downloads per non-OA book'] / \
    #                                                         figdata.publications
    #
    # panel = map_compare(figdata, ['OA Downloads Normalized by Publication',
    #                               'Non-OA Downloads Normalized by Publication'],
    #                     figsize=(12, 12),
    #                     panel_titles=True,
    #                     legend_kwds={'label': 'Downloads',
    #                                  'orientation': "horizontal"})
    # panel.savefig('norm_downloads.png')
    # af.add_existing_file('norm_downloads.png', remove=True)
    # plt.close()

    pubs = normal.set_index('Country')
    oa_download = usage[usage.is_oa == True].groupby(['country']).sum()['downloads'].to_frame().reset_index().set_index(
        'country')
    noa_download = usage[usage.is_oa == False].groupby(['country']).sum()['downloads'].to_frame().reset_index().set_index(
        'country')
    oa_effect = oa_download['downloads'].div(pubs['Publication']).div(usage[usage['is_oa'] == True]['isbn'].nunique())
    noa_effect = noa_download['downloads'].div(pubs['Publication']).div(
        usage[usage['is_oa'] == False]['isbn'].nunique())
    oa_effect = oa_effect.to_frame()
    noa_effect = noa_effect.to_frame()

    oa_effect.rename(columns={0: 'downloads'}, inplace=True)
    noa_effect.rename(columns={0: 'downloads'}, inplace=True)

    mapdata = world.join(oa_effect, on='iso_a3')
    mapdata = mapdata.join(noa_effect, on='iso_a3', rsuffix='_noa')

    figdata = mapdata
    figdata['OA book downloads normalized by publication'] = figdata.downloads.fillna(0.0001)
    figdata['Non-OA book downloads normalized by publication'] = figdata.downloads_noa.fillna(0.0001)

    panel = map_compare(figdata, ['OA book downloads normalized by publication',
                                  'Non-OA book downloads normalized by publication'],
                        figsize=(12, 12),
                        panel_titles=True, cmap=lilacs,
                        legend_kwds={'label': 'Downloads',
                                     'orientation': "horizontal"})
    panel.savefig('norm_downloads_lilac.png')
    af.add_existing_file('norm_downloads_lilac.png', remove=True)
    plt.close()

##############
# Case Study #
##############

def case_study(isbn, usage, world, af):
    case_study_metadata(isbn, usage, af)
    casestudy_advantage_map(isbn, usage, world, af)
    casestudy_countrytable(isbn, usage, 'KEN', af)


def case_study_book(isbn, df):
    return df[df.isbn == isbn]


def case_study_group(cluster, category, year, df):
    return df[(df.cluster == cluster) &
              (df.category == category) &
              (df.year == year)]


def case_study_metadata(isbn, usage, af):
    book = case_study_book(isbn, usage)
    cluster = book.cluster.values[0]
    category = book.category.values[0]
    year = book.year.values[0]
    group = case_study_group(cluster, category, year, usage)

    nonoa = usage[usage.is_oa == False].groupby(['isbn']).agg(
        num_countries=pd.NamedAgg(column='country', aggfunc='nunique'),
        num_downloads=pd.NamedAgg(column='downloads', aggfunc='sum')
    )
    av_nonoa_countries = nonoa.num_countries.mean()

    nonoa_group = group[group.is_oa == False].groupby(['isbn']).agg(
        num_countries=pd.NamedAgg(column='country', aggfunc='nunique'),
        num_downloads=pd.NamedAgg(column='downloads', aggfunc='sum'),
        num_months=pd.NamedAgg(column='month', aggfunc='nunique')
    )
    av_group_nonoa_countries = nonoa_group.num_countries.mean()
    av_groupnonoa_downloads = nonoa_group.num_downloads.mean()
    av_groupnonoa_monthlydownloads = (nonoa_group.num_downloads / nonoa_group.num_months).mean()

    countries = usage.groupby(['isbn', 'Open Access', 'iso_a3']).sum()['downloads']
    countries = countries.unstack('iso_a3', fill_value=0)
    countries = countries.sort_index(level=0)
    gini = usage[['isbn', 'short_cluster', 'category', 'Open Access']]
    gini = gini.groupby('isbn').first()
    gini.reset_index(inplace=True)
    gini.sort_values('isbn')
    cort = countries.reset_index().sort_values('isbn')
    del cort['Open Access']
    cort = cort.set_index(cort.isbn)
    del cort['isbn']
    cort = cort.values.astype(int)
    gini['Gini Coefficient'] = np.array([ineq(x) for x in cort])
    book_gini = gini.loc[gini.isbn == isbn, 'Gini Coefficient'].values[0]

    case_study_metadata = {
        'Publication Year': int(year),
        'ISBN': isbn,
        'Discipline': 'Economics',
        'Product category': category,
        'Cluster': cluster,
        'Imprint': 'Palgrave Macmillan',
        'Total number of countries': int(book.country.nunique()),
        'Average number of countries for non-OA titles': int(av_nonoa_countries),
        'Average number of countries for non-OA titles in the same group': int(av_group_nonoa_countries),
        'Country Gini coefficient': np.round(book_gini, decimals=3),
        'Total chapter downloads': int(book.downloads.sum()),
        'Chapter downloads average for non-OA titles in the same category': int(av_groupnonoa_downloads),
        'Monthly mean average chapter downloads': int(book.downloads.sum() / book.month.nunique()),
        'Monthly mean average chapter download for non-OA titles in the same category':
            int(av_groupnonoa_monthlydownloads)
    }

    for f in af.generate_file('case_study_metadata.json'):
        json.dump(case_study_metadata, f)


def casestudy_advantage_map(isbn, usage, world, af):
    mapall = usage.groupby('iso_a3').sum()['downloads'].to_frame().reset_index()
    mapall.set_index('iso_a3', inplace=True)

    df1 = usage[usage.isbn == isbn].groupby(['iso_a3', ]).sum()['downloads'].to_frame()
    df1['times'] = df1.div(mapall).multiply(usage['isbn'].nunique())
    mapdata = world.query('continent == "Africa"')

    oa_effect = df1.div(mapall).multiply(usage['isbn'].nunique())
    mapdata = mapdata.join(oa_effect)

    figdata = mapdata
    figdata['Book downloads normalized by publication'] = figdata.downloads.fillna(1)
    panel = map_compare(figdata, ['Book downloads normalized by publication'],
                        xlim=(-20, 55), ylim=(-35, 38),
                        panel_titles=False,
                        legend_kwds={'label': 'Increase in usage',
                                     'orientation': "horizontal"})

    panel.savefig('case_study_advantage_map.png')
    af.add_existing_file('case_study_advantage_map.png', remove=True)
    plt.close()

    panel = map_compare(figdata, ['Book downloads normalized by publication'],
                        xlim=(-20, 55), ylim=(-35, 38),
                        panel_titles=False, cmap=lilacs,
                        legend_kwds={'label': 'Increase in usage',
                                     'orientation': "horizontal"})

    panel.savefig('case_study_advantage_map_lilac.png')
    af.add_existing_file('case_study_advantage_map_lilac.png', remove=True)
    plt.close()

def casestudy_countrytable(isbn, usage, focus_country, af):
    countries_all = usage.groupby('iso_a3').agg(
        country=pd.NamedAgg(column='country', aggfunc='first'),
        downloads=pd.NamedAgg(column='downloads', aggfunc='sum')
    )
    countries_all['rank'] = countries_all.downloads.rank(ascending=False)
    countries_all = countries_all.sort_values('downloads', ascending=False)

    book = case_study_book(isbn, usage)
    book_logged = book[book.logged == True].groupby('iso_a3').agg(
        country=pd.NamedAgg(column='country', aggfunc='first'),
        downloads=pd.NamedAgg(column='downloads', aggfunc='sum')
    )
    book_logged['rank'] = book_logged.downloads.rank()
    book_logged = book_logged.sort_values('downloads', ascending=False)

    book_anon = book[book.logged == False].groupby('iso_a3').agg(
        country=pd.NamedAgg(column='country', aggfunc='first'),
        downloads=pd.NamedAgg(column='downloads', aggfunc='sum')
    )
    book_anon = book_anon.sort_values('downloads', ascending=False)

    rows = []
    for i in range(10):
        rows.append({
            'Overall (all books)': countries_all.country.values[i],
            'Logged (Digital Kenya)': book_logged.country.values[i],
            'Anonymous (Digital Kenya)': book_anon.country.values[i]
        })

    if focus_country:
        rank = countries_all.loc[focus_country, 'rank']
        rows.append({
            'Overall (all books)': f'{focus_country} ({rank})',
            'Logged (Digital Kenya)': '',
            'Anonymous (Digital Kenya)': ''
        })

    case_study_countrytable = {
        'title': '',
        'columns': [{'name': 'Overall (all books)'},
                    {'name': 'Logged (Digital Kenya)'},
                    {'name': 'Anonymous (Digital Kenya)'}],
        'rows': rows
    }

    for f in af.generate_file('case_study_countrytable.json'):
        json.dump(case_study_countrytable, f)


#################
# Graph Layouts #
#################

def top_panel(chart_type,
              df, x, y,
              hue='Open Access', hue_order=[True, False],
              xlim=None, ylim=None,
              **kwargs):
    figpanel = sns.FacetGrid(df,
                             xlim=xlim, ylim=ylim,
                             legend_out=True,
                             height=4, aspect=2)
    if hue:
        figpanel.map(chart_type, x, y,
                     hue, hue_order=hue_order, palette=['darkorange', 'blue'], **kwargs)
    elif 'color' in kwargs.keys():
        figpanel.map(chart_type, x, y, **kwargs)
    else:
        figpanel.map(chart_type, x, y, palette=['darkorange', 'blue'], **kwargs)
    figpanel.add_legend(labels=['Open Access', 'Not Open Access'])
    return figpanel


def grid_panel(chart_type,
               df, x, y,
               hue='Open Access', hue_order=[True, False],
               xlim=None, ylim=None,
               row_order=['Monograph', 'Contributed volume', 'Brief'],
               col_order=['Humanities', 'Social Sci', 'Business & Econ', 'Biomed', 'Phys & Com Sci'],
               **kwargs):
    figpanel = sns.FacetGrid(df,
                             row='category', col='short_cluster',
                             margin_titles=True, sharey='row',
                             xlim=xlim, ylim=ylim,
                             row_order=row_order, col_order=col_order,
                             height=3.5)
    if hue:
        figpanel.map(chart_type, x, y,
                     hue, hue_order=hue_order, palette=['darkorange', 'blue'],
                     **kwargs)
    elif 'color' in kwargs.keys():
        figpanel.map(chart_type, x, y, **kwargs)
    else:
        figpanel.map(chart_type, x, y, palette=['darkorange', 'blue'],
                     **kwargs)

    for ax in figpanel.axes.flat:
        plt.setp(ax.texts, text="")
    figpanel.set_titles(template="", row_template='{row_name}', col_template='{col_name}')
    return figpanel


def combine_panels(af,
                   panela,
                   panelb,
                   new_image_name: str,
                   y_pad: int = 0):
    a_filepath = af.path_to_cached_file(
        panela)
    b_filepath = af.path_to_cached_file(
        panelb)

    a = Image.open(a_filepath)
    b = Image.open(b_filepath)

    total_width = b.size[0]
    total_height = a.size[1] + b.size[1]

    new_image = Image.new('RGB', (total_width, total_height), (255, 255, 255))
    new_image.paste(a, (0, 0))
    new_image.paste(b, (0, a.size[1] + y_pad))
    new_image.save(new_image_name)
    af.add_existing_file(new_image_name, remove=True)


###############
# Map Layouts #
###############

def map_compare(df, columns, title=None,
                vmin=None, vmax=None,
                cmap=None, colornorm=colors.LogNorm,  # Blues
                panel_titles=False,
                figsize=(18, 10),
                xlim=(-170, 180), ylim=(-60, 85),
                fig_kwargs={},
                gs_kwargs={'hspace': 0.2},
                legend_label=None,
                legend_kwds={'orientation': "horizontal"}):
    if not vmin:
        vmin = min([min(df[col]) for col in columns])
    if not vmax:
        vmax = max([max(df[col]) for col in columns])

    if not cmap:
        cmap = 'Blues'

    fig = plt.figure(figsize=figsize, **fig_kwargs)
    num_maps = len(columns)  # Extra row for colorbar
    height_ratios = ([1] * len(columns)) + [0.04]
    grid = gs.GridSpec(num_maps + 1, 3,
                       width_ratios=[1, 2, 1],
                       height_ratios=height_ratios,
                       **gs_kwargs)
    map_axes = []
    cax = fig.add_subplot(grid[-1:, 1:2])
    cmp = cmap
    for i, col in enumerate(columns):
        ax = fig.add_subplot(grid[i:i + 1, :])
        ax.set(xlim=xlim, ylim=ylim)
        ax.set_axis_off()
        if panel_titles:
            ax.text(1, 1, col,
                    transform=ax.transAxes,
                    fontsize='large',
                    fontweight='heavy',
                    horizontalalignment='right')
        if type(cmap) == list:  # If you want different cmaps
            cmp = cmap[i]
        if legend_label:
            legend_kwds['label'] = legend_label
        panel = df.plot(column=col, ax=ax,
                        norm=colornorm(vmin, vmax),
                        cmap=cmp, cax=cax,
                        linewidth=0.2, edgecolor="black",
                        # vmin=vmin, vmax=vmax,
                        legend=True,
                        legend_kwds=legend_kwds)
        # ax.set_title(title, fontsize=25)
        map_axes.append(ax)
        st = fig.suptitle(title, fontsize="x-large")
        st.set_y(0.95)
        fig.subplots_adjust(top=0.85)

    return ax.figure


def regional_effect(region, maptitles, continents, usage, world, colornorm=None, cmap=None):
    dfregion = continents.loc[continents[region] == True]
    rtitles = dfregion.reset_index()['ISBN13']
    mapall = usage.groupby('iso_a3').sum()['downloads'].to_frame().reset_index()
    mapall.set_index('iso_a3', inplace=True)

    # Regional OA titles in the set
    roatitles = dfregion[dfregion['isOA'] == 'yes'].reset_index()['ISBN13']

    # Regional Non-OA titles in the set
    rnoatitles = dfregion[dfregion['isOA'] == 'non'].reset_index()['ISBN13']

    # Downloads of OA and non-OA titles related to the region
    rDownload = usage[usage['isbn'].isin(rtitles.values)].groupby(['iso_a3']).sum()[
        'downloads'].to_frame().reset_index()
    roaDownload = usage[usage['isbn'].isin(roatitles.values)].groupby(['iso_a3']).sum()[
        'downloads'].to_frame().reset_index()
    rnoaDownload = usage[usage['isbn'].isin(rnoatitles.values)].groupby(['iso_a3']).sum()[
        'downloads'].to_frame().reset_index()

    # Number of OA and non-OA title(s) in the set
    rTitleCount = rtitles.nunique()
    roaTitleCount = roatitles.nunique()
    rnoaTitleCount = rnoatitles.nunique()

    # Regional Effect (Times a book is more downloaded than a book on the whole corpus)
    r_effect = rDownload.set_index('iso_a3').div(rTitleCount).div(mapall).multiply(usage['isbn'].nunique())
    roa_effect = roaDownload.set_index('iso_a3').div(roaTitleCount).div(mapall).multiply(usage['isbn'].nunique())
    rnoa_effect = rnoaDownload.set_index('iso_a3').div(rnoaTitleCount).div(mapall).multiply(usage['isbn'].nunique())

    mapdata = world.join(r_effect)
    mapdata = mapdata.join(roa_effect, rsuffix='_oa')
    mapdata = mapdata.join(rnoa_effect, rsuffix='_noa')

    mapdata['downloads'] = mapdata.downloads.fillna(1)
    mapdata['downloads_oa'] = mapdata.downloads_oa.fillna(1)
    mapdata['downloads_noa'] = mapdata.downloads_noa.fillna(1)
    mapdata[maptitles[0]] = mapdata.downloads
    mapdata[maptitles[1]] = mapdata.downloads_oa
    mapdata[maptitles[2]] = mapdata.downloads_noa

    if not colornorm:
        colornorm = colors.LogNorm
    panel = map_compare(mapdata, maptitles,
                        title=None,
                        figsize=(12, 18),
                        vmin=1, vmax=mapdata['downloads'].max(),
                        panel_titles=True,
                        colornorm=colornorm,
                        cmap=cmap,
                        legend_kwds={'label': 'Increase in Downloads', 'orientation': "horizontal"})
    return panel


def get_continuous_cmap(hex_list, float_list=None):
    rgb_list = [rgb_to_dec(hex_to_rgb(i)) for i in hex_list]
    if float_list:
        pass
    else:
        float_list = list(np.linspace(0, 1, len(rgb_list)))

    cdict = dict()
    for num, col in enumerate(['red', 'green', 'blue']):
        col_list = [[float_list[i], rgb_list[i][num], rgb_list[i][num]] for i in range(len(float_list))]
        cdict[col] = col_list
    cmp = colors.LinearSegmentedColormap('my_cmp', segmentdata=cdict, N=256)
    return cmp


def hex_to_rgb(value):
    '''
    Converts hex to rgb colours
    value: string of 6 characters representing a hex colour.
    Returns: list length 3 of RGB values'''
    value = value.strip("#")  # removes hash symbol if present
    lv = len(value)
    return tuple(int(value[i:i + lv // 3], 16) for i in range(0, lv, lv // 3))


def rgb_to_dec(value):
    '''
    Converts rgb to decimal colours (i.e. divides each value by 256)
    value: list (length 3) of RGB values
    Returns: list (length 3) of decimal values'''
    return [v / 256 for v in value]
