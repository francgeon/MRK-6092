
# import requests
import mysql.connector
import time
import sys
import numpy as np
import pandas as pd
from scipy.stats import f
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer as _sa

# globals
def _defGlobals():
    global the_base_url
    global the_sql
    global the_timer
    global the_months

    the_sql = dict()
    the_sql['dBase'] = ''
    the_sql['table'] = ''
    the_sql['user'] = ''
    the_sql['PW'] = ''

    the_sql['credentials'] = {
            'user': the_sql['user'],
            'password': the_sql['PW'],
            'db': the_sql['dBase'],
            'use_unicode': True,
            'charset': 'utf8',
            'host': '',
            'port': ,
            'connection_timeout': 6000,
            'allow_local_infile': True
        }


# functions
def _progress(total, progress):
    barLength, status = 20, ""
    progress = float(progress) / float(total)
    if progress >= 1.:
        progress, status = 1, "\r\n"
    block = int(round(barLength * progress))
    text = "\r[{}] {:.0f}% {}".format(
        "#" * block + "-" * (barLength - block), round(progress * 100, 0), status)
    sys.stdout.write(text)
    sys.stdout.flush()


def _getDB():
    the_db = mysql.connector.connect(**the_sql['credentials'])
    return the_db


def _execute(the_query, the_type=''):
    the_DB = _getDB()
    c = the_DB.cursor(buffered=True)
    c.execute(the_query)
    the_result = ''

    try:
        if 'fetchone' in the_type:
            the_result = c.fetchone()
            if the_result:
                the_result = the_result[0]
        if 'fetchall' in the_type:
            the_result = c.fetchall()
    except mysql.connector.Error:
        pass

    c.close()
    the_DB.commit()
    the_DB.close()

    return the_result


def cronbach_alpha(data=None, items=None, scores=None, subject=None, nan_policy='pairwise', ci=.95):
    #safety check
    assert isinstance(data, pd.DataFrame), 'data must be a dataframe.'
    assert nan_policy in ['pairwise', 'listwise']

    if all([v is not None for v in [items, scores, subject]]):
        # Data in long-format: we first convert to a wide format
        data = data.pivot(index=subject, values=scores, columns=items)

    # From now we assume that data is in wide format
    n, k = data.shape
    assert k >= 2, 'At least two items are required.'
    assert n >= 2, 'At east two raters/subjects are required.'
    err = 'All columns must be numeric.'

    assert all([data[c].dtype.kind in 'bfi' for c in data.columns]), err
    if data.isna().any().any() and nan_policy == 'listwise':
        # In R = psych:alpha(data, use="complete.obs")
        data = data.dropna(axis=0, how='any')

    # Compute covariance matrix and Cronbach's alpha
    C = data.cov()
    cronbach = (k / (k - 1)) * (1-np.trace(C) / C.sum().sum())
    # which is equivalent to
    # v = np.diag(C).mean()
    # c = C.values[np.tril_indices_from(C, k=-1)].mean()
    # cronbach = (k * c) / (v + (k - 1) * c)

    # Confidence intervals
    alpha = 1 - ci
    df1 = n - 1
    df2 = df1 * (k - 1)
    lower = 1 - (1 - cronbach) * f.isf(alpha / 2, df1, df2)
    upper = 1 - (1 - cronbach) * f.isf(1 - alpha / 2, df1, df2)
    return round(cronbach, 6), np.round([lower, upper], 3)


def _computeVader(the_table, the_comments):
    # setup for progress bar
    the_total = len(the_comments)
    i = 0
    # set analyzer
    analyzer = _sa()

    # compute and save the VADER score
    for comment in the_comments:
        score = analyzer.polarity_scores(comment[1])
        if score['compound'] != 0:
            the_query = (f"UPDATE {the_table} SET vader ={score['compound']} WHERE commentID = '{comment[0]}'")
            _execute(the_query)
        i += 1
        _progress(the_total, i)

    return




if __name__ == "__main__":
    _defGlobals()

    # 1. test connection
    #the_result = _execute("SELECT COUNT(*) FROM tripAdvisor", "fetchone")

    # 2. import data
    #_execute(f"DROP TABLE IF EXISTS {the_sql['table']}")
    #_execute(f"CREATE TABLE {the_sql['table']} LIKE tripAdvisor")
    #_execute(f"INSERT INTO {the_sql['table']} SELECT * from tripAdvisor")
    #print("We can now work on mySQL. Fetching data.")

    #quit()

    # 3. compute correlation
    # get raw data from our table
    the_query = f"SELECT audrey, emile, eloge, sidney, francis, sg\
                    FROM {the_sql['table']}\
                    WHERE commentID >= 677950538"
    the_query = ' '.join(the_query.split()) # replace runs of spaces by a single space

    # pass the array to pandas
    dataframe = pd.read_sql_query(the_query, _getDB())

    # 4. compute Cronbach's Alpha
    the_alpha = cronbach_alpha(data=dataframe)

    print()
    print('correlations')
    print(dataframe.corr())
    print()
    print('Alpha: ' + str(the_alpha))

    #quit()

    # 5. compute VADER
    # fetch comments
    the_query = f"SELECT commentID, comment\
                    FROM {the_sql['table']}\
                    WHERE vader IS NULL\
                        AND commentID >= 677950538"
    the_comments = _execute(the_query, 'fetchall')

    print()
    print('calculating VADER scores')
    print()
    _computeVader(the_sql['table'], the_comments)

    # 6. recompute correlations and alpha
    # get raw data fro mour table
    the_query = f"SELECT audrey, emile, eloge, sidney, francis, sg\
                    FROM {the_sql['table']}\
                    WHERE commentID >= 677950538"
    the_query = ' '.join(the_query.split()) # replace runs of spaces by a single space


    # pass the array to pandas
    dataframe = pd.read_sql_query(the_query, _getDB())
    the_alpha = cronbach_alpha(data=dataframe)

    print()
    print('correlations')
    print(dataframe.corr())
    print()
    print('Alpha: ' + str(the_alpha))

    # all done
    print('all done')