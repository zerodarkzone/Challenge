import pandas as pd
import requests
import hashlib
import sqlite3
import time
import sys


def get_regions():
    try:
        s = requests.Session()
        s.headers.update({"x-rapidapi-host": "restcountries-v1.p.rapidapi.com",
                          "x-rapidapi-key": "d225a175a7msh7c36c2b25bf385ep16e06cjsn4359a0de5f78",
                          "useQueryString": "true"})
        response = s.get("https://restcountries-v1.p.rapidapi.com/all")
        response.raise_for_status()
        response_json = response.json()
        regions = set([])
        for country in response_json:
            regions.add(country['region'])
        regions = set(filter(lambda x: x != '', regions))
        return regions
    except (requests.HTTPError, ValueError, KeyError) as error:
        print("An error occurred while trying to get all the regions.", file=sys.stderr)
        print(error, file=sys.stderr)
        return set([])


def get_country_by_region(region):
    try:
        start = time.time()
        response = requests.get("https://restcountries.eu/rest/v2/region/%s" % region)
        response.raise_for_status()
        response_json = response.json()
        country = response_json[0]
        country_dict = {'Region': region, 'Country Name': country['name'],
                        'Language': hashlib.sha1(country['languages'][0]['nativeName'].encode()).hexdigest(),
                        'Time': (time.time() - start)}
        return country_dict
    except (requests.HTTPError, ValueError, KeyError, IndexError) as error:
        print("An error occurred while trying to get the country info.", file=sys.stderr)
        print(error, file=sys.stderr)
        return {}


def get_statistics(df):
    try:
        return {'Total': [df.Time.sum()], 'Mean': [df.Time.mean()], 'Min': [df.Time.min()], 'Max': [df.Time.max()]}
    except AttributeError as error:
        print("Could not get statistics.", file=sys.stderr)
        print(error, file=sys.stderr)
        return {}


def save_to_sqlite(df, name):
    try:
        con = sqlite3.connect('challenge.db')
        df.to_sql(name=name, con=con, if_exists='replace')
        con.close()
    except ValueError as error:
        print("Could not store DataFrame.", file=sys.stderr)
        print(error, file=sys.stderr)


def main():
    _regions = get_regions()
    _countries = [get_country_by_region(reg) for reg in _regions]
    c_df = pd.DataFrame(_countries)
    s_df = pd.DataFrame(get_statistics(c_df), index=['Time'])
    print(c_df)
    print(s_df)
    save_to_sqlite(c_df, 'Countries')
    save_to_sqlite(s_df, 'Statistics')
    c_df.to_json('data.json')


if __name__ == '__main__':
    main()
