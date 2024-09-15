from bs4 import BeautifulSoup
import json
import feedparser
from time import sleep
from common import list_to_df
import requests_cache
import pandas as pd

def get_meetup_events(meetup_list, sleep_time=2, verbose=False, debug=True):
    session = requests_cache.CachedSession('eventCache')
    session.settings.expire_after = 60 * 60 * 24 * 7
    meetup_event_list = []
    for meetup in meetup_list:
        url = f'https://openrss.org/{meetup}/events/'
        feed = feedparser.parse(url)
        sleep(sleep_time)
        for entry in feed['entries']:
            feed_text = entry['summary']
            feed_soup = BeautifulSoup(feed_text, 'html.parser')
            event_link = feed_soup.find('a')['href']
            meetup_event_list.append(event_link)

    # dedupe event_list
    meetup_event_list = list(set(meetup_event_list))

    meetup_event_detail_list = []
    for event in meetup_event_list:
        response = session.get(event)
        if response.from_cache:
            datetimeScraped = response.created_at
        else:
            datetimeScraped = pd.to_datetime('now').strftime('%Y-%m-%dT%H:%M:%S%z')

        try:
            soup = BeautifulSoup(response.text, 'html.parser')
            if soup.title.string == 'Login to Meetup | Meetup':
                print('Past Event', event)
                continue
            next_data_script = soup.find('script', id='__NEXT_DATA__')
            json_data = json.loads(next_data_script.string)
            eventVenueName = json_data['props']['pageProps']['event']['venue']['name']
            if eventVenueName == 'Online event':
                if verbose: print('Online event', event)
                pass
            event_description = json_data['props']['pageProps']['event']['description']
            eventName = json_data['props']['pageProps']['event']['title']
            eventStartTime = json_data['props']['pageProps']['event']['dateTime']
            eventendTime = json_data['props']['pageProps']['event']['endTime']
            eventAddress = json_data['props']['pageProps']['event']['venue']['address']
            eventCity = json_data['props']['pageProps']['event']['venue']['city']
            eventState = json_data['props']['pageProps']['event']['venue']['state']
            groupName = json_data['props']['pageProps']['event']['group']['name']
            eventGoogleMaps = soup.find('a', {'data-event-label': 'event-location'})['href']
            meetup_event_detail_list.append(
                    [eventName, event, eventStartTime, eventendTime, eventVenueName, eventAddress, eventCity,
                     eventState, groupName, eventGoogleMaps, event_description, datetimeScraped])
            if verbose: print(event)
        except Exception as e:
            if debug: print('Exception', event, e)

        if not response.from_cache:
            sleep(sleep_time)
    return list_to_df(meetup_event_detail_list)
