from bs4 import BeautifulSoup
import json
from time import sleep
from common import list_to_df
import requests_cache
import pandas as pd
import requests

def get_meetup_events(meetup_list, sleep_time=2, verbose=False, debug=True):
    session = requests_cache.CachedSession('eventCache')
    session.settings.expire_after = 60 * 60 * 24 * 7
    meetup_event_list = []
    for cnt, meetup in enumerate(meetup_list):
        url = f'{meetup}/events/'
        if verbose: print(f'{cnt}/{len(meetup_list)} -  {url}')
        response = requests.get(url)
        sleep(sleep_time)
        soup = BeautifulSoup(response.content, 'html.parser')
        script = soup.find_all('script', {'id': '__NEXT_DATA__'})[0]
        script_text = script.text
        script_json = json.loads(script_text)
        apollo_state = script_json['props']['pageProps']['__APOLLO_STATE__']
        event_keys = [key for key in apollo_state.keys() if 'Event:' in key]
        for key_cnt, key in enumerate(event_keys):
            event = apollo_state[key]
            event_url = event['eventUrl']
            meetup_event_list.append(event_url)
            if verbose: print(f'{key_cnt}/{len(event_keys)} -  {event_url}')

    # dedupe event_list
    meetup_event_list = list(set(meetup_event_list))

    meetup_event_detail_list = []
    for cnt, event in enumerate(meetup_event_list):
        if verbose: print(f'{cnt}/{len(meetup_event_list)} -  {event}')
        response = session.get(event)
        if response.from_cache:
            datetimeScraped = response.created_at
        else:
            datetimeScraped = pd.to_datetime('now').strftime('%Y-%m-%dT%H:%M:%S%z')

        try:
            soup = BeautifulSoup(response.text, 'html.parser')
            if soup.title.string == 'Login to Meetup | Meetup':
                if verbose: print('Past Event', event)
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
        except Exception as e:
            if debug: print('Exception', event, e)

        if not response.from_cache:
            sleep(sleep_time)
    return list_to_df(meetup_event_detail_list)
