from bs4 import BeautifulSoup
import json
import feedparser
from time import sleep
from common import list_to_df
import requests_cache
import pandas as pd


def get_luma_group_events(luma_groups, sleep_time=2, verbose=False, debug=True):
    session = requests_cache.CachedSession('eventCache')
    event_data = []
    for group_url in luma_groups:
        session.settings.expire_after = 60 * 60 * 24 * 7
        response = session.get(group_url)
        if not response.from_cache:
            sleep(sleep_time)
        soup = BeautifulSoup(response.text, 'html.parser')



        # get the contents of <script data-cfasync="false" type="application/ld+json">
        script = soup.find('script', {'type': 'application/ld+json'})
        json_data = json.loads(script.string)
        for event in json_data['events']:
            try:
                groupName = json_data['name']
                eventURL = event['@id']
                if verbose: print(eventURL)
                response = session.get(eventURL)
                if not response.from_cache:
                    sleep(sleep_time)

                soup = BeautifulSoup(response.text, 'html.parser')
                script = soup.find('script', {'type': 'application/ld+json'})
                json_data = json.loads(script.string)
                eventName = event['name']
                event_description = json_data['description']
                eventStartTime = json_data['startDate']
                eventendTime = json_data['endDate']
                if json_data['location']['address'] == 'Register to See Address' and json_data['location']['name'] == "Chicago, Illinois":
                    eventAddress = 'Register to See Address'
                    eventCity = 'Chicago'
                    eventState = 'Illinois'
                    eventGoogleMaps = ''
                else:
                    try:
                        eventAddress = event['location']['address']['streetAddress']
                        eventCity = event['location']['address']['addressLocality']
                        eventState = event['location']['address']['addressRegion']
                        eventVenueName = json_data['location']['name']

                        maps_links = soup.find('a', href=lambda href: href and 'https://www.google.com/maps/' in href)
                        eventGoogleMaps = maps_links['href']
                    except:
                        eventAddress = ''
                        eventCity = ''
                        eventState = ''
                        eventGoogleMaps = ''
                if eventCity == 'Chicago':
                    event_data.append(
                        [eventName, eventURL, eventStartTime, eventendTime, eventVenueName, eventAddress, eventCity, eventState,
                         groupName, eventGoogleMaps, event_description])
            except Exception as e:
                if verbose or debug: print(e)

    return list_to_df(event_data)