from bs4 import BeautifulSoup
import json
import feedparser
from time import sleep
from common import list_to_df
import requests_cache
import pandas as pd


def get_mHub_event_details(response):
    soup = BeautifulSoup(response.text, 'html.parser')

    event_details = soup.find('div', class_='event-details-col')

    eventVenueName = event_details.find('h4', string='Venue').find_next('p').text
    # if eventVenueName one of mHUB 1623 W Fulton St, mHUB Classroom, replace it with mHUB
    if eventVenueName in ['mHUB 1623 W Fulton St', 'mHUB Classroom']:
        eventVenueName = 'mHUB'

    if eventVenueName == '':
        return None, None, None, None, None, None, None

    location = event_details.find('h4', string='Location').find_next('p').text
    if '1623 W Fulton St' in location or '1623 West Fulton St' in location:
        eventAddress = '1623 W Fulton St'
        eventCity = 'Chicago'
        eventState = 'IL'
    else:
        eventAddress = location
        eventCity = None
        eventState = None

    eventGoogleMaps = event_details.find('h4', string='Location').find_next('a')['href']

    eventStartTime = event_details.find('h4', string='Date and Time').find_next('p').text
    eventStartTime = pd.to_datetime(eventStartTime, format='%m/%d/%y @ %I:%M %p').strftime('%Y-%m-%dT%H:%M:%S%z')

    # Getting the description
    description_tag = event_details.find('p', id='evDescription')
    description = ''
    for element in description_tag.next_siblings:
        if element.name == 'div' and element.find('h4', string='Venue'):
            break
        description += element.get_text(strip=True) + '\n'

    # Remove extra whitespace and strip
    event_description = ' '.join(description.split()).strip()

    return eventVenueName, eventAddress, eventCity, eventState, eventGoogleMaps, eventStartTime, event_description


def get_mhub_events(sleep_time=2, verbose=False, debug=True):
    url_mhub = 'https://www.mhubchicago.com/events'
    session = requests_cache.CachedSession('eventCache')
    session.settings.expire_after = 60 * 60 * 24
    responseurl_mhub = session.get(url_mhub)

    # Parse the HTML content with BeautifulSoup
    soup = BeautifulSoup(responseurl_mhub.text, 'html.parser')

    # Find all event elements on the page
    events = soup.find_all('div', class_='event__card')

    session.settings.expire_after = 60 * 60 * 24 * 5
    # Extract event names and URLs
    event_data = []
    for event in events:
        if verbose: print(event)
        try:
            eventName = event.find('h3').text.strip()
            eventURL = event.find('a')['href']
            eventURL = f"https://www.mhubchicago.com{eventURL}"
            response = session.get(eventURL)
            eventVenueName, eventAddress, eventCity, eventState, eventGoogleMaps, eventStartTime, event_description = get_mHub_event_details(
                    response)
            if eventVenueName is not None:
                event_data.append(
                        [eventName, eventURL, eventStartTime, '', eventVenueName, eventAddress, eventCity, eventState,
                         'mHUB',
                         eventGoogleMaps, event_description])
            if not response.from_cache:
                sleep(sleep_time)
        except Exception as e:
            if verbose or debug: print('Exception', event, e)

    return list_to_df(event_data)
