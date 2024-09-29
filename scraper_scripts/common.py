import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from dateutil import parser


def list_to_df(event_list):
    # Create a DataFrame from the event_detail_list
    event_df = pd.DataFrame(event_list,
                            columns=['eventName', 'eventURL', 'eventStartTime', 'eventendTime', 'eventVenueName',
                                     'eventAddress', 'eventCity', 'eventState', 'groupName', 'eventGoogleMaps',
                                     'event_description', 'datetimeScraped'])
    # We have cases where the same event is shared on multiple groups, just need to keep one copy
    event_df = event_df.sort_values('eventStartTime', ascending=True).drop_duplicates(
            subset=['eventName', 'eventVenueName', 'eventStartTime'])

    return event_df


def upload_to_gsheets(events_df, key_columns=['eventURL'], verbose=False):
    # Authenticate and initialize the Google Sheets client
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("chicago-events-435701-b0daa42d7c25.json", scope)
    client = gspread.authorize(creds)

    # Open the Google Sheet and select a specific worksheet by title
    spreadsheet = client.open("Chicago Engineering Community Sheet")
    sheet = spreadsheet.worksheet("Events")  # Replace with your sheet title

    # Get existing data from the sheet
    existing_event_df = pd.DataFrame(sheet.get_all_records())
    if verbose: print(f'Existing data: {existing_event_df.shape}')

    # get only rows in combined_df that aren't in existing_data, based on the key_columns
    # Create a boolean mask for rows in new_df that are not in old_df
    mask = ~events_df.set_index(key_columns).index.isin(existing_event_df.set_index(key_columns).index)

    # Use the mask to get unique rows from new_df
    new_events_df = events_df[mask].reset_index(drop=True)
    if verbose: print(f'New data: {new_events_df.shape}')

    # If there are any new records to append
    if not new_events_df.empty:
        # Find the starting row for appending (after the existing data)
        next_row = len(existing_event_df) + 2  # Add 2 because index starts from 0 and there's a header row

        # Append the new data to the Google Sheet
        set_with_dataframe(sheet, new_events_df, include_column_header=False, row=next_row)
    else:
        print("No new records to append")


def get_gsheet_df(worksheet_title):
    # Authenticate and initialize the Google Sheets client
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("chicago-events-435701-b0daa42d7c25.json", scope)
    client = gspread.authorize(creds)

    # Open the Google Sheet and select a specific worksheet by title
    spreadsheet = client.open("Chicago Engineering Community Sheet")
    sheet = spreadsheet.worksheet(worksheet_title)  # Replace with your sheet title

    # Get existing data from the sheet
    existing_event_df = pd.DataFrame(sheet.get_all_records())
    return existing_event_df

def create_event_markdown(event_df):
    # start by cleaning up the eventStartTime column
    event_df['eventStartTime'] = event_df['eventStartTime'].apply(
        lambda x: parser.parse(x) if pd.notnull(x) else pd.NaT)

    # sort the events by date
    event_df = event_df.sort_values(by='eventStartTime', ascending=True)
    # filter events before today
    event_df = event_df[event_df['eventStartTime'] >= pd.Timestamp.now()]

    def custom_strftime(dt):
        return dt.strftime('%A, %B %d @ %-I:%M%p').replace(' 0', ' ')

    event_df['formatted_date'] = pd.to_datetime(event_df['eventStartTime']).dt.strftime('%A, %B %d @ %I:%M%p')

    # if there are multiple records with the same eventName, only keep the one with the earliest eventStartTime
    event_df = event_df.drop_duplicates(subset='eventName', keep='first')
    # if an event has the same eventStartTime and eventVenueName, only keep the first one
    event_df = event_df.drop_duplicates(subset=['eventStartTime', 'eventVenueName'], keep='first')

    event_string = ''

    # iterate over the dataframe
    for index, row in event_df.iterrows():
        # convert a eventStartTime to a string like July 6 @ 5PM

        event_string += f"**[{row['eventName']}]({row['eventURL']})**\n"
        event_string += f"* *When:* {row['formatted_date']}\n\n"
        event_string += f"* *Where:* {row['eventVenueName']}\n\n"
        # Get the first 300 characters of the description, but make sure to not cut off a word
        description = row['event_description']
        if len(description) > 500:
            description = description[:500].rsplit(' ', 1)[0] + '...'

        # if there are new lines in the description, add a > after them
        description = description.replace('\n', '\n>')
        event_string += f">{description}\n\n"


    # save the event string to a file with utf-8 encoding
    with open('events.md', 'w', encoding='utf-8') as f:
        f.write(event_string)