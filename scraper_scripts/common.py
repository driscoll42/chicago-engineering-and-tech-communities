import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials


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
