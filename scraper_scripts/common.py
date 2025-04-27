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

    # Get rid of timezone if it exists, such as a -05:00 at the end of the string
    event_df['eventStartTime'] = event_df['eventStartTime'].str.replace(r'([-+]\d{2}:\d{2})', '', regex=True)
    event_df['eventendTime'] = event_df['eventendTime'].str.replace(r'([-+]\d{2}:\d{2})', '', regex=True)

    # Remove the T from the eventStartTime and eventendTime columns
    event_df['eventStartTime'] = event_df['eventStartTime'].str.replace('T', ' ')
    event_df['eventendTime'] = event_df['eventendTime'].str.replace('T', ' ')


    return event_df


def upload_to_gsheets(events_df, sheet_name = "Events", key_columns=['eventURL'], verbose=False):
    # Authenticate and initialize the Google Sheets client
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("chicago-events-435701-b0daa42d7c25.json", scope)
    client = gspread.authorize(creds)

    # Open the Google Sheet and select a specific worksheet by title
    spreadsheet = client.open("Chicago Engineering Community Sheet")
    sheet = spreadsheet.worksheet(sheet_name)  # Replace with your sheet title

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

import pandas as pd
from dateutil import parser
import markdown  # Make sure this is installed!

def create_event_markdown(event_df, name='Events'):
    # Clean up the eventStartTime column
    event_df['eventStartTime'] = event_df['eventStartTime'].apply(
        lambda x: parser.parse(x) if pd.notnull(x) else pd.NaT)

    # Sort and filter
    event_df = event_df.sort_values(by='eventStartTime', ascending=True)
    event_df = event_df[event_df['eventStartTime'] >= pd.Timestamp.now()]

    # Format date
    event_df['formatted_date'] = pd.to_datetime(event_df['eventStartTime']).dt.strftime('%A, %B %d @ %I:%M%p')

    # Deduplication
    event_df = event_df.drop_duplicates(subset='eventName', keep='first')
    event_df = event_df.drop_duplicates(subset=['eventStartTime', 'eventVenueName'], keep='first')

    event_string = ''

    # Build Markdown string
    for index, row in event_df.iterrows():
        event_string += f"### 🎉 **[{row['eventName'].strip()}]({row['eventURL']})**\n\n"
        event_string += f"👥 *Organization:* **{row['groupName']}**\n\n"
        event_string += f"🕒 *When:* **{row['formatted_date']}**\n\n"
        event_string += f"📍 *Where:* **{str(row['eventVenueName']).strip()}**\n\n"
        
        description = row['event_description'].strip()
        if len(description) > 500:
            description = description[:500].rsplit(' ', 1)[0] + '...'
        description = description.replace('\n', '\n>')
        event_string += f"📝 *Details:*\n> {description}\n\n"

    # Save Markdown
    with open(f'{name}.md', 'w', encoding='utf-8') as f:
        f.write(event_string)

    # Convert to HTML
    html_output = markdown.markdown(event_string, extensions=['extra', 'nl2br'])

    # Save HTML
    with open(f'{name}.html', 'w', encoding='utf-8') as f:
        f.write(html_output)


def create_discord_list(event_df, name='Events'):
    # start by cleaning up the eventStartTime column
    event_df['eventStartTime'] = event_df['eventStartTime'].apply(
            lambda x: parser.parse(x) if pd.notnull(x) else pd.NaT)

    # sort the events by date
    event_df = event_df.sort_values(by='eventStartTime', ascending=True)
    # filter events before today
    event_df = event_df[event_df['eventStartTime'] >= pd.Timestamp.now()]

    event_df['formatted_date'] = pd.to_datetime(event_df['eventStartTime']).dt.strftime('%Y-%m-%d')

    # if there are multiple records with the same eventName, only keep the one with the earliest eventStartTime
    event_df = event_df.drop_duplicates(subset='eventName', keep='first')
    # if an event has the same eventStartTime and eventVenueName, only keep the first one
    event_df = event_df.drop_duplicates(subset=['eventStartTime', 'eventVenueName'], keep='first')

    event_string_len = 0

    event_string = ''

    # iterate over the dataframe
    for index, row in event_df.iterrows():
        event_string_to_add = f"* [{row['eventName'].strip()}]({row['eventURL']}) - {row['formatted_date']}\n"
        if event_string_len + len(event_string_to_add) > 2000:
            # add two new lines
            event_string += '\n\n'
            event_string_len = 0
        event_string_len += len(event_string_to_add)
        event_string += event_string_to_add

        # save the event string to a file with utf-8 encoding
    with open(f'{name} Discord List.txt', 'w', encoding='utf-8') as f:
        f.write(event_string)







from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import pandas as pd

def scrape_user_events(luma_groups):
    """
    Scrapes user event pages from a list of user URLs and extracts event data into a DataFrame.
    
    Args:
    - luma_groups (list): List of URLs, filtering for those that contain "/user/".
    
    Returns:
    - pd.DataFrame: DataFrame containing user URLs, event URLs, and event content.
    """
    # Setup Chrome options
    options = Options()
    # options.add_argument('--headless')  # Uncomment if you want headless
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 10)
    
    event_data = []
    
    try:
        # Filter for user URLs
        user_pages = [url for url in luma_groups if "/user/" in url]
        print(f"👤 Found {len(user_pages)} user pages")

        for user_url in user_pages:
            print(f"\n🌐 Opening user page: {user_url}")
            driver.get(user_url)
            time.sleep(2)  # Wait for page to load

            # Find all event links
            print("🔎 Searching for event links...")
            try:
                links = driver.find_elements(By.CLASS_NAME, "event-link")
                print(f"✅ Found {len(links)} event(s)")
            except Exception as e:
                print(f"⚠️ Failed to find event links: {e}")
                continue

            # Get all hrefs before DOM changes
            event_hrefs = [link.get_attribute("href") for link in links if link.get_attribute("href")]

            for idx, href in enumerate(event_hrefs):
                print(f"\n🔹 [{idx+1}/{len(event_hrefs)}] Visiting event: {href}")
                try:
                    driver.get(href)

                    # Wait for content container
                    container = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "page-container")))
                    page_source = driver.page_source
                    soup = BeautifulSoup(page_source, 'html.parser')

                    content_div = soup.find("div", class_="jsx-307934893 page-container")
                    event_text = content_div.get_text(separator="\n", strip=True) if content_div else "No content found"

                    print("📝 Event content snippet:")
                    print(event_text[:300], "...")

                    event_data.append({
                        "user_url": user_url,
                        "event_url": href,
                        "content": event_text
                    })

                except Exception as e:
                    print(f"❌ Error processing event: {e}")

                time.sleep(1)  # Be gentle

    except Exception as e:
        print(f"🔥 Top-level error: {e}")

    finally:
        print("🧹 Closing browser...")
        driver.quit()

    # Convert to DataFrame
    df = pd.DataFrame(event_data)
    print("\n📊 Final DataFrame preview:")
    print(df.head())
    return df


import pandas as pd

import pandas as pd

def filter_non_chicago_or_online_events(df):
    """
    This function takes a DataFrame and filters out events that are either not in Chicago or marked as 'Online'.
    It checks both the 'event_type' column and the 'eventVenueName' column for 'Online' status.

    Parameters:
    df (pd.DataFrame): The input DataFrame containing event data with 'location', 'event_type', and 'eventVenueName' columns.

    Returns:
    pd.DataFrame: A DataFrame containing only the events that are either not in Chicago or are labeled as 'Online'.
    """
    
    # Create a mask to identify events that are not in "Chicago" or are "Online"
    mask = (df['eventCity'] != 'Chicago') | (df['eventVenueName'].str.contains('Online', case=False, na=False))

    # Create a new DataFrame for those events
    not_chicago_or_online_df = df[mask].copy()

    # Add a column to label the events accordingly
    not_chicago_or_online_df['event_label'] = not_chicago_or_online_df.apply(
        lambda row: 'Not Chicago' if row['eventCity'] != 'Chicago' else ('Online' if 'Online' in row['eventVenueName'] else ''),
        axis=1
    )

    return not_chicago_or_online_df


events_df = get_gsheet_df("Events")