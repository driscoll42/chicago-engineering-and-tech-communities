import pandas as pd

def list_to_df(event_list):
    # Create a DataFrame from the event_detail_list
    event_df = pd.DataFrame(event_list,
                            columns=['eventName', 'eventURL', 'eventStartTime', 'eventendTime', 'eventVenueName',
                                     'eventAddress',
                                     'eventCity', 'eventState', 'groupName', 'eventGoogleMaps', 'event_description'])
    # We have cases where the same event is shared on multiple groups, just need to keep one copy
    event_df = event_df.sort_values('eventStartTime', ascending=True).drop_duplicates(
        subset=['eventName', 'eventVenueName', 'eventStartTime'])

    return event_df