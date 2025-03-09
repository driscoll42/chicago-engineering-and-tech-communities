import os
import json
import asyncio
from pydantic import BaseModel
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
from crawl4ai.extraction_strategy import LLMExtractionStrategy


# Add this to llm_scrape.py
def scrape_events(urls):
    """
    Synchronous wrapper function for extract_events_with_llm
    that can be called from a Jupyter notebook
    """
    # Use subprocess to run the extraction in a separate process
    import subprocess
    import sys
    import json
    import tempfile

    # Create a temporary file to store the results
    with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as temp:
        temp_file = temp.name

    # Create a small script that will run the extraction
    script = f"""
import asyncio
import json
import sys
sys.path.append('.')  # Add current directory to path
from llm_scrape import extract_events_with_llm

async def main():
    urls = {urls}
    results = await extract_events_with_llm(urls)
    with open('{temp_file}', 'w') as f:
        json.dump(results, f)

asyncio.run(main())
"""

    # Write the script to a temporary file
    with tempfile.NamedTemporaryFile(suffix='.py', delete=False) as f:
        f.write(script.encode())
        script_file = f.name

    # Run the script in a separate process
    subprocess.run([sys.executable, script_file], check=True)

    # Read the results from the temporary file
    with open(temp_file, 'r') as f:
        results = json.load(f)

    # Clean up temporary files
    import os
    os.unlink(temp_file)
    os.unlink(script_file)

    return results


# Define your event data model
class EventDetails(BaseModel):
    event_title: str
    event_start_time: str
    event_end_time: str
    event_description: str
    event_organizer: str
    event_location: str

# Event Title, Event Start Datetime, Event End Datetime, Event Description, Event Group, Event Location.",
async def extract_events_with_llm(urls):
    # Create LLM extraction strategy

    # import the openai key from open_ai_key.json
    with open('open_ai_key.json', 'r') as f:
        openai_key = json.load(f)['key']

    llm_strategy = LLMExtractionStrategy(
            provider="openai/gpt-4o-mini",  # You can use other models too
            api_token=openai_key,
            schema=EventDetails.model_json_schema(),
            extraction_type="schema",
            instruction="From this webpage, search for and extract the following information: Event Title, Event Start Datetime, Event End Datetime, Event Description, Event Organizer, Event Location.",
            input_format="markdown",
            extra_args={"temperature": 0.0}
    )

    # Configure crawler
    config = CrawlerRunConfig(extraction_strategy=llm_strategy)

    results = []
    async with AsyncWebCrawler() as crawler:
        for url in urls:
            result = await crawler.arun(url=url, config=config)

            if result.success:
                data = json.loads(result.extracted_content)
                results.append({
                    "url"    : url,
                    "details": data
                })
            else:
                print(f"Failed to crawl {url}: {result.error_message}")

    # Show token usage statistics
    llm_strategy.show_usage()

    return results


event_urls = ['https://calendar.time.ly/s9lik1ia/event/77881281/20250319110000?r=https:%2F%2Fmn.gov%2Flaunchmn%2Fcalendar%2F;event%3D77881281;instance%3D20250319110000&popup=1',
'https://carlsonschool.umn.edu/faculty-research/gary-s-holmes-center-entrepreneurship/blog/foundersday2023',
'https://events.humanitix.com/angel-fest-2025/tickets',
'https://hclib.bibliocommons.com/events/6759fe81d6b435360093197f',
'https://hclib.bibliocommons.com/events/6759feaed6b4353600931a03',
'https://hclib.bibliocommons.com/events/6759ff2717cee0280055f62b']

# Run the extraction
event_data = asyncio.run(extract_events_with_llm(event_urls))
print(json.dumps(event_data, indent=2))

# save json to file
with open("event_data.json", "w") as f:
    json.dump(event_data, f, indent=2)


