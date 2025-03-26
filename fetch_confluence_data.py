import requests
import json
import os
import logging
from conf.confluence_config import CONFLUENCE_BASE_URL, CONFLUENCE_USERNAME, CONFLUENCE_API_TOKEN, OUTPUT_DIR

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

def get_confluence_auth():
    """
    Determine the authentication method based on the availability of CONFLUENCE_API_TOKEN.
    If CONFLUENCE_API_TOKEN is empty, use CONFLUENCE_PASSWORD for authentication.
    If CONFLUENCE_PERSONAL_ACCESS_TOKEN is provided, use it as a Bearer token.
    """
    from conf.confluence_config import (
        CONFLUENCE_API_TOKEN,
        CONFLUENCE_PASSWORD,
        CONFLUENCE_USERNAME,
        CONFLUENCE_PERSONAL_ACCESS_TOKEN,
    )
    if CONFLUENCE_PERSONAL_ACCESS_TOKEN:
        return {"Authorization": f"Bearer {CONFLUENCE_PERSONAL_ACCESS_TOKEN}"}
    elif CONFLUENCE_API_TOKEN:
        return (CONFLUENCE_USERNAME, CONFLUENCE_API_TOKEN)
    else:
        return (CONFLUENCE_USERNAME, CONFLUENCE_PASSWORD)

def fetch_confluence_data(endpoint, output_file, output_dir="."):
    """
    Fetch data from a Confluence REST API endpoint and save it to a JSON file.

    :param endpoint: API endpoint to fetch data from
    :param output_file: Name of the output JSON file
    :param output_dir: Directory to save the output file
    """
    logging.info(f"Fetching data from Confluence endpoint: {endpoint}")
    url = f"{CONFLUENCE_BASE_URL}/rest/api/{endpoint}"
    auth = get_confluence_auth()  # Use the updated authentication method
    headers = {
        "Content-Type": "application/json"
    }
    if isinstance(auth, dict):  # If using Personal Access Token
        headers.update(auth)
        auth = None  # No basic auth when using PAT

    start_at = 0
    limit = 50  # Confluence's default page size
    all_data = []

    try:
        while True:
            params = {"start": start_at, "limit": limit}
            response = requests.get(url, headers=headers, auth=auth, params=params)
            response.raise_for_status()
            data = response.json()

            # Append the current page's data to the all_data list
            if "results" in data:
                all_data.extend(data["results"])
                logging.info(f"Fetched {len(data['results'])} items (start={start_at}).")
            else:
                all_data.append(data)

            # Check if there are more pages
            if "_links" in data and "next" in data["_links"]:
                start_at += limit
            else:
                break

        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, output_file)
        with open(file_path, "w") as file:
            json.dump(all_data, file, indent=4)
        logging.info(f"Saved Confluence data from {endpoint} to {file_path}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from Confluence endpoint {endpoint}: {e}")

def confluence_fetch_and_save_all(output_dir="."):
    """
    Fetch all spaces from Confluence and save them to a JSON file.
    """
    fetch_confluence_spaces("confluence_spaces.json", "confluence_data")
    fetch_confluence_content("confluence_content.json", "confluence_data")
    fetch_confluence_groups("confluence_groups.json", "confluence_data")
    fetch_confluence_users("confluence_users.json", "confluence_data")

def fetch_confluence_spaces(output_file, output_dir="."):
    """
    Fetch all spaces from Confluence and save them to a JSON file.
    """
    logging.info("Fetching Confluence spaces...")
    fetch_confluence_data("space", output_file, output_dir)

def fetch_confluence_content(output_file, output_dir="."):
    """
    Fetch all content from Confluence and save it to a JSON file.
    """
    logging.info("Fetching Confluence content...")
    fetch_confluence_data("content", output_file, output_dir)

def fetch_confluence_groups(output_file, output_dir="."):
    """
    Fetch all groups from Confluence and save them to a JSON file.
    """
    logging.info("Fetching Confluence groups...")
    fetch_confluence_data("group", output_file, output_dir)

def fetch_confluence_users(output_file, output_dir="."):
    """
    Fetch all users from Confluence and save them to a JSON file.
    """
    logging.info("Fetching Confluence users...")
    fetch_confluence_data("user", output_file, output_dir)

if __name__ == "__main__":
    logging.info("Starting Confluence data fetch script...")
    confluence_fetch_and_save_all(OUTPUT_DIR)
    logging.info("Confluence data fetch script completed.")
