import requests
import json
import os
import logging
from jira_config import JIRA_BASE_URL, JIRA_USERNAME, JIRA_API_TOKEN

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

def jira_fetch_and_save_all(output_dir="."):
    """
    Fetch all available data from Jira and save it to JSON files.
    
    :param output_dir: Directory to save the output files
    """
    logging.info("Starting to fetch all Jira data...")
    # fetch_jira_issues("project = TEST AND status = 'To Do'", "jira_issues.json", output_dir)
    fetch_users("jira_users.json", output_dir)
    fetch_groups("jira_groups.json", output_dir)
    fetch_projects("jira_projects.json", output_dir)
    fetch_project_categories("jira_project_categories.json", "jira_data")
    fetch_configuration("jira_configuration.json", "jira_data")
    fetch_custom_fields("jira_custom_fields.json", "jira_data")
    fetch_cluster_nodes("jira_cluster_nodes.json", "jira_data")
    fetch_fields("jira_fields.json", output_dir)
    fetch_dashboards("jira_dashboards.json", output_dir)
    fetch_filters("jira_filters.json", output_dir)
    fetch_system_info("jira_system_info.json", output_dir)
    fetch_issue_security_schemes("jira_issue_security_schemes.json", output_dir)
    fetch_issue_types("jira_issue_types.json", output_dir)
    fetch_issue_type_schemes("jira_issue_type_schemes.json", output_dir)
    fetch_notification_schemes("jira_notification_schemes.json", output_dir)
    fetch_permission_schemes("jira_permission_schemes.json", output_dir)
    fetch_priorities("jira_priorities.json", output_dir)
    fetch_priority_schemes("jira_priority_schemes.json", output_dir)
    fetch_screens("jira_screens.json", output_dir)
    fetch_statuses("jira_statuses.json", output_dir)
    fetch_status_categories("jira_status_categories.json", output_dir)
    logging.info("Finished fetching all Jira data.")

def get_auth():
    """
    Determine the authentication method based on the availability of JIRA_API_TOKEN.
    If JIRA_API_TOKEN is empty, use JIRA_PASSWORD for authentication.
    """
    from jira_config import JIRA_API_TOKEN, JIRA_PASSWORD, JIRA_USERNAME
    if JIRA_API_TOKEN:
        return (JIRA_USERNAME, JIRA_API_TOKEN)
    else:
        return (JIRA_USERNAME, JIRA_PASSWORD)

def fetch_jira_issues(jql_query, output_file, output_dir="."):
    """
    Fetch Jira issues using the Jira REST API with paging and save them to a JSON file.
    
    :param jql_query: JQL query string to filter issues
    :param output_file: Name of the output JSON file
    :param output_dir: Directory to save the output file
    """
    logging.info(f"Fetching Jira issues with JQL: {jql_query}")
    url = f"{JIRA_BASE_URL}/rest/api/2/search"
    headers = {
        "Content-Type": "application/json"
    }
    auth = get_auth()  # Use the updated authentication method
    start_at = 0
    max_results = 50  # Jira's default page size
    all_issues = []

    try:
        while True:
            params = {
                "jql": jql_query,
                "startAt": start_at,
                "maxResults": max_results
            }
            response = requests.get(url, headers=headers, auth=auth, params=params)
            response.raise_for_status()
            data = response.json()

            # Append the current page's issues to the all_issues list
            if "issues" in data:
                all_issues.extend(data["issues"])
                logging.info(f"Fetched {len(data['issues'])} issues (startAt={start_at}).")

            # Check if there are more pages
            if "startAt" in data and "total" in data:
                start_at += max_results
                if start_at >= data["total"]:
                    break
            else:
                break

        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, output_file)
        with open(file_path, "w") as file:
            json.dump(all_issues, file, indent=4)
        logging.info(f"Saved Jira issues to {file_path}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching Jira issues: {e}")

def fetch_and_save(endpoint, output_file, output_dir="."):
    """
    Generic function to fetch data from a Jira REST API endpoint with paging and save it to a JSON file.
    
    :param endpoint: API endpoint to fetch data from
    :param output_file: Name of the output JSON file
    :param output_dir: Directory to save the output file
    """
    logging.info(f"Fetching data from endpoint: {endpoint}")
    url = f"{JIRA_BASE_URL}/rest/api/2/{endpoint}"
    headers = {
        "Content-Type": "application/json"
    }
    auth = get_auth()  # Use the updated authentication method
    start_at = 0
    max_results = 50  # Jira's default page size
    all_data = []

    try:
        while True:
            params = {"startAt": start_at, "maxResults": max_results}
            response = requests.get(url, headers=headers, auth=auth, params=params)
            response.raise_for_status()
            data = response.json()

            # Append the current page's data to the all_data list
            if "values" in data:
                all_data.extend(data["values"])
                logging.info(f"Fetched {len(data.get('values', []))} items (startAt={start_at}).")
            elif isinstance(data, list):
                all_data.extend(data)
            else:
                all_data.append(data)

            # Check if there are more pages
            if "isLast" in data and data["isLast"]:
                break
            if "startAt" in data and "total" in data:
                start_at += max_results
                if start_at >= data["total"]:
                    break
            else:
                break

        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, output_file)
        with open(file_path, "w") as file:
            json.dump(all_data, file, indent=4)
        logging.info(f"Saved data from {endpoint} to {file_path}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from {endpoint}: {e}")

def fetch_users(output_file, output_dir="."):
    """
    Fetch all users from Jira and save them to a JSON file.
    """
    logging.info("Fetching users...")
    fetch_and_save("user/search?query=", output_file, output_dir)

def fetch_groups(output_file, output_dir="."):
    """
    Fetch all groups from Jira and save them to a JSON file.
    """
    logging.info("Fetching groups...")
    fetch_and_save("group", output_file, output_dir)

def fetch_projects(output_file, output_dir="."):
    """
    Fetch all projects from Jira and save them to a JSON file.
    """
    logging.info("Fetching projects...")
    fetch_and_save("project/search", output_file, output_dir)

def fetch_fields(output_file, output_dir="."):
    """
    Fetch all custom fields from Jira and save them to a JSON file.
    """
    logging.info("Fetching fields...")
    fetch_and_save("field", output_file, output_dir)

def fetch_dashboards(output_file, output_dir="."):
    """
    Fetch all dashboards from Jira and save them to a JSON file.
    """
    logging.info("Fetching dashboards...")
    fetch_and_save("dashboard/search", output_file, output_dir)

def fetch_filters(output_file, output_dir="."):
    """
    Fetch all filters from Jira and save them to a JSON file.
    """
    logging.info("Fetching filters...")
    fetch_and_save("filter/search", output_file, output_dir)

def fetch_system_info(output_file, output_dir="."):
    """
    Fetch Jira system information and save it to a JSON file.
    """
    logging.info("Fetching system information...")
    fetch_and_save("serverInfo", output_file, output_dir)

def fetch_issue_security_schemes(output_file, output_dir="."):
    """
    Fetch issue security schemes from Jira and save them to a JSON file.
    """
    logging.info("Fetching issue security schemes...")
    fetch_and_save("issuesecurityschemes", output_file, output_dir)

def fetch_issue_types(output_file, output_dir="."):
    """
    Fetch issue types from Jira and save them to a JSON file.
    """
    logging.info("Fetching issue types...")
    fetch_and_save("issuetype", output_file, output_dir)

def fetch_issue_type_schemes(output_file, output_dir="."):
    """
    Fetch issue type schemes from Jira and save them to a JSON file.
    """
    logging.info("Fetching issue type schemes...")
    fetch_and_save("issuetypescheme", output_file, output_dir)

def fetch_notification_schemes(output_file, output_dir="."):
    """
    Fetch notification schemes from Jira and save them to a JSON file.
    """
    logging.info("Fetching notification schemes...")
    fetch_and_save("notificationscheme", output_file, output_dir)

def fetch_permission_schemes(output_file, output_dir="."):
    """
    Fetch permission schemes from Jira and save them to a JSON file.
    """
    logging.info("Fetching permission schemes...")
    fetch_and_save("permissionscheme", output_file, output_dir)

def fetch_priorities(output_file, output_dir="."):
    """
    Fetch priorities from Jira and save them to a JSON file.
    """
    logging.info("Fetching priorities...")
    fetch_and_save("priority", output_file, output_dir)

def fetch_priority_schemes(output_file, output_dir="."):
    """
    Fetch priority schemes from Jira and save them to a JSON file.
    """
    logging.info("Fetching priority schemes...")
    fetch_and_save("priorityschemes", output_file, output_dir)

def fetch_screens(output_file, output_dir="."):
    """
    Fetch screens from Jira and save them to a JSON file.
    """
    logging.info("Fetching screens...")
    fetch_and_save("screens", output_file, output_dir)

def fetch_statuses(output_file, output_dir="."):
    """
    Fetch all statuses from Jira and save them to a JSON file.
    """
    logging.info("Fetching statuses...")
    fetch_and_save("status", output_file, output_dir)

def fetch_status_categories(output_file, output_dir="."):
    """
    Fetch all status categories from Jira and save them to a JSON file.
    """
    logging.info("Fetching status categories...")
    fetch_and_save("statuscategory", output_file, output_dir)

def fetch_project_categories(output_file, output_dir="."):
    """
    Fetch all project categories from Jira and save them to a JSON file.
    """
    logging.info("Fetching project categories...")
    fetch_and_save("projectCategory", output_file, output_dir)

def fetch_configuration(output_file, output_dir="."):
    """
    Fetch Jira configuration information and save it to a JSON file.
    """
    logging.info("Fetching Jira configuration...")
    fetch_and_save("configuration", output_file, output_dir)

def fetch_custom_fields(output_file, output_dir="."):
    """
    Fetch all custom fields from Jira and save them to a JSON file.
    """
    logging.info("Fetching custom fields...")
    fetch_and_save("customFields", output_file, output_dir)

def fetch_cluster_nodes(output_file, output_dir="."):
    """
    Fetch cluster nodes information from Jira and save it to a JSON file.
    """
    logging.info("Fetching cluster nodes...")
    fetch_and_save("cluster/nodes", output_file, output_dir)

if __name__ == "__main__":
    logging.info("Starting Jira data fetch script...")
    jira_fetch_and_save_all("jira_data")
    
    logging.info("Jira data fetch script completed.")
