import requests
import os

# Get the necessary parameters from environment variables
PT_API_KEY = os.environ['PT_API_KEY']
PT_PROJECT_ID = os.environ['PT_PROJECT_ID']
GH_REPO = os.environ['GITHUB_REPOSITORY']
GH_ACCESS_TOKEN = os.environ['GH_ACCESS_TOKEN']

# Search for issues with the "pt" tag on GitHub
url = f"https://api.github.com/search/issues?q=repo:{GH_REPO}+label:planned"
headers = {
    "Authorization": f"Bearer {GH_ACCESS_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28"
}

print("Get Github Issues")
print(f"URL: {url}\nHeaders: {headers}\n")

response = requests.get(url, headers=headers)

print(f"Retrieved issues from Github API\n")

# Iterate over the issues and synchronize them to Pivotal Tracker
for issue in response.json()["items"]:
    # Check if the issue has already been synced to Pivotal Tracker
    if "external_id" in issue:
        continue

    title = issue["title"]
    body = issue["body"]
    url = issue["html_url"]
    issue_id = issue["id"]

    # Create a new story in the Pivotal Tracker project
    story_url = f"https://www.pivotaltracker.com/services/v5/projects/{PT_PROJECT_ID}/stories"
    story_headers = {
        "X-TrackerToken": PT_API_KEY
    }
    story_data = {
        "name": title,
        "description": f"Original issue: {url}\n\n{body}",
        "external_id": issue_id
    }
    story_response = requests.post(story_url, headers=story_headers, json=story_data)

    print(f"Created story in Pivotal Tracker\n")
    print(f"Response: {story_response.content}\n")

    # Set the ID of the new story as the external ID of the original issue
    story_id = story_response.json()["id"]
    issue_url = f"https://api.github.com/repos/{GH_REPO}/issues/{issue['number']}"
    issue_headers = {
        "Authorization": f"Bearer {GH_ACCESS_TOKEN}"
    }
    issue_data = {
        "external_id": story_id
    }
    requests.patch(issue_url, headers=issue_headers, json=issue_data)
