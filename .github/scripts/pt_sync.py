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

response = requests.get(url, headers=headers)

# Report how many issues were found or the error if the request failed
if response.status_code >= 200 and response.status_code <= 299:
    print(f"Found {len(response.json()['items'])} issues to sync")
else:
    print(f"Failed to search for issues: {response.content}")
    exit(1)

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
        "description": f"Original issue: {url}\n\nOriginal description (may be out of date):\n{body}"
    }
    story_response = requests.post(story_url, headers=story_headers, json=story_data)
    # Verify status code is between 200 and 299 inclusive
    if story_response.status_code < 200 or story_response.status_code > 299:
        print(f"Failed to create story in Pivotal Tracker: {story_response}")
        exit(1)

    story_json = story_response.json()

    # Extract the Pivotal Tracker story ID and URL from the response
    pt_story_id = story_json["id"]
    pt_story_url = story_json["url"]

    # Add a comment to the source Github issue with the Pivotal Tracker story URL
    comment_url = f"https://api.github.com/repos/{GH_REPO}/issues/{issue_id}/comments"

    print("Comment URL: " + comment_url + "\n")

    comment_headers = {
        "Authorization": f"Bearer {GH_ACCESS_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    comment_data = {
        "body": f"Pivotal Tracker story: {pt_story_url}"
        }
    comment_response = requests.post(comment_url, headers=comment_headers, json=comment_data)
    if comment_response.status_code != 201:
        print(f"Failed to add comment to Github issue: {comment_response}")
        exit(1)
