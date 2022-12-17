import os
import requests

# Get the necessary parameters from environment variables
PT_API_KEY = os.environ['PT_API_KEY']
PT_PROJECT_ID = os.environ['PT_PROJECT_ID']
GH_OWNER = os.environ['GH_OWNER']
GH_REPO = os.environ['GH_REPO']
GH_ACCESS_TOKEN = os.environ['GH_ACCESS_TOKEN']

def sync_issues():
  # Fetch the current list of issues from Pivotal Tracker
  pt_url = f'https://www.pivotaltracker.com/services/v5/projects/{PT_PROJECT_ID}/stories'
  pt_headers = {'X-TrackerToken': PT_API_KEY}
  pt_response = requests.get(pt_url, headers=pt_headers)
  pt_issues = pt_response.json()

  # Fetch the current list of issues from GitHub
  gh_url = f'https://api.github.com/repos/{GH_OWNER}/{GH_REPO}/issues'
  gh_headers = {'Authorization': f'token {GH_ACCESS_TOKEN}'}
  gh_response = requests.get(gh_url, headers=gh_headers)
  gh_issues = gh_response.json()

  # Iterate over the issues in Pivotal Tracker and see if they exist in GitHub
  for pt_issue in pt_issues:
    # Check if the issue exists in GitHub
    gh_matching_issues = [i for i in gh_issues if i['external_id'] == pt_issue['id']]
    if not gh_matching_issues:
      # If the issue doesn't exist in GitHub, create it
      gh_data = {
        'title': pt_issue['name'],
        'body': pt_issue['description'],
        'state': 'open' if pt_issue['current_state'] == 'unstarted' else pt_issue['current_state'],
        'external_id': pt_issue['id']
      }
      gh_response = requests.post(gh_url, json=gh_data, headers=gh_headers)

  # Iterate over the issues in GitHub and see if they exist in Pivotal Tracker
  for gh_issue in gh_issues:
    # Check if the issue exists in Pivotal Tracker
    pt_matching_issues = [i for i in pt_issues if i['external_id'] == gh_issue['id']]
    if not pt_matching_issues:
      # If the issue doesn't exist in Pivotal Tracker, create it
      pt_data = {
        'name': gh_issue['title'],
        'description': gh_issue['body'],
        'current_state': 'unstarted' if gh_issue['state'] == 'open' else gh_issue['state'],
        'external_id': gh_issue['id']
      }
      pt_response = requests.post(pt_url, json=pt_data, headers=pt_headers)
