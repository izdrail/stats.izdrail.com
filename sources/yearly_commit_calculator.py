from asyncio import sleep
from json import dumps
from re import search
from datetime import datetime
from typing import Dict, Tuple

from manager_download import DownloadManager as DM
from manager_environment import EnvironmentManager as EM
from manager_github import GitHubManager as GHM
from manager_file import FileManager as FM
from manager_debug import DebugManager as DBM


async def calculate_commit_data(repositories: Dict) -> Tuple[Dict, Dict]:
    """
    Calculate commit data by years.
    Commit data includes contribution additions and deletions in each quarter of each recorded year.

    :param repositories: user repositories info dictionary.
    :returns: Commit quarter yearly data dictionary.
    """
    DBM.i("Calculating commit data...")
    if EM.DEBUG_RUN:
        content = FM.cache_binary("commits_data.pick", assets=True)
        if content is not None:
            DBM.g("Commit data restored from cache!")
            return tuple(content)
        else:
            DBM.w("No cached commit data found, recalculating...")

    yearly_data = dict()
    date_data = dict()
    for ind, repo in enumerate(repositories):
        if repo["name"] not in EM.IGNORED_REPOS:
            repo_name = "[private]" if repo["isPrivate"] else f"{repo['owner']['login']}/{repo['name']}"
            DBM.i(f"\t{ind + 1}/{len(repositories)} Retrieving repo: {repo_name}")
            await update_data_with_commit_stats(repo, yearly_data, date_data)
    DBM.g("Commit data calculated!")

    if EM.DEBUG_RUN:
        FM.cache_binary("commits_data.pick", [yearly_data, date_data], assets=True)
        FM.write_file("commits_data.json", dumps([yearly_data, date_data]), assets=True)
        DBM.g("Commit data saved to cache!")
    return yearly_data, date_data



    """
    Updates yearly commit data with commits from given repository.
    Skips update if the commit isn't related to any repository.

    :param repo_details: Dictionary with information about the given repository.
    :param yearly_data: Yearly data dictionary to update.
    :param date_data: Commit date dictionary to update.
    """
    owner = repo_details["owner"]["login"]
    branch_data = await DM.get_remote_graphql("repo_branch_list", owner=owner, name=repo_details["name"])
    if len(branch_data) == 0:
        DBM.w("\t\tSkipping repo.")
        return

    for branch in branch_data:
        commit_data = await DM.get_remote_graphql("repo_commit_list", owner=owner, name=repo_details["name"], branch=branch["name"], id=GHM.USER.node_id, first=100, after=0)
        #print(f"Commit data for {branch['name']}...")
        #print(commit_data)
        #print("--------------------------------")
        for commit in commit_data:
            date = search(r"\d+-\d+-\d+", commit["committedDate"]).group()
            curr_year = datetime.fromisoformat(date).year
            quarter = (datetime.fromisoformat(date).month - 1) // 3 + 1

            if repo_details["name"] not in date_data:
                date_data[repo_details["name"]] = dict()
            if branch["name"] not in date_data[repo_details["name"]]:
                date_data[repo_details["name"]][branch["name"]] = dict()
            date_data[repo_details["name"]][branch["name"]][commit["oid"]] = commit["committedDate"]

            if repo_details["primaryLanguage"] is not None:
                if curr_year not in yearly_data:
                    yearly_data[curr_year] = dict()
                if quarter not in yearly_data[curr_year]:
                    yearly_data[curr_year][quarter] = dict()
                if repo_details["primaryLanguage"]["name"] not in yearly_data[curr_year][quarter]:
                    yearly_data[curr_year][quarter][repo_details["primaryLanguage"]["name"]] = {"add": 0, "del": 0}
                yearly_data[curr_year][quarter][repo_details["primaryLanguage"]["name"]]["add"] += commit["additions"]
                yearly_data[curr_year][quarter][repo_details["primaryLanguage"]["name"]]["del"] += commit["deletions"]

        if not EM.DEBUG_RUN:
            await sleep(0.4)
async def update_data_with_commit_stats(repo_details: Dict, yearly_data: Dict, date_data: Dict):
    owner = repo_details["owner"]["login"]
    branch_data = await DM.get_remote_graphql("repo_branch_list", owner=owner, name=repo_details["name"])
    if len(branch_data) == 0:
        DBM.w("\t\tSkipping repo.")
        return

    for branch in branch_data:
        commit_data = await DM.get_remote_graphql(
            "repo_commit_list",
            owner=owner,
            name=repo_details["name"],
            branch=branch["name"],
            id=GHM.USER.node_id
        )

        #print(f"\t\tFetching commit stats for branch '{branch['name']}' in repo '{repo_details['name']}'...")
        #print(f"\t\t\t{len(commit_data)} commits found.")

        #print(commit_data)
        
        if not isinstance(commit_data, list):
            #print(f"\t\tCommit fetch failed for branch '{branch['name']}' in repo '{repo_details['name']}'!")
            #print(f"\t\tError: {commit_data}")
            continue  # Skip this branch

        for commit in commit_data:
            date_match = search(r"\d+-\d+-\d+", commit.get("committedDate", ""))
            if not date_match:
                continue

            date = date_match.group()
            curr_year = datetime.fromisoformat(date).year
            quarter = (datetime.fromisoformat(date).month - 1) // 3 + 1

            if repo_details["name"] not in date_data:
                date_data[repo_details["name"]] = dict()
            if branch["name"] not in date_data[repo_details["name"]]:
                date_data[repo_details["name"]][branch["name"]] = dict()
            date_data[repo_details["name"]][branch["name"]][commit["oid"]] = commit["committedDate"]

            if repo_details["primaryLanguage"] is not None:
                lang = repo_details["primaryLanguage"]["name"]
                yearly_data.setdefault(curr_year, {}).setdefault(quarter, {}).setdefault(lang, {"add": 0, "del": 0})
                yearly_data[curr_year][quarter][lang]["add"] += commit["additions"]
                yearly_data[curr_year][quarter][lang]["del"] += commit["deletions"]

        if not EM.DEBUG_RUN:
            await sleep(0.4)