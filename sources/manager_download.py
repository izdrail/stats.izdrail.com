from asyncio import Task
from hashlib import md5
from json import dumps
from string import Template
from typing import Awaitable, Dict, Callable, Optional, List, Tuple

from httpx import AsyncClient
from yaml import safe_load

from manager_environment import EnvironmentManager as EM
from manager_debug import DebugManager as DBM


GITHUB_API_QUERIES = {
    # Query to collect info about all user repositories, including: is it a fork, name and owner login.
    # NB! Query includes information about recent repositories only (apparently, contributed within a year).
    "repos_contributed_to": """
{
    user(login: "$username") {
        repositoriesContributedTo(orderBy: {field: CREATED_AT, direction: DESC}, $pagination, includeUserRepositories: true) {
            nodes {
                primaryLanguage {
                    name
                }
                name
                owner {
                    login
                }
                isPrivate
                isFork
            }
            pageInfo {
                endCursor
                hasNextPage
            }
        }
    }
}""",
    # Query to collect info about all repositories user created or collaborated on, including: name, primary language and owner login.
    # NB! Query doesn't include information about repositories user contributed to via pull requests.
    "user_repository_list": """
{
    user(login: "$username") {
        repositories(orderBy: {field: CREATED_AT, direction: DESC}, $pagination, affiliations: [OWNER, COLLABORATOR], isFork: false) {
            nodes {
                primaryLanguage {
                    name
                }
                name
                owner {
                    login
                }
                isPrivate
            }
            pageInfo {
                endCursor
                hasNextPage
            }
        }
    }
}
""",
    # Query to collect info about branches in the given repository, including: names.
    "repo_branch_list": """
{
    repository(owner: "$owner", name: "$name") {
        refs(refPrefix: "refs/heads/", orderBy: {direction: DESC, field: TAG_COMMIT_DATE}, $pagination) {
            nodes {
                name
            }
            pageInfo {
                endCursor
                hasNextPage
            }
        }
    }
}
""",
    # Query to collect info about user commits to given repository, including: commit date, additions and deletions numbers.
    "repo_commit_list": """
{
    repository(owner: "$owner", name: "$name") {
        ref(qualifiedName: "refs/heads/$branch") {
            target {
                ... on Commit {
                    history(author: { id: "$id" }, $pagination) {
                        nodes {
                            ... on Commit {
                                additions
                                deletions
                                committedDate
                                oid
                            }
                        }
                        pageInfo {
                            endCursor
                            hasNextPage
                        }
                    }
                }
            }
        }
    }
}
""",
    # Query to hide outdated PR comment.
    "hide_outdated_comment": """
mutation {
    minimizeComment(input: {classifier: OUTDATED, subjectId: "$id"}) {
        clientMutationId
    }
}
""",
}


async def init_download_manager(user_login: str):
    await DownloadManager.load_remote_resources(
        linguist="https://cdn.jsdelivr.net/gh/github/linguist@master/lib/linguist/languages.yml",
        waka_latest=f"https://wakatime.com/api/v1/users/current/stats/last_7_days?api_key={EM.WAKATIME_API_KEY}",
        waka_all=f"https://wakatime.com/api/v1/users/current/all_time_since_today?api_key={EM.WAKATIME_API_KEY}",
        github_stats=f"https://github-contributions.vercel.app/api/v1/{user_login}",
    )


class DownloadManager:
    _client = AsyncClient(timeout=60.0)
    _REMOTE_RESOURCES_CACHE = {}

    @staticmethod
    async def load_remote_resources(**resources: str):
        for resource, url in resources.items():
            DownloadManager._REMOTE_RESOURCES_CACHE[resource] = await DownloadManager._client.get(url)

    @staticmethod
    async def close_remote_resources():
        for resource in DownloadManager._REMOTE_RESOURCES_CACHE.values():
            if isinstance(resource, Task):
                resource.cancel()
            elif isinstance(resource, Awaitable):
                await resource

    @staticmethod
    async def _get_remote_resource(resource: str, convertor: Optional[Callable[[bytes], Dict]]) -> Optional[Dict]:
        DBM.i(f"\tMaking a remote API query named '{resource}'...")
        res = DownloadManager._REMOTE_RESOURCES_CACHE[resource]

        DBM.g(f"\tQuery '{resource}' {'finished' if isinstance(res, Task) else 'loaded from cache'}!")

        if res.status_code == 200:
            return convertor(res.content) if convertor else res.json()
        elif res.status_code in (201, 202):
            DBM.w(f"\tQuery '{resource}' returned status code {res.status_code}")
            return None
        else:
            raise Exception(f"Query '{res.url}' failed with code {res.status_code}: {res.text}")

    @staticmethod
    async def get_remote_json(resource: str) -> Optional[Dict]:
        return await DownloadManager._get_remote_resource(resource, None)

    @staticmethod
    async def get_remote_yaml(resource: str) -> Optional[Dict]:
        return await DownloadManager._get_remote_resource(resource, safe_load)

    @staticmethod
    async def _fetch_graphql_query(query: str, retries_count: int = 10, **kwargs) -> Dict:
        headers = {"Authorization": f"Bearer {EM.GH_TOKEN}"}
        #print(query)
        res = await DownloadManager._client.post(
            "https://api.github.com/graphql",
            json={"query": Template(GITHUB_API_QUERIES[query]).substitute(kwargs)},
            headers=headers,
        )
        if res.status_code == 200:
            return res.json()
        elif res.status_code == 502 and retries_count > 0:
            return await DownloadManager._fetch_graphql_query(query, retries_count - 1, **kwargs)
        else:
            raise Exception(f"Query '{query}' failed with code {res.status_code}: {res.text}")

    @staticmethod
    def _find_pagination_and_data_list(response: Dict) -> Tuple[List, Dict]:
        if "nodes" in response and "pageInfo" in response:
            return response["nodes"], response["pageInfo"]
        elif len(response) == 1 and isinstance(list(response.values())[0], dict):
            return DownloadManager._find_pagination_and_data_list(list(response.values())[0])
        else:
            return [], {"hasNextPage": False}

    @staticmethod
    async def _fetch_graphql_paginated(query: str, **kwargs) -> List[Dict]:
        #(query)
        kwargs["first"] = 100  # GitHub's hard limit
        initial = await DownloadManager._fetch_graphql_query(query, **kwargs, pagination='first: 100')
        nodes, page_info = DownloadManager._find_pagination_and_data_list(initial)

        while page_info.get("hasNextPage"):
            after = page_info.get("endCursor")
            paginated = await DownloadManager._fetch_graphql_query(query, **kwargs, pagination=f'first: 100, after: "{after}"')
            new_nodes, page_info = DownloadManager._find_pagination_and_data_list(paginated)
            nodes.extend(new_nodes)

        return nodes

    @staticmethod
    async def get_remote_graphql(query: str, **kwargs) -> Dict:
        key = f"{query}_{md5(dumps(kwargs, sort_keys=True).encode('utf-8')).hexdigest()}"
        if key not in DownloadManager._REMOTE_RESOURCES_CACHE:
            if "$pagination" in GITHUB_API_QUERIES[query]:
                result = await DownloadManager._fetch_graphql_paginated(query, **kwargs)
                #print(f"Fetched {len(result)} results from {query}!")
            else:
                result = await DownloadManager._fetch_graphql_query(query, **kwargs)
            DownloadManager._REMOTE_RESOURCES_CACHE[key] = result
        return DownloadManager._REMOTE_RESOURCES_CACHE[key]
