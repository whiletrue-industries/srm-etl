import os
import re
import requests
from srm_tools.logger import logger
from srm_tools.error_notifier import invoke_on


def configs():
    GITHUB_TOKEN = os.getenv("KZ_KOLSHERUT_APPLICATION_GITHUB_TOKEN")
    REPO_OWNER = "kolzchut"
    REPO_NAME = "Kolsherut-Application"
    WORKFLOW_FILENAME = "fe-release.yml"

    API_BASE = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }
    return WORKFLOW_FILENAME, API_BASE, headers


def get_latest_release_info(api_base, headers):
    try:
        response = requests.get(f"{api_base}/tags?per_page=100", headers=headers, timeout=30)
        response.raise_for_status()
        tags = response.json()
    except requests.RequestException as e:
        logger.error(f"❌ Error fetching tags: {e}")
        return None, None

    latest_version = (0, 0, 0)
    target_sha = None

    version_pattern = re.compile(r"^v?(\d+)\.(\d+)\.(\d+)")

    for tag in tags:
        match = version_pattern.match(tag.get("name", ""))
        if match:
            current_version = tuple(map(int, match.groups()))
            if current_version > latest_version:
                latest_version = current_version
                target_sha = tag.get("commit", {}).get("sha")

    return latest_version, target_sha


def run(*_):
    logger.info("Starting FE Auto-Release Trigger")
    WORKFLOW_FILENAME, API_BASE, headers = configs()

    # 1. Get SHA of the LAST TAG
    current_ver, target_sha = get_latest_release_info(API_BASE, headers)

    if not target_sha:
        logger.error("❌ No previous tags found. Cannot determine SHA.")
        return

    # 2. Calculate New Version
    new_ver_string = f"v{current_ver[0]}.{current_ver[1]}.{current_ver[2] + 1}-auto"

    logger.info(f"Target SHA: {target_sha}")
    logger.info(f"New Version: {new_ver_string}")

    # 3. Trigger Workflow
    payload = {
        "ref": "main",  # Triggers the pipeline DEFINITION from main
        "inputs": {
            "manual_tag_name": new_ver_string,
            "commit_ref": target_sha  # This SHA will override 'main' in the checkout step
        }
    }

    try:
        url = f"{API_BASE}/actions/workflows/{WORKFLOW_FILENAME}/dispatches"
        r = requests.post(url, json=payload, headers=headers, timeout=30)

        if r.status_code == 204:
            logger.info(f"✅ Triggered '{WORKFLOW_FILENAME}' successfully!")
        else:
            logger.error(f"❌ Error {r.status_code}: {r.text}")

    except Exception as e:
        logger.error(f"❌ Request Failed: {e}")


def operator(*_):
    invoke_on(run, 'SSG Upadater Operator')


if __name__ == '__main__':
    run()
