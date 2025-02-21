import os
import requests
import json
from collections import defaultdict
from jinja2 import Environment, FileSystemLoader

# Load API credentials from environment variables
CLOUD_API_KEY = os.getenv("CLOUD_API_KEY")
CLOUD_API_SECRET = os.getenv("CLOUD_API_SECRET")

if not CLOUD_API_KEY or not CLOUD_API_SECRET:
    print("❌ Error: CLOUD_API_KEY or CLOUD_API_SECRET is not set in environment variables.")
    exit(1)

# Confluent Cloud API Base URL
BASE_URL = "https://api.confluent.cloud"

# Headers for authentication
HEADERS = {"Accept": "application/json"}

# Create a session with authentication
session = requests.Session()
session.auth = (CLOUD_API_KEY, CLOUD_API_SECRET)


def fetch_environments():
    """Fetch all environments from Confluent Cloud."""
    url = f"{BASE_URL}/org/v2/environments"
    environments = []
    next_url = url

    while True:
        response = session.get(next_url, headers=HEADERS)

        if response.status_code != 200:
            print(f"❌ Error fetching environments: {response.text}")
            exit(1)

        data = response.json()
        environments.extend(data.get("data", []))

        # Check if there's a next URL in metadata
        metadata = data.get("metadata", {})
        next_url = metadata.get("next")

        if not next_url:
            break

    return {env["id"]: env["display_name"] for env in environments}


def fetch_kafka_clusters(environment_id):
    """Fetch Kafka clusters for a given environment."""
    url = f"{BASE_URL}/cmk/v2/clusters?environment={environment_id}"
    response = session.get(url, headers=HEADERS)

    if response.status_code != 200:
        print(f"❌ Error fetching clusters for environment {environment_id}: {response.text}")
        return []  # Return empty list instead of implicit None

    data = response.json()

    if not data.get("data"):
        print(f"⚠️ No data returned for environment {environment_id}")
        return []

    return data.get("data", [])  # Ensure we always return a list


# Fetch all environments
environments = fetch_environments()

# Fetch clusters and group them by environment
cluster_groups = defaultdict(list)
for env_id, env_name in environments.items():
    clusters = fetch_kafka_clusters(env_id)
    for cluster in clusters:
        cluster_groups[env_name].append({
            "id": cluster["id"],
            "name": cluster["spec"]["display_name"],
            "kind": cluster["spec"]["config"]["kind"],
            "cloud": cluster["spec"]["cloud"],
            "region": cluster["spec"]["region"],
        })
# Print summary
total_clusters = sum(len(clusters) for clusters in cluster_groups.values())
print("\n📊 Environment and Cluster Summary:")
print(f"Total Environments: {len(cluster_groups)}")
print(f"Total Clusters: {total_clusters}\n")

for env_name, clusters in cluster_groups.items():
    print(f"Environment: {env_name}")
    if clusters:
        for cluster in clusters:
            print(f"  • {cluster['name']}")
    print()

# Load Jinja2 template
env = Environment(loader=FileSystemLoader("."))
template = env.get_template("prometheus_template.yml.j2")

# Render template
prometheus_config = template.render(
    cluster_groups=cluster_groups,
    cloud_api_key=CLOUD_API_KEY,
    cloud_api_secret=CLOUD_API_SECRET
)

# Save to prometheus.yml
with open("prometheus.yml", "w") as file:
    file.write(prometheus_config)

print("✅ Prometheus config generated: prometheus.yml")