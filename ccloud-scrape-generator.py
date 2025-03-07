import os
import requests
import json
import pathlib
import yaml  # Add PyYAML import
from collections import defaultdict
from jinja2 import Environment, FileSystemLoader

# Load API credentials from environment variables
CLOUD_API_KEY = os.getenv("CLOUD_API_KEY")
CLOUD_API_SECRET = os.getenv("CLOUD_API_SECRET")

if not CLOUD_API_KEY or not CLOUD_API_SECRET:
    print("❌ Error: CLOUD_API_KEY or CLOUD_API_SECRET is not set in environment variables.")
    exit(1)

# Confluent Cloud API Base URLs
BASE_URL = "https://api.confluent.cloud"
TELEMETRY_URL = "https://api.telemetry.confluent.cloud"

# Headers for authentication
HEADERS = {"Accept": "application/json"}

# Create a session with authentication
session = requests.Session()
session.auth = (CLOUD_API_KEY, CLOUD_API_SECRET)

# Directory for service discovery files
SD_DIR = "target_configs"


# Existing functions remain unchanged
def fetch_environments():
    """Fetch all environments from Confluent Cloud."""
    # [existing code]
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
    # [existing code]
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


def get_resource_types():
    """Discover all available resource types that can be monitored from Telemetry API."""
    # [existing code]
    url = f"{TELEMETRY_URL}/v2/metrics/cloud/descriptors/resources"
    response = session.get(url, headers=HEADERS)

    if response.status_code != 200:
        print(f"❌ Error discovering resource types: {response.text}")
        exit(1)

    data = response.json()
    resources_data = data.get("data", [])
    
    # Extract resource types and their metadata from the response
    resource_types = {}
    for resource in resources_data:
        if isinstance(resource, dict) and 'type' in resource:
            resource_type = resource["type"]
            resource_types[resource_type] = {
                "description": resource.get("description", ""),
                "labels": resource.get("labels", [])
            }
            
            # Find the ID label key for each resource type
            for label in resource.get("labels", []):
                if "id" in label.get("key", "").lower() and label.get("exportable", False):
                    resource_types[resource_type]["id_label"] = label.get("key")
                    break
    
    return resource_types


def fetch_resource_ids(resource_type, resource_metadata):
    """Fetch all resource IDs from Confluent Cloud based on resource type."""
    resources = []
    
    # Get resource IDs based on resource type
    if resource_type == "kafka":
        print(f"  🔍 Fetching {resource_type} resources via Confluent Cloud API...")
        for env_id, env_name in environments.items():
            clusters = fetch_kafka_clusters(env_id)
            for cluster in clusters:
                resources.append({
                    "id": cluster["id"],
                    "name": cluster["spec"]["display_name"],
                    "kind": cluster["spec"]["config"]["kind"],
                    "cloud": cluster["spec"]["cloud"],
                    "region": cluster["spec"]["region"],
                    "environment": env_id,
                    "environment_name": env_name
                })
    
    elif resource_type == "schema_registry":
        print(f"  🔍 Fetching {resource_type} resources via Confluent Cloud API...")
        # Use v3 endpoint as v2 is deprecated per API spec
        for env_id, env_name in environments.items():
            url = f"{BASE_URL}/srcm/v3/clusters?environment={env_id}"
            response = session.get(url, headers=HEADERS)
            
            if response.status_code != 200:
                print(f"  ⚠️ Error fetching Schema Registry in environment {env_name}: {response.status_code}")
                continue
                
            try:
                sr_clusters = response.json().get("data", [])
                for sr in sr_clusters:
                    resources.append({
                        "id": sr["id"],
                        "name": sr.get("display_name", sr["id"]),
                        "environment": env_id,
                        "environment_name": env_name,
                        "cloud": sr.get("spec", {}).get("cloud"),
                        "region": sr.get("spec", {}).get("region")
                    })
            except Exception as e:
                print(f"  ⚠️ Error parsing Schema Registry response: {e}")
    
    elif resource_type == "ksql":
        print(f"  🔍 Fetching {resource_type} resources via Confluent Cloud API...")
        for env_id, env_name in environments.items():
            url = f"{BASE_URL}/ksqldbcm/v2/clusters?environment={env_id}"
            response = session.get(url, headers=HEADERS)
            
            if response.status_code != 200:
                print(f"  ⚠️ Error fetching KSQL clusters in environment {env_name}: {response.status_code}")
                continue
                
            try:
                # Debug the response
                resp_data = response.json()
                if not isinstance(resp_data, dict):
                    print(f"  ⚠️ KSQL response is not a dictionary: {type(resp_data)}")
                    continue

                # Get the data array or empty list if not present
                ksql_clusters = resp_data.get("data")
                if not isinstance(ksql_clusters, list):
                    print(f"  ⚠️ KSQL clusters data is not a list: {type(ksql_clusters)}")
                    continue
                
                for ksql in ksql_clusters:
                    if not isinstance(ksql, dict):
                        continue
                    
                    # Use defensive dictionary access with defaults
                    resource_data = {
                        "id": ksql.get("id", f"unknown-{len(resources)}"),
                        "name": "Unknown KSQL Cluster",
                        "environment": env_id,
                        "environment_name": env_name
                    }
                
                    # Only add optional fields if they exist
                    if "spec" in ksql and isinstance(ksql["spec"], dict):
                        spec = ksql["spec"]
                        if "display_name" in spec:
                            resource_data["name"] = spec["display_name"]
                        if "cloud" in spec:
                            resource_data["cloud"] = spec["cloud"]
                        if "region" in spec:
                            resource_data["region"] = spec["region"]
                        if "kafka_cluster" in spec and isinstance(spec["kafka_cluster"], dict):
                            resource_data["kafka_cluster"] = spec["kafka_cluster"].get("id")
                
                    resources.append(resource_data)
            except Exception as e:
                print(f"  ⚠️ Error parsing KSQL response: {e}")
                # Print more debug info
                print(f"  Response content: {response.text[:200]}...")
    
    elif resource_type == "connector":
        print(f"  🔍 Fetching {resource_type} resources via Confluent Cloud API...")
        # Connectors are tied to Kafka clusters, so we first need to get all Kafka clusters
        for env_id, env_name in environments.items():
            kafka_clusters = fetch_kafka_clusters(env_id)
            for cluster in kafka_clusters:
                kafka_id = cluster["id"]
                # From API spec: /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors
                url = f"{BASE_URL}/connect/v1/environments/{env_id}/clusters/{kafka_id}/connectors"
                response = session.get(url, headers=HEADERS)
                
                if response.status_code != 200:
                    print(f"  ⚠️ Error fetching connectors for cluster {kafka_id}: {response.status_code}")
                    continue
                
                try:
                    connectors = response.json()
                    if isinstance(connectors, list):
                        for connector_name in connectors:
                            # For each connector name, get details
                            detail_url = f"{BASE_URL}/connect/v1/environments/{env_id}/clusters/{kafka_id}/connectors/{connector_name}"
                            detail_resp = session.get(detail_url, headers=HEADERS)
                            
                            if detail_resp.status_code != 200:
                                print(f"  ⚠️ Error fetching connector details for {connector_name}: {detail_resp.status_code}")
                                continue
                                
                            connector = detail_resp.json()
                            resources.append({
                                "id": connector_name,  # Connector ID is its name
                                "name": connector_name,
                                "environment": env_id,
                                "environment_name": env_name,
                                "cluster_id": kafka_id,
                                "cluster_name": cluster["spec"]["display_name"]
                            })
                except Exception as e:
                    print(f"  ⚠️ Error parsing connector response: {e}")
    
    elif resource_type == "flink":
        print(f"  🔍 Fetching Flink compute pools...")
        for env_id, env_name in environments.items():
            # First fetch compute pools
            url = f"{BASE_URL}/fcpm/v2/compute-pools?environment={env_id}"
            response = session.get(url, headers=HEADERS)
            
            if response.status_code != 200:
                print(f"  ⚠️ Error fetching Flink compute pools in {env_name}: {response.status_code}")
            else:
                try:
                    compute_pools = response.json().get("data", [])
                    for pool in compute_pools:
                        resources.append({
                            "id": pool["id"],
                            "name": pool.get("spec", {}).get("display_name", pool["id"]),
                            "environment": env_id,
                            "environment_name": env_name,
                            "cloud": pool.get("spec", {}).get("cloud"),
                            "region": pool.get("spec", {}).get("region")
                        })
                except Exception as e:
                    print(f"  ⚠️ Error parsing compute pool response: {e}")
        
        # Now fetch SQL statements if needed
        print(f"  🔍 Fetching Flink SQL statements...")
        org_id = None  # We need the org ID for SQL statements
        
        # Get org ID from environment (first environment is fine)
        if environments:
            first_env_id = next(iter(environments))
            url = f"{BASE_URL}/org/v2/environments/{first_env_id}"
            response = session.get(url, headers=HEADERS)
            if response.status_code == 200:
                org_id = response.json().get("org_id")
        
        if org_id:
            for env_id, env_name in environments.items():
                url = f"{BASE_URL}/sql/v1/organizations/{org_id}/environments/{env_id}/statements"
                response = session.get(url, headers=HEADERS)
                
                if response.status_code != 200:
                    print(f"  ⚠️ Error fetching Flink statements in {env_name}: {response.status_code}")
                    continue
                    
                try:
                    statements = response.json()
                    if isinstance(statements, list):
                        for stmt in statements:
                            resources.append({
                                "id": stmt["name"],  # Statement name is its ID
                                "name": stmt["name"],
                                "environment": env_id,
                                "environment_name": env_name
                            })
                except Exception as e:
                    print(f"  ⚠️ Error parsing Flink statements response: {e}")
    else:
        print(f"  ⚠️ Resource type {resource_type} not supported")
        
    if resources:
        print(f"  ✅ Found {len(resources)} {resource_type} resources")
    else:
        print(f"  ⚠️ No {resource_type} resources found")
        
    return resources


# New function to standardize labels across different resource types
def standardize_labels(resources, resource_type):
    """Apply common label standardization across different resource types."""
    cloud_map = {
        "AWS": "aws",
        "GCP": "gcp",
        "AZURE": "azure"
    }
    env_map = {
        "prod": "production",
        "prd": "production",
        "production": "production",
        "stg": "staging",
        "staging": "staging",
        "stage": "staging",
        "dev": "development",
        "development": "development",
        "test": "test",
        "tst": "test",
        "qa": "test"
    }

    for resource in resources:
        resource["component_type"] = resource_type
        if resource_type == "kafka" and "kind" in resource:
            resource["service_tier"] = resource["kind"]
        if "cloud" in resource and resource["cloud"] is not None:
            cloud = resource["cloud"].upper()
            resource["cloud_provider"] = cloud_map.get(cloud, cloud.lower())
        env_name = resource.get("environment_name", "").lower()
        resource["environment_type"] = next((env_map[k] for k in env_map if k in env_name), "other")
    return resources


# New function to generate service discovery files
def generate_sd_files(resource_groups):
    """Generate service discovery files for Prometheus to load dynamically."""
    # Create directory if it doesn't exist
    pathlib.Path(SD_DIR).mkdir(parents=True, exist_ok=True)
    
    # Store existing files before generating new ones
    existing_files = set()
    if os.path.exists(SD_DIR):
        existing_files = {os.path.join(SD_DIR, f) for f in os.listdir(SD_DIR) if f.endswith('.yml')}
    
    # Track newly created files
    new_files = set()
    
    # Generate files for each resource type
    for resource_type, resources in resource_groups.items():
        if not resources:
            continue
            
        # Group by environment for better organization
        env_resources = defaultdict(list)
        for resource in resources:
            # Skip resources without valid telemetry IDs
            if resource.get('no_telemetry_id'):
                continue
                
            env_name = resource.get("environment_name", "unknown")
            env_resources[env_name].append(resource)
        
        # Create a file for each environment
        for env_name, env_data in env_resources.items():
            if not env_data:
                continue
                
            # Create sanitized environment name for filename
            safe_env = env_name.lower().replace(" ", "_").replace("-", "_")
            
            # Group resources by cloud provider
            cloud_resources = defaultdict(list)
            for resource in env_data:
                cloud = resource.get("cloud_provider", "unknown")
                cloud_resources[cloud].append(resource)
                
            # Create separate files for each cloud provider
            for cloud, cloud_data in cloud_resources.items():
                # Get resource IDs
                resource_ids = [r["id"] for r in cloud_data]
                
                # Create the service discovery file
                sd_config = [{
                    "targets": ["api.telemetry.confluent.cloud"],
                    "labels": {
                        "job": f"confluent_{resource_type}",
                        "environment": env_name,
                        "cloud_provider": cloud,
                        "environment_type": cloud_data[0].get("environment_type", "other")
                    },
                    "params": {
                        f"resource.{resource_type}.id": resource_ids
                    }
                }]
                
                # Write to file using YAML instead of JSON
                filename = f"{SD_DIR}/{resource_type}_{safe_env}_{cloud}.yml"
                full_path = os.path.abspath(filename)
                new_files.add(full_path)
                
                with open(filename, "w") as f:
                    yaml.safe_dump(sd_config, f, default_flow_style=False)
                    
                print(f"  ✅ Generated service discovery file: {filename}")
    
    # Clean up old files that are no longer needed
    files_to_remove = existing_files - {os.path.abspath(f) for f in new_files}
    for old_file in files_to_remove:
        try:
            os.remove(old_file)
            print(f"  🧹 Removed outdated service discovery file: {old_file}")
        except Exception as e:
            print(f"  ⚠️ Error removing file {old_file}: {e}")
    
    # Generate a list of all discovery files for the main Prometheus config
    return [f for f in os.listdir(SD_DIR) if f.endswith(".yml")]


# --- Existing script starts here ---

# Fetch all environments
environments = fetch_environments()

# Discover available resource types from Telemetry API
print("🔍 Discovering available resource types from Telemetry API...")
resource_types_metadata = get_resource_types()
print(f"✅ Found {len(resource_types_metadata)} resource types: {', '.join(resource_types_metadata.keys())}")

# Initialize resource groups dictionary to store resources by type
resource_groups = {}

# Process each resource type
print("\n📊 Resource Collection:")
for resource_type, metadata in resource_types_metadata.items():
    print(f"🔍 Processing {resource_type} resources ({metadata['description']})...")
    resources = fetch_resource_ids(resource_type, metadata)
    
    # Apply standardized labels to resources
    resources = standardize_labels(resources, resource_type)
    
    resource_groups[resource_type] = resources

# Build backward-compatible cluster groups structure
# This is for template compatibility with the existing prometheus_template.yml.j2
cluster_groups = defaultdict(list)
if "kafka" in resource_groups:
    for cluster in resource_groups["kafka"]:
        env_name = cluster.get("environment_name", "Unknown")
        cluster_groups[env_name].append({
            "id": cluster["id"],
            "name": cluster["name"],
            "kind": cluster.get("kind", "Unknown"),
            "cloud": cluster.get("cloud", "Unknown"),
            "region": cluster.get("region", "Unknown"),
        })

# Print summary
print("\n📊 Resource Summary:")
for resource_type, resources in resource_groups.items():
    print(f"{resource_type}: {len(resources)} resources")
    
    # Debug: check if there are any real connector IDs (for telemetry)
    if resource_type == "connector":
        valid_telemetry_ids = [r for r in resources if not r.get('no_telemetry_id', False)]
        print(f"  • Valid connector IDs for telemetry: {len(valid_telemetry_ids)}")
        if valid_telemetry_ids:
            # Print a sample of the first one
            print(f"  • Sample connector with telemetry ID: {valid_telemetry_ids[0]['name']} - {valid_telemetry_ids[0]['id']}")

# Print Kafka clusters summary for backward compatibility
total_clusters = sum(len(clusters) for clusters in cluster_groups.values())
print(f"\n📊 Environment and Cluster Summary:")
print(f"Total Environments: {len(cluster_groups)}")
print(f"Total Clusters: {total_clusters}\n")

for env_name, clusters in cluster_groups.items():
    print(f"Environment: {env_name}")
    if clusters:
        for cluster in clusters:
            print(f"  • {cluster['name']}")
    print()

# Generate service discovery files
print("\n📦 Generating service discovery files...")
sd_files = generate_sd_files(resource_groups)

# Instead of generating a full Prometheus config, just create an example file
# if it doesn't already exist
example_config_path = "prometheus_example.yml"
if not os.path.exists(example_config_path):
    example_config = """global:
  scrape_timeout: 1m

scrape_configs:
  # Dynamic file-based service discovery for Confluent resources
  - job_name: "confluent_cloud_resources"
    file_sd_configs:
      - files:
        - "target_configs/*.yml"
        refresh_interval: 5m
    scheme: https
    basic_auth:
      username: "YOUR_CONFLUENT_API_KEY"
      password: "YOUR_CONFLUENT_API_SECRET"
    metrics_path: /v2/metrics/cloud/export
    honor_timestamps: true
    
    # Apply common relabeling for better usability
    metric_relabel_configs:
      # Extract resource ID into a uniform label
      - source_labels: ["kafka_id", "connector_id", "ksql_id", "schema_registry_id", "compute_pool_id", "flink_statement_id"]
        regex: "(.+)"
        action: replace
        target_label: "resource_id"
        replacement: "$1"
        
      # Rename some metric labels for better consistency
      - source_labels: ["kind"]
        regex: "(Basic|Standard|Dedicated)"
        target_label: "service_tier"
        replacement: "$1"
"""
    with open(example_config_path, "w") as file:
        file.write(example_config)
    print(f"✅ Created example Prometheus config: {example_config_path}")

print("✅ Service discovery files generated in: {SD_DIR}/")
print("🚀 Done! Use the service discovery files in your Prometheus config.")
print("   See prometheus_example.yml for reference configuration.")
print("\n💡 Tips:")
print("   1. Configure your Prometheus to watch the target_configs/ directory")
print("   2. Set your Confluent Cloud API credentials in the Prometheus config")
print("   3. Run this script regularly to keep service discovery files updated")