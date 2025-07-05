import requests
from django.conf import settings
import logging # Added for logging

logger = logging.getLogger(__name__) # Get a logger instance for this module

class ConfluentCloudService:
    def __init__(self):
        self.api_key = settings.CONFLUENT_CLOUD_API_KEY
        self.api_secret = settings.CONFLUENT_CLOUD_API_SECRET
        self.base_url = settings.CONFLUENT_CLOUD_API_BASE_URL

        if not self.api_key or not self.api_secret:
            logger.warning("CONFLUENT_CLOUD_API_KEY or CONFLUENT_CLOUD_API_SECRET is not set in Django settings. "
                           "Confluent Cloud API calls will likely fail.")

    def _request(self, method, endpoint, **kwargs):
        url = f"{self.base_url}{endpoint}"
        auth = (self.api_key, self.api_secret)

        try:
            response = requests.request(method, url, auth=auth, **kwargs)
            response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)
            return response.json()
        except requests.exceptions.HTTPError as e:
            # Log error or handle specific status codes
            logger.error(f"HTTP error occurred calling {url}: {e} - Response: {e.response.text if e.response and hasattr(e.response, 'text') else 'No response body'}",
                         exc_info=True)
            error_details = {"error": f"HTTP error: {e.response.status_code if e.response else 'Unknown status'}"}
            if e.response is not None:
                try:
                    error_details["details"] = e.response.json()
                except ValueError:
                    error_details["details"] = e.response.text if hasattr(e.response, 'text') else "Non-JSON response"
            else:
                error_details["details"] = str(e)
            return error_details
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception occurred calling {url}: {e}", exc_info=True)
            return {"error": "Request exception during API call.", "details": str(e)}

    def list_clusters(self):
        """
        Lists Kafka clusters from Confluent Cloud.
        The actual endpoint might differ. This is an example.
        Refer to Confluent Cloud API documentation for the correct endpoint.
        Example endpoint: /cmk/v2/clusters or /org/v2/environments/{env_id}/clusters
        For now, using a placeholder /kafka/v3/clusters as per some docs.
        It's common that you might need to list environments first, then clusters within an environment.
        """
        # This endpoint is a common one, but might require specific environment/organization IDs.
        # Replace with the correct endpoint from Confluent Cloud API documentation.
        # For example: "/cmk/v2/clusters" or "/org/v2/environments/<env-id>/clusters"
        # Using "/kafka/v3/clusters" as a placeholder based on some Confluent documentation.
        # The API may also be paginated.

        # A common pattern is to list environments first:
        # environments_data = self._request("GET", "/org/v2/environments")
        # if not environments_data or "data" not in environments_data:
        #     return {"error": "Could not fetch environments or no environments found."}
        #
        # clusters = []
        # for env in environments_data["data"]:
        # env_id = env["id"]
        # clusters_data = self._request("GET", f"/cmk/v2/environments/{env_id}/clusters") # Older API
        # # Or for newer API (example):
        # # clusters_data = self._request("GET", f"/kafka/v3/clusters?environment={env_id}")
        #     if clusters_data and "data" in clusters_data:
        #         clusters.extend(clusters_data["data"])
        # return clusters

        # For a simpler direct listing if available (less common for multi-environment setups):
        endpoint = "/kafka/v3/clusters" # Placeholder, verify this.
        # Note: Confluent Cloud APIs are often structured around organizations and environments.
        # A more realistic scenario would involve:
        # 1. Getting the organization ID.
        # 2. Listing environments within that organization.
        # 3. Listing clusters within a specific environment.
        # This simplified version assumes a direct cluster listing endpoint or that
        # the API key is scoped in a way that direct listing is possible.

        # print(f"Attempting to list clusters from: {self.base_url}{endpoint}")
        # print(f"Using API Key: {'Set' if self.api_key else 'Not Set'}")
        # Do not print the secret!

        # If API key/secret are not set, return an indicative message
        if not self.api_key or not self.api_secret:
            return {"error": "API credentials are not configured."}

        return self._request("GET", endpoint)

    def get_cluster_details(self, cluster_id: str):
        """
        Retrieves details for a specific Kafka cluster from Confluent Cloud.
        The endpoint /kafka/v3/clusters/{cluster_id} is a common pattern.
        """
        if not self.api_key or not self.api_secret:
            return {"error": "API credentials are not configured."}

        endpoint = f"/kafka/v3/clusters/{cluster_id}" # Placeholder, verify this.
        # print(f"Attempting to get cluster details from: {self.base_url}{endpoint}")
        return self._request("GET", endpoint)

    def list_topics(self, cluster_id: str):
        """
        Lists topics for a given Kafka cluster ID using confluent_kafka.AdminClient.
        """
        from confluent_kafka.admin import AdminClient, ConfigResource, TopicMetadata
        from confluent_kafka import KafkaError, KafkaException

        if not self.api_key or not self.api_secret:
            return {"error": "API credentials for Confluent Cloud are not configured."}

        # Step 1: Get cluster details to find bootstrap_servers
        cluster_details_response = self.get_cluster_details(cluster_id)

        if "error" in cluster_details_response:
            return {"error": f"Could not retrieve details for cluster {cluster_id} to get bootstrap servers.",
                    "details": cluster_details_response.get("details", cluster_details_response["error"])}

        # Assuming the cluster details response structure from /kafka/v3/clusters/{cluster_id}
        # has spec.http_endpoint or status.bootstrap_endpoint or spec.kafka_bootstrap_endpoint
        # Example path: cluster_details_response.get("status", {}).get("bootstrap_endpoint")
        # Example path: cluster_details_response.get("spec", {}).get("kafka_bootstrap_endpoint")
        # Let's try a common one for Confluent Cloud Kafka clusters:
        bootstrap_servers = cluster_details_response.get("spec", {}).get("kafka_bootstrap_endpoint")
        if not bootstrap_servers:
             # Fallback, sometimes it's directly in status for older API versions or different cluster types
            bootstrap_servers = cluster_details_response.get("status", {}).get("bootstrap_endpoint")


        if not bootstrap_servers:
            return {"error": f"Bootstrap servers not found in details for cluster {cluster_id}.",
                    "details": f"Response from get_cluster_details: {cluster_details_response}"}

        admin_config = {
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': self.api_key,
            'sasl.password': self.api_secret,
            # Add other necessary client configurations, e.g., request.timeout.ms
            'request.timeout.ms': 10000 # 10 seconds
        }

        try:
            admin_client = AdminClient(admin_config)
        except Exception as e:
            logger.error(f"Error creating AdminClient for cluster {cluster_id} (list_topics): {e}", exc_info=True)
            return {"error": f"Failed to initialize Kafka AdminClient for cluster {cluster_id}.", "details": str(e)}

        try:
            # The list_topics (or cluster_metadata) call returns a ClusterMetadata object
            # which contains brokers and topics.
            # Timeout is in seconds for this call.
            metadata = admin_client.list_topics(timeout=10)

            topics_info = []
            for topic_name, topic_metadata in metadata.topics.items():
                # topic_metadata is a confluent_kafka.admin.TopicMetadata object
                # We can extract partitions, replicas, etc.
                partitions_info = []
                for partition_id, partition_metadata in topic_metadata.partitions.items():
                    partitions_info.append({
                        "id": partition_id,
                        "leader": partition_metadata.leader,
                        "replicas": list(partition_metadata.replicas),
                        "isrs": list(partition_metadata.isrs)
                    })

                # Check for topic-level errors
                topic_error = None
                if topic_metadata.error is not None and topic_metadata.error.code() != KafkaError._NO_ERROR:
                    topic_error = {
                        "code": topic_metadata.error.code(),
                        "name": topic_metadata.error.name(),
                        "str": topic_metadata.error.str()
                    }

                topics_info.append({
                    "name": topic_name,
                    "partitions": partitions_info,
                    "error": topic_error # Will be None if no error
                })

            return {"data": topics_info}

        except KafkaException as e:
            # Specific Kafka library exceptions
            kafka_error = e.args[0] if e.args else None
            error_details = {
                "message": str(e),
                "kafka_error_code": kafka_error.code() if hasattr(kafka_error, 'code') else None,
                "kafka_error_name": kafka_error.name() if hasattr(kafka_error, 'name') else None,
                "kafka_error_str": kafka_error.str() if hasattr(kafka_error, 'str') else None,
            }
            logger.error(f"KafkaException while listing topics for cluster {cluster_id}: {error_details}", exc_info=True)
            return {"error": f"Kafka operation failed while listing topics for cluster {cluster_id}.",
                    "details": error_details}
        except Exception as e:
            logger.error(f"Unexpected error listing topics for cluster {cluster_id}: {e}", exc_info=True)
            return {"error": f"An unexpected error occurred while listing topics for cluster {cluster_id}.",
                    "details": str(e)}


# Example usage (for testing purposes, not part of the service itself):
# if __name__ == '__main__':
#     # This block would require Django settings to be configured.
#     # You'd typically test this through Django's shell or a management command.
#     # from django.conf import settings
#     # settings.configure(CONFLUENT_CLOUD_API_KEY='your_key', CONFLUENT_CLOUD_API_SECRET='your_secret', CONFLUENT_CLOUD_API_BASE_URL='https_api_url')
#     # import django
#     # django.setup()
#
#     service = ConfluentCloudService()
#     clusters = service.list_clusters()
#     if "error" in clusters:
#         print(f"Error listing clusters: {clusters['error']}")
#         if "details" in clusters:
#             print(f"Details: {clusters['details']}")
#     else:
#         print("Clusters listed successfully:")
#         for cluster in clusters.get("data", []): # Adjust based on actual API response structure
#             print(f"  ID: {cluster.get('id')}, Name: {cluster.get('spec', {}).get('display_name')}")

def get_confluent_cloud_service():
    """Factory function to get an instance of the service."""
    return ConfluentCloudService()

    def get_committed_offsets(self, cluster_id: str, group_id: str, topic_name: str):
        """
        Retrieves committed offsets for a specific consumer group on a given topic.
        """
        from confluent_kafka import Consumer, TopicPartition, KafkaError, KafkaException

        if not self.api_key or not self.api_secret:
            return {"error": "API credentials for Confluent Cloud are not configured."}

        cluster_details_response = self.get_cluster_details(cluster_id)
        if "error" in cluster_details_response:
            return {"error": f"Could not retrieve details for cluster {cluster_id} to get bootstrap servers.",
                    "details": cluster_details_response.get("details", cluster_details_response["error"])}

        bootstrap_servers = cluster_details_response.get("spec", {}).get("kafka_bootstrap_endpoint")
        if not bootstrap_servers:
            bootstrap_servers = cluster_details_response.get("status", {}).get("bootstrap_endpoint")

        if not bootstrap_servers:
            return {"error": f"Bootstrap servers not found in details for cluster {cluster_id}.",
                    "details": f"Response from get_cluster_details: {cluster_details_response}"}

        consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': self.api_key,
            'sasl.password': self.api_secret,
            'group.id': group_id,
            'enable.auto.commit': False,
            # Timeouts:
            'socket.timeout.ms': 10000, # Default is 60000
            'session.timeout.ms': 30000, # Default is 45000. For fetching offsets, can be shorter.
            # 'debug': 'consumer,cgrp,topic,fetch' # For verbose debugging if needed
        }

        consumer = None
        try:
            consumer = Consumer(consumer_config)
        except Exception as e:
            logger.error(f"Error creating Consumer for group {group_id} on cluster {cluster_id}: {e}", exc_info=True)
            return {"error": f"Failed to initialize Kafka Consumer for group {group_id}.", "details": str(e)}

        try:
            # Get metadata for the specific topic to find its partitions
            # The timeout for list_topics is in seconds.
            topic_metadata_obj = consumer.list_topics(topic=topic_name, timeout=10)

            if topic_metadata_obj is None or topic_name not in topic_metadata_obj.topics:
                # consumer.close() # Close before returning
                return {"error": f"Topic '{topic_name}' not found or metadata not available on cluster {cluster_id} for group {group_id}."}

            if topic_metadata_obj.topics[topic_name].error is not None:
                topic_err = topic_metadata_obj.topics[topic_name].error
                # consumer.close()
                return {"error": f"Error fetching metadata for topic '{topic_name}': {topic_err.str()}",
                        "details": {"code": topic_err.code(), "name": topic_err.name()}}

            partitions_metadata = topic_metadata_obj.topics[topic_name].partitions
            if not partitions_metadata:
                # consumer.close()
                return {"error": f"No partitions found for topic '{topic_name}' on cluster {cluster_id}."}

            # Create TopicPartition list for the committed() call
            # The offset in TopicPartition objects for the input list is ignored by committed().
            topic_partitions_for_query = [TopicPartition(topic_name, p_id) for p_id in partitions_metadata.keys()]

            if not topic_partitions_for_query: # Should be redundant given previous check
                # consumer.close()
                return {"error": f"Cannot query committed offsets: No partitions constructed for topic '{topic_name}'."}

            # Fetch committed offsets. Timeout is in seconds.
            committed_partitions = consumer.committed(topic_partitions_for_query, timeout=10)

            offsets_data = []
            for tp in committed_partitions:
                offset_val = tp.offset
                tp_error_details = None
                if tp.error: # Check if there's an error object associated with this TopicPartition
                    tp_error_details = {
                        "code": tp.error.code(),
                        "name": tp.error.name(),
                        "str": tp.error.str()
                    }

                # If offset is OFFSET_INVALID (-1001), it means no offset committed or group unknown/inactive for this partition.
                # It's not an "error" in the sense of a failed operation, but data indicating state.
                # The serializer can choose how to represent OFFSET_INVALID (e.g. as null, or the number itself).
                offsets_data.append({
                    "partition": tp.partition,
                    "offset": offset_val, # Could be -1001 (OFFSET_INVALID)
                    "metadata": tp.metadata, # Consumer group metadata (rarely used)
                    "error": tp_error_details # Error specific to this partition's offset fetch
                })

            return {"data": offsets_data}

        except KafkaException as e:
            kafka_error = e.args[0] if e.args else None
            error_details = {
                "message": str(e),
                "kafka_error_code": kafka_error.code() if hasattr(kafka_error, 'code') else None,
                "kafka_error_name": kafka_error.name() if hasattr(kafka_error, 'name') else None,
                "kafka_error_str": kafka_error.str() if hasattr(kafka_error, 'str') else None,
            }
            logger.error(f"KafkaException while getting committed offsets for group {group_id}, topic {topic_name}: {error_details}",
                         exc_info=True)
            return {"error": f"Kafka operation failed for group {group_id}, topic {topic_name}.", "details": error_details}
        except Exception as e:
            logger.error(f"Unexpected error getting committed offsets for group {group_id}, topic {topic_name}: {e}",
                         exc_info=True)
            return {"error": "An unexpected error occurred while fetching committed offsets.", "details": str(e)}
        finally:
            if consumer:
                consumer.close()

    def _build_metrics_query_payload(self, metric_name: str, resource_kafka_id: str,
                                     granularity: str = "PT1M", intervals: list[str] = None,
                                     group_by: list[str] = None, filters: list[dict] = None,
                                     limit: int = 100): # Added limit
        """Helper to construct the JSON payload for the Metrics API query."""

        # Default interval to last 1 hour if not provided
        if intervals is None:
            intervals = ["PT1H/now"] # Last 1 hour

        payload = {
            "aggregations": [{"metric": metric_name}],
            "filter": {"op": "AND", "filters": []}, # Start with AND to easily add filters
            "granularity": granularity,
            "intervals": intervals,
            "limit": limit
        }

        # Base filter for kafka cluster id
        payload["filter"]["filters"].append({
            "field": "resource.kafka.id",
            "op": "EQ",
            "value": resource_kafka_id
        })

        # Add additional filters if provided
        if filters:
            payload["filter"]["filters"].extend(filters)

        # If only one filter (the default kafka_id filter), simplify the structure if desired,
        # though "AND" with one filter is valid.
        # For now, always using "AND" for simplicity of adding more filters.

        if group_by:
            payload["group_by"] = group_by

        return payload

    def query_confluent_metrics(self, payload: dict):
        """
        Generic method to query the Confluent Cloud Metrics API.
        The API endpoint is /v2/metrics/cloud/query
        """
        if not self.api_key or not self.api_secret:
            return {"error": "API credentials for Confluent Cloud are not configured."}

        # The Metrics API uses a different base URL path structure sometimes,
        # or might be on a different subdomain like `api.telemetry.confluent.cloud`.
        # The documentation examples use `https://api.telemetry.confluent.cloud`.
        # Let's assume our self.base_url is for general Confluent Cloud APIs (like /cmk/v2)
        # and construct the telemetry URL specifically here.
        # This should ideally be a setting.
        # For now, hardcoding based on documentation for `https://api.telemetry.confluent.cloud`

        # metrics_api_base_url = "https://api.telemetry.confluent.cloud" # From docs
        # Using the self.base_url for now, assuming it's correctly set for telemetry or general API
        # If self.base_url = "https://api.confluent.cloud", then endpoint should be "/v2/metrics/cloud/query" to match docs.
        # The docs show POST https://api.telemetry.confluent.cloud/v2/metrics/cloud/query
        # Let's make this configurable or use a more specific base URL for telemetry.
        # For now, using a fixed one from docs.

        # The base URL for _request is settings.CONFLUENT_CLOUD_API_BASE_URL
        # which is "https://api.confluent.cloud".
        # The metrics endpoint is "https://api.telemetry.confluent.cloud/v2/metrics/cloud/query"
        # This implies we might need a separate way to call this or adjust _request.
        # For simplicity, I will use `requests` directly here, assuming the telemetry endpoint.

        metrics_endpoint_url = "https://api.telemetry.confluent.cloud/v2/metrics/cloud/query"
        auth = (self.api_key, self.api_secret)
        headers = {"Content-Type": "application/json"}

        try:
            # print(f"Querying Confluent Metrics API: {metrics_endpoint_url} with payload: {payload}")
            response = requests.post(metrics_endpoint_url, auth=auth, json=payload, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error occurred querying metrics via {metrics_endpoint_url}: {e} - "
                         f"Response: {e.response.text if e.response and hasattr(e.response, 'text') else 'No response body'}",
                         exc_info=True)
            error_details = {"error": f"HTTP error: {e.response.status_code if e.response else 'Unknown status'}"}
            if e.response is not None:
                try:
                    error_details["details"] = e.response.json()
                except ValueError:
                    error_details["details"] = e.response.text if hasattr(e.response, 'text') else "Non-JSON response"
            else:
                error_details["details"] = str(e)
            return error_details
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception occurred querying metrics via {metrics_endpoint_url}: {e}", exc_info=True)
            return {"error": "Request exception during metrics query.", "details": str(e)}


    def get_consumer_lag(self, cluster_id: str, group_id: str,
                         topic_name: str = None,
                         start_time_str: str = None, end_time_str: str = None,
                         granularity: str = "PT1M", interval_str: str = "PT1H/now"):
        """
        Get consumer lag for a specific group, optionally filtered by topic.
        Intervals are ISO 8601 format e.g., "2023-01-01T00:00:00Z/2023-01-01T01:00:00Z" or "PT1H/now".
        """
        metric_name = "io.confluent.kafka.server/consumer_lag_offsets"

        # Determine intervals: use start/end if provided, else use interval_str
        if start_time_str and end_time_str:
            intervals = [f"{start_time_str}/{end_time_str}"]
        else:
            intervals = [interval_str]

        additional_filters = []
        if group_id: # group_id is mandatory for this conceptual metric
             additional_filters.append({"field": "metric.consumer_group_id", "op": "EQ", "value": group_id})
        else: # Should not happen if API requires group_id
            return {"error": "Consumer group ID is required for lag metrics."}

        if topic_name:
            additional_filters.append({"field": "metric.topic", "op": "EQ", "value": topic_name})

        # For consumer lag, we typically want to see it per partition as well,
        # so group_by should include metric.partition.
        # Also, if not filtering by topic_name, group by metric.topic.
        group_by_fields = ["metric.consumer_group_id", "metric.partition"]
        if not topic_name: # If no specific topic, group by topic to see lag per topic
            group_by_fields.append("metric.topic")
        else: # If specific topic, it's already in filter, no need to group by it unless comparing multiple specific topics
             pass # Already filtered by topic if topic_name is provided.

        payload = self._build_metrics_query_payload(
            metric_name=metric_name,
            resource_kafka_id=cluster_id,
            granularity=granularity,
            intervals=intervals,
            group_by=group_by_fields,
            filters=additional_filters
        )
        return self.query_confluent_metrics(payload)

    def get_cluster_throughput(self, cluster_id: str,
                               start_time_str: str = None, end_time_str: str = None,
                               granularity: str = "PT1M", interval_str: str = "PT1H/now"):
        """
        Get cluster throughput (received_bytes and sent_bytes).
        """
        if start_time_str and end_time_str:
            intervals = [f"{start_time_str}/{end_time_str}"]
        else:
            intervals = [interval_str]

        results = {}
        metrics_to_fetch = {
            "received_bytes": "io.confluent.kafka.server/received_bytes",
            "sent_bytes": "io.confluent.kafka.server/sent_bytes"
        }

        for key, metric_name in metrics_to_fetch.items():
            payload = self._build_metrics_query_payload(
                metric_name=metric_name,
                resource_kafka_id=cluster_id,
                granularity=granularity,
                intervals=intervals,
                group_by=None # Total for the cluster
            )
            response = self.query_confluent_metrics(payload)
            if "error" in response:
                # If one metric fails, we might want to return partial results or a combined error
                results[key] = response # Store error for this specific metric
            else:
                results[key] = response.get("data", []) # Expecting list of {"timestamp": "...", "value": ...}

        # Check if all results are errors to return a top-level error
        all_errors = True
        any_error = False
        for key in metrics_to_fetch:
            if isinstance(results.get(key), dict) and "error" in results[key]:
                any_error = True
            else: # Found data
                all_errors = False

        if all_errors and any_error: # All attempted metrics resulted in an error structure
             # Combine errors or return the first one
            first_error_key = next(iter(metrics_to_fetch))
            return {"error": f"Failed to fetch all throughput metrics. First error on {first_error_key}: {results[first_error_key].get('error')}",
                    "details": results[first_error_key].get('details')}

        # Return structure like: {"received_bytes": [...data...], "sent_bytes": [...data...]}
        # Or: {"received_bytes": {"error":...}, "sent_bytes": [...data...]}
        return {"data": results} # The 'data' key here wraps the dict of metric results

    def get_topic_details(self, cluster_id: str, topic_name: str):
        """
        Retrieves details for a specific topic, including configuration and partition information.
        """
        from confluent_kafka.admin import AdminClient, ConfigResource, ResourceType, KafkaError, KafkaException
        from confluent_kafka import TopicPartition # For partition metadata if needed, though list_topics gives it

        if not self.api_key or not self.api_secret:
            return {"error": "API credentials for Confluent Cloud are not configured."}

        cluster_details_response = self.get_cluster_details(cluster_id)
        if "error" in cluster_details_response:
            return {"error": f"Could not retrieve cluster details for {cluster_id} to get bootstrap servers.",
                    "details": cluster_details_response.get("details", cluster_details_response["error"])}

        bootstrap_servers = cluster_details_response.get("spec", {}).get("kafka_bootstrap_endpoint")
        if not bootstrap_servers:
            bootstrap_servers = cluster_details_response.get("status", {}).get("bootstrap_endpoint")

        if not bootstrap_servers:
            return {"error": f"Bootstrap servers not found for cluster {cluster_id}.",
                    "details": f"Response from get_cluster_details: {cluster_details_response}"}

        admin_config = {
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': self.api_key,
            'sasl.password': self.api_secret,
            'request.timeout.ms': 10000
        }

        admin_client = None
        try:
            admin_client = AdminClient(admin_config)
        except Exception as e:
            logger.error(f"Error creating AdminClient for cluster {cluster_id} (get_topic_details): {e}", exc_info=True)
            return {"error": "Failed to initialize Kafka AdminClient.", "details": str(e)}

        topic_details = {"name": topic_name, "partitions": [], "config": {}, "error": None}

        try:
            # 1. Get topic configuration
            resource = ConfigResource(ResourceType.TOPIC, topic_name)
            # describe_configs returns a future map: {ConfigResource: future}
            future_map = admin_client.describe_configs([resource], request_timeout=10)

            config_entries_dict = {}
            for res, future in future_map.items():
                try:
                    config_result = future.result() # This is a dict of {config_name: ConfigEntry}
                    for entry_name, entry_obj in config_result.items():
                        config_entries_dict[entry_name] = {
                            "name": entry_obj.name,
                            "value": entry_obj.value,
                            "source": entry_obj.source.name, # Source enum to string
                            "is_sensitive": entry_obj.is_sensitive,
                            "is_default": entry_obj.is_default,
                            "read_only": entry_obj.is_read_only,
                        }
                    topic_details["config"] = config_entries_dict
                except KafkaException as ke:
                    kafka_error = ke.args[0]
                    err_msg = f"Failed to describe config for topic {topic_name}: {kafka_error.str()}"
                    logger.warning(f"{err_msg} (cluster: {cluster_id})", exc_info=True)
                    topic_details["error"] = topic_details.get("error","") + err_msg + "; "
                    # Continue to try and get partition info
                except Exception as e:
                    err_msg = f"Unexpected error describing config for topic {topic_name}: {e}"
                    logger.warning(f"{err_msg} (cluster: {cluster_id})", exc_info=True)
                    topic_details["error"] = topic_details.get("error","") + err_msg + "; "
                    # Continue

            # 2. Get topic partition information (leader, replicas, isr)
            # list_topics returns metadata for all topics, or specific ones if specified.
            # We need to filter for our specific topic.
            cluster_metadata = admin_client.list_topics(topic=topic_name, timeout=10)

            if topic_name not in cluster_metadata.topics:
                err_msg = f"Topic {topic_name} not found in cluster metadata for cluster {cluster_id}."
                logger.warning(err_msg)
                topic_details["error"] = topic_details.get("error","") + err_msg + "; "
                # If topic doesn't exist, config description above would also likely fail or return empty.
                # We can return early if a definitive "topic not found" occurs.
                # However, describe_configs might not error if topic doesn't exist, just return empty.
                # Let's check the error from describe_configs if it indicated topic missing.
                # For now, assume it might be a partial failure.
            else:
                topic_meta = cluster_metadata.topics[topic_name]
                if topic_meta.error is not None:
                    err_msg = f"Error fetching metadata for topic {topic_name} on cluster {cluster_id}: {topic_meta.error.str()}"
                    logger.warning(err_msg, exc_info=True) # exc_info might be relevant if error object has more
                    topic_details["error"] = topic_details.get("error","") + err_msg + "; "
                else:
                    partitions_info = []
                    for p_id, p_meta in topic_meta.partitions.items():
                        partitions_info.append({
                            "id": p_id,
                            "leader": p_meta.leader,
                            "replicas": list(p_meta.replicas),
                            "isrs": list(p_meta.isrs)
                        })
                    topic_details["partitions"] = partitions_info

            # If there were errors but we got some data, return it with error messages.
            # If the only thing is an error message, it means the topic likely doesn't exist or is inaccessible.
            if topic_details["error"] and not topic_details["config"] and not topic_details["partitions"]:
                 return {"error": f"Failed to retrieve details for topic {topic_name}.", "details": topic_details["error"]}

            return {"data": topic_details}

        except KafkaException as e:
            kafka_error = e.args[0]
            error_details = {
                "message": str(e),
                "kafka_error_code": kafka_error.code(),
                "kafka_error_name": kafka_error.name(),
                "kafka_error_str": kafka_error.str(),
            }
            logger.error(f"KafkaException in get_topic_details for topic {topic_name} on cluster {cluster_id}: {error_details}",
                         exc_info=True)
            return {"error": f"Kafka operation failed for topic {topic_name}.", "details": error_details}
        except Exception as e:
            logger.error(f"Unexpected error in get_topic_details for topic {topic_name} on cluster {cluster_id}: {e}",
                         exc_info=True)
            return {"error": "An unexpected error occurred.", "details": str(e)}
        # No finally block for admin_client.close() as AdminClient doesn't have a close method.
        # It's designed to be relatively lightweight.

    def dummy_method_for_testing_reloads(self):
        return "dummy_method_called"
