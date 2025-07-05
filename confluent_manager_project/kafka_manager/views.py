from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from .services import get_confluent_cloud_service
from .serializers import ClusterSerializer, ClusterListSerializer, ErrorSerializer

class ClusterListView(APIView):
    """
    API endpoint to list Kafka clusters from Confluent Cloud.
    """
    def get(self, request, format=None):
        """
        Return a list of all Kafka clusters.
        """
        service = get_confluent_cloud_service()

        # Check if API credentials are set, return error if not
        if not service.api_key or not service.api_secret:
            error_data = {"error": "Confluent Cloud API credentials are not configured in settings."}
            serializer = ErrorSerializer(data=error_data)
            serializer.is_valid(raise_exception=True) # Should always be valid
            return Response(serializer.data, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        clusters_data = service.list_clusters()

        if clusters_data is None: # Should not happen if service._request handles errors
            error_data = {"error": "Failed to fetch clusters from Confluent Cloud. Service returned None."}
            serializer = ErrorSerializer(data=error_data)
            serializer.is_valid(raise_exception=True)
            return Response(serializer.data, status=status.HTTP_503_SERVICE_UNAVAILABLE)

        if "error" in clusters_data:
            # If the service layer already prepared an error structure
            serializer = ErrorSerializer(data=clusters_data)
            if serializer.is_valid(): # Check if it matches ErrorSerializer
                return Response(serializer.data, status=status.HTTP_502_BAD_GATEWAY) # Error from upstream
            else: # Fallback if error structure is unexpected
                return Response({"error": "An unexpected error occurred while fetching data from Confluent Cloud.",
                                 "details": str(clusters_data.get("details", clusters_data))},
                                status=status.HTTP_502_BAD_GATEWAY)

        # The Confluent Cloud API for listing clusters (e.g., /kafka/v3/clusters)
        # typically returns a response like:
        # {
        #   "api_version": "kafka.v3",
        #   "kind": "ClusterList",
        #   "metadata": { "first": "...", "last": "...", ... },
        #   "data": [ {cluster_object_1}, {cluster_object_2} ]
        # }
        # We need to pass the list of clusters (value of "data" key) to the serializer if it expects a list.

        # ClusterListSerializer expects the whole dict including the 'data' key.
        list_serializer = ClusterListSerializer(data=clusters_data)
        if list_serializer.is_valid():
            return Response(list_serializer.data) # This will be like {"data": [serialized_clusters]}
        else:
            # This case might happen if the API response structure is not what ClusterListSerializer expects
            # (e.g., no 'data' key, or 'data' is not a list).
            # Try serializing with ClusterSerializer(many=True) if clusters_data is already a list
            if isinstance(clusters_data, list):
                # This path would be taken if service.list_clusters() returned just the list of clusters
                many_serializer = ClusterSerializer(data=clusters_data, many=True)
                if many_serializer.is_valid():
                    return Response(many_serializer.data)
                else:
                    # Errors from ClusterSerializer(many=True)
                    return Response({"error": "Failed to serialize cluster list.", "details": many_serializer.errors},
                                    status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            # If it's not a list and ClusterListSerializer failed, then it's an unknown structure or serializer error
            return Response({"error": "Invalid data structure received from Confluent Cloud or serializer error.",
                             "details": list_serializer.errors},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class TopicListView(APIView):
    """
    API endpoint to list topics for a specific Kafka cluster.
    """
    def get(self, request, cluster_id, format=None):
        """
        Return a list of topics for the given cluster_id.
        """
        service = get_confluent_cloud_service()

        # Check if API credentials are set in Django settings (for Confluent Cloud API interaction)
        if not service.api_key or not service.api_secret:
            error_data = {"error": "Confluent Cloud API credentials are not configured in settings."}
            # Using ErrorSerializer defined earlier
            serializer = ErrorSerializer(data=error_data)
            serializer.is_valid(raise_exception=True)
            return Response(serializer.data, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # Call the service method to list topics
        # This method internally handles getting cluster details for bootstrap servers
        # and then uses AdminClient to list topics.
        topics_response = service.list_topics(cluster_id=cluster_id)

        # Use TopicListSerializer to serialize the response from the service.
        # This serializer is designed to handle both success ({"data": [...]}) and error ({"error": ...}) structures.
        serializer = TopicListSerializer(data=topics_response)

        if serializer.is_valid():
            if "error" in serializer.validated_data:
                # Determine appropriate status code based on the error source if possible
                # For now, using a generic 502 if it's an error from the service call
                # or 503 if Kafka itself is unavailable, etc.
                # The service layer error messages might give clues.
                # If error is "Could not retrieve details for cluster..." it's likely a 404 or bad config.
                # If error is "Bootstrap servers not found..." it's a config issue from CCloud API.
                # If error is "Kafka operation failed..." it's likely 503 or 502.
                # Let's use 502 for upstream failures generally from the service.
                # Could refine this based on specific error strings from the service.
                if "Could not retrieve details for cluster" in serializer.validated_data["error"]:
                     return Response(serializer.data, status=status.HTTP_404_NOT_FOUND) # Or 502 if cluster exists but details fail
                return Response(serializer.data, status=status.HTTP_502_BAD_GATEWAY)
            return Response(serializer.data) # Contains {"data": [serialized_topics]}
        else:
            # This means the structure returned by list_topics was not what TopicListSerializer expected
            # (e.g., neither 'data' nor 'error' key, or other validation fails)
            return Response(
                {"error": "Invalid response structure from topic listing service or serializer error.",
                 "details": serializer.errors},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class TopicDetailView(APIView):
    """
    API endpoint to get details for a specific topic within a cluster.
    """
    def get(self, request, cluster_id, topic_name, format=None):
        service = get_confluent_cloud_service()
        if not service.api_key or not service.api_secret:
            error_data = {"error": "Confluent Cloud API credentials are not configured."}
            err_serializer = ErrorSerializer(data=error_data)
            err_serializer.is_valid(raise_exception=True)
            return Response(err_serializer.data, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        topic_details_response = service.get_topic_details(
            cluster_id=cluster_id,
            topic_name=topic_name
        )

        serializer = TopicDetailsResponseSerializer(data=topic_details_response)

        if serializer.is_valid():
            if "error" in serializer.validated_data: # Top-level error from service
                # Could refine status code based on error content from service
                error_msg = serializer.validated_data.get("error", "").lower()
                details = serializer.validated_data.get("details", "")
                if "could not retrieve cluster details" in error_msg or \
                   "bootstrap servers not found" in error_msg:
                    status_code = status.HTTP_502_BAD_GATEWAY
                elif "topic not found" in str(details).lower() or "topic not found" in error_msg : # Check details too
                    status_code = status.HTTP_404_NOT_FOUND
                else: # Other Kafka operational errors or init errors
                    status_code = status.HTTP_503_SERVICE_UNAVAILABLE
                return Response(serializer.data, status=status_code)

            # Successful response, data might contain its own partial 'error' string field
            return Response(serializer.data)
        else:
            # Serializer validation failed - means the service returned an unexpected structure
            return Response(
                {"error": "Invalid response structure from topic details service or serializer error.",
                 "details": serializer.errors},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

# Assuming these serializers are defined in .serializers
from .serializers import (
    ClusterSerializer, ClusterListSerializer, # Added back for ClusterListView
    TopicSerializer, TopicListSerializer, # Added back for TopicListView
    CommittedOffsetSerializer, CommittedOffsetListSerializer, # Added for CommittedOffsetsView
    DatapointSerializer,
    ThroughputResponseSerializer,
    GenericMetricListResponseSerializer,
    ErrorSerializer,
    TopicDetailsResponseSerializer
)


class ConsumerGroupLagView(APIView):
    """
    API endpoint to get consumer group lag metrics.
    Path params: cluster_id, group_id
    Query params: topic_name (optional), start, end, interval, granularity
    """
    def get(self, request, cluster_id, group_id, format=None):
        service = get_confluent_cloud_service()
        if not service.api_key or not service.api_secret:
            # ... (credential error handling as in other views)
            error_data = {"error": "Confluent Cloud API credentials are not configured."}
            err_serializer = ErrorSerializer(data=error_data)
            err_serializer.is_valid(raise_exception=True)
            return Response(err_serializer.data, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # Extract query parameters
        topic_name = request.query_params.get('topic_name')
        start_time_str = request.query_params.get('start') # e.g., "2023-01-01T00:00:00Z"
        end_time_str = request.query_params.get('end')     # e.g., "2023-01-01T01:00:00Z"
        interval_str = request.query_params.get('interval', "PT1H/now") # Default: last 1 hour
        granularity = request.query_params.get('granularity', "PT1M") # Default: 1 minute

        # Basic validation for time parameters (more can be added)
        # For simplicity, passing them as strings to service, service can validate/format further.

        metrics_response = service.get_consumer_lag(
            cluster_id=cluster_id,
            group_id=group_id,
            topic_name=topic_name,
            start_time_str=start_time_str,
            end_time_str=end_time_str,
            interval_str=interval_str,
            granularity=granularity
        )

        # The service method get_consumer_lag returns a dict like:
        # {"data": [datapoint_objects_with_labels]} or {"error": "...", "details": "..."}
        # GenericMetricListResponseSerializer expects this structure.
        serializer = GenericMetricListResponseSerializer(data=metrics_response)

        if serializer.is_valid():
            if "error" in serializer.validated_data:
                # Determine status code based on error (could be more refined)
                return Response(serializer.data, status=status.HTTP_502_BAD_GATEWAY)
            return Response(serializer.data)
        else:
            return Response(
                {"error": "Invalid response structure from consumer lag service or serializer error.",
                 "details": serializer.errors},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class ClusterThroughputView(APIView):
    """
    API endpoint to get cluster throughput metrics (received and sent bytes).
    Path params: cluster_id
    Query params: start, end, interval, granularity
    """
    def get(self, request, cluster_id, format=None):
        service = get_confluent_cloud_service()
        if not service.api_key or not service.api_secret:
            # ... (credential error handling)
            error_data = {"error": "Confluent Cloud API credentials are not configured."}
            err_serializer = ErrorSerializer(data=error_data)
            err_serializer.is_valid(raise_exception=True)
            return Response(err_serializer.data, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        start_time_str = request.query_params.get('start')
        end_time_str = request.query_params.get('end')
        interval_str = request.query_params.get('interval', "PT1H/now")
        granularity = request.query_params.get('granularity', "PT1M")

        metrics_response = service.get_cluster_throughput(
            cluster_id=cluster_id,
            start_time_str=start_time_str,
            end_time_str=end_time_str,
            interval_str=interval_str,
            granularity=granularity
        )

        # The service method get_cluster_throughput returns a dict like:
        # {"data": {"received_bytes": [...], "sent_bytes": [...]}} or
        # {"error": "...", "details": "..."} or
        # {"data": {"received_bytes": {"error": ...}, "sent_bytes": [...]}} if one metric fails
        # ThroughputResponseSerializer expects this structure.
        serializer = ThroughputResponseSerializer(data=metrics_response)

        if serializer.is_valid():
            if "error" in serializer.validated_data: # Top-level error from service (e.g., all metric queries failed)
                return Response(serializer.data, status=status.HTTP_502_BAD_GATEWAY)

            # Check for partial errors within the data structure
            # (e.g., received_bytes failed but sent_bytes succeeded)
            # The ThroughputResponseSerializer and its nested ThroughputDataSerializer
            # will serialize whatever data or error is present for each metric.
            # Client will need to check for errors at metric level if data isn't as expected.
            return Response(serializer.data)
        else:
            return Response(
                {"error": "Invalid response structure from cluster throughput service or serializer error.",
                 "details": serializer.errors},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class CommittedOffsetsView(APIView):
    """
    API endpoint to get committed offsets for a specific consumer group on a topic.
    """
    def get(self, request, cluster_id, group_id, topic_name, format=None):
        """
        Return committed offsets for the given cluster, group, and topic.
        """
        service = get_confluent_cloud_service()

        if not service.api_key or not service.api_secret:
            error_data = {"error": "Confluent Cloud API credentials are not configured in settings."}
            serializer = ErrorSerializer(data=error_data) # Reusing existing ErrorSerializer
            serializer.is_valid(raise_exception=True)
            return Response(serializer.data, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        offset_response = service.get_committed_offsets(
            cluster_id=cluster_id,
            group_id=group_id,
            topic_name=topic_name
        )

        serializer = CommittedOffsetListSerializer(data=offset_response)

        if serializer.is_valid():
            if "error" in serializer.validated_data:
                # Basic error handling based on error messages from service layer.
                # This could be more sophisticated by returning specific error codes from service.
                error_msg = serializer.validated_data.get("error", "").lower()
                if "could not retrieve details for cluster" in error_msg or \
                   "bootstrap servers not found" in error_msg or \
                   f"topic '{topic_name}' not found" in error_msg:
                    status_code = status.HTTP_404_NOT_FOUND
                elif "failed to initialize kafka consumer" in error_msg or \
                     "kafka operation failed" in error_msg:
                    status_code = status.HTTP_503_SERVICE_UNAVAILABLE # Kafka service unavailable or operational error
                else:
                    status_code = status.HTTP_502_BAD_GATEWAY # Generic upstream error
                return Response(serializer.data, status=status_code)

            # Success case: data is present
            return Response(serializer.data)
        else:
            # Serializer validation failed - means the service returned an unexpected structure
            return Response(
                {"error": "Invalid response structure from committed offsets service or serializer error.",
                 "details": serializer.errors},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
