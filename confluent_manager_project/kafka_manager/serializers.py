from rest_framework import serializers

class ClusterSpecSerializer(serializers.Serializer):
    """
    Serializer for the 'spec' part of a Confluent Cloud Kafka cluster.
    Fields might vary based on the actual API response.
    """
    display_name = serializers.CharField(max_length=255, required=False)
    cloud = serializers.CharField(max_length=50, required=False)
    region = serializers.CharField(max_length=50, required=False)
    availability = serializers.CharField(max_length=50, required=False) # e.g., "SINGLE_ZONE", "MULTI_ZONE"
    # Add other relevant spec fields as needed

class ClusterStatusSerializer(serializers.Serializer):
    """
    Serializer for the 'status' part of a Confluent Cloud Kafka cluster.
    """
    phase = serializers.CharField(max_length=50, required=False) # e.g. "PROVISIONED"
    # Add other relevant status fields as needed

class ClusterMetadataSerializer(serializers.Serializer):
    """
    Serializer for the 'metadata' part of a Confluent Cloud Kafka cluster.
    """
    self = serializers.URLField(required=False)
    resource_name = serializers.CharField(max_length=255, required=False)
    created_at = serializers.DateTimeField(required=False)
    updated_at = serializers.DateTimeField(required=False)
    deleted_at = serializers.DateTimeField(required=False, allow_null=True)


class ClusterSerializer(serializers.Serializer):
    """
    Serializer for a Confluent Cloud Kafka cluster.
    This structure is based on common Confluent Cloud API responses (e.g., /kafka/v3/clusters).
    Adjust fields based on the actual data returned by `ConfluentCloudService.list_clusters()`.
    """
    api_version = serializers.CharField(max_length=50, required=False, default="kafka.v3")
    kind = serializers.CharField(max_length=50, required=False, default="Cluster")
    id = serializers.CharField(max_length=100)
    metadata = ClusterMetadataSerializer(required=False)
    spec = ClusterSpecSerializer(required=False)
    status = ClusterStatusSerializer(required=False)

    # If the list_clusters() service returns a flat structure, you might have fields directly here:
    # display_name = serializers.CharField(source="spec.display_name", read_only=True) # Example if data is nested
    # name = serializers.CharField(max_length=255, required=False) # Alternative if 'display_name' isn't always there
    # bootstrap_servers = serializers.CharField(source="status.bootstrap_endpoint", read_only=True) # Example

    def to_representation(self, instance):
        """
        Handle cases where the input `instance` might be a dictionary that doesn't perfectly
        match the serializer structure, especially if some keys are missing.
        """
        # Basic example: if 'spec' or 'status' or 'metadata' is not in the instance,
        # DRF would typically skip it or error if 'required=True'.
        # Since they are not required=True at the field level by default, this is mostly fine.
        # You can add custom logic here if needed.
        # For example, to provide default sub-dictionaries if they are missing:
        # instance.setdefault('spec', {})
        # instance.setdefault('status', {})
        # instance.setdefault('metadata', {})
        return super().to_representation(instance)

class ClusterListSerializer(serializers.Serializer):
    """
    Serializer for the response from the Confluent Cloud API when listing clusters,
    which often includes pagination or a 'data' wrapper.
    Example: { "api_version": "...", "kind": "...", "metadata": {...}, "data": [...] }
    """
    # api_version = serializers.CharField(max_length=50, required=False)
    # kind = serializers.CharField(max_length=50, required=False)
    # metadata = serializers.DictField(child=serializers.CharField(), required=False) # For list metadata like pagination
    data = ClusterSerializer(many=True)

    def to_representation(self, instance):
        """
        The `ConfluentCloudService.list_clusters()` method is expected to return
        the parsed JSON response. If this response is a dictionary containing a 'data' key
        (which holds the list of clusters), this serializer handles that.
        If the service call directly returns a list of cluster dictionaries,
        then the view should use `ClusterSerializer(many=True)` directly.
        """
        # Assuming `instance` is the direct dictionary returned by `_request`
        # which might be {"data": [cluster1, cluster2], "metadata": {...}, ...}
        # or it could be just an error object {"error": "..."}
        if "error" in instance:
            # If the service returned an error, we pass it through or transform it.
            # For now, let this be handled by the view.
            return instance

        # If the actual list of clusters is under a 'data' key in the response:
        if 'data' in instance and isinstance(instance['data'], list):
             # Use this if the API returns a structure like {"data": [...], "metadata": {...}}
            return super().to_representation(instance)

        # If the instance itself is the list of clusters (e.g. service already extracted from 'data')
        if isinstance(instance, list):
            # Use this if the service modifies the response to return just the list
            return super().to_representation({'data': instance})

        # Fallback or error if structure is unexpected
        # This might indicate the service layer needs to standardize its output
        # or the serializer needs to be more flexible.
        # For now, let's assume the view will handle pre-processing if service returns raw list.
        return {'data': []} # Default to empty data if structure is not recognized

class ErrorSerializer(serializers.Serializer):
    """
    Serializer for error responses.
    """
    error = serializers.CharField()
    details = serializers.CharField(required=False, allow_blank=True, allow_null=True)


# === Topic Serializers ===

class KafkaErrorSerializer(serializers.Serializer):
    """Serializer for KafkaError details."""
    code = serializers.IntegerField(required=False, allow_null=True)
    name = serializers.CharField(max_length=100, required=False, allow_null=True)
    str = serializers.CharField(required=False, allow_null=True)

class PartitionMetadataSerializer(serializers.Serializer):
    """Serializer for Kafka Topic Partition Metadata."""
    id = serializers.IntegerField()
    leader = serializers.IntegerField()
    replicas = serializers.ListField(child=serializers.IntegerField())
    isrs = serializers.ListField(child=serializers.IntegerField())
    # error = KafkaErrorSerializer(required=False, allow_null=True) # Partition specific error if any

class TopicSerializer(serializers.Serializer):
    """
    Serializer for a Kafka topic's metadata obtained from AdminClient.
    """
    name = serializers.CharField(max_length=255)
    partitions = PartitionMetadataSerializer(many=True)
    error = KafkaErrorSerializer(required=False, allow_null=True) # Topic-level error

class TopicListSerializer(serializers.Serializer):
    """
    Serializer for the response from the list_topics service,
    which returns a dictionary like {"data": [topic1, topic2]} or {"error": ...}.
    """
    data = TopicSerializer(many=True, required=False)
    error = serializers.CharField(required=False)
    details = serializers.CharField(required=False, allow_blank=True, allow_null=True)

    def validate(self, data):
        # Ensure that either 'data' or 'error' is present.
        if 'data' not in data and 'error' not in data:
            raise serializers.ValidationError("Response must include 'data' or 'error'.")
        if 'data' in data and 'error' in data:
            # This shouldn't happen if service layer is consistent, but good to check.
            raise serializers.ValidationError("Response cannot include both 'data' and 'error'.")
        return data

    def to_representation(self, instance):
        # If the instance is an error structure from the service like {"error": "...", "details": "..."}
        if "error" in instance and "data" not in instance:
            # Use ErrorSerializer to represent this structure if it matches,
            # otherwise, this serializer will handle 'error' and 'details' fields directly.
            # This assumes the 'details' field from service's error matches what ErrorSerializer expects or is a string.
            error_serializer = ErrorSerializer(data=instance)
            if error_serializer.is_valid():
                 return error_serializer.data
            # Fallback if it doesn't fit ErrorSerializer structure but has an error key
            return {"error": instance.get("error"), "details": str(instance.get("details"))}

        # If it's a success structure like {"data": [...]}
        if "data" in instance:
            return super().to_representation(instance)

        # Fallback for unexpected structures, though validate should catch some.
        return {"error": "Invalid topic list data structure.", "details": str(instance)}


# === Committed Offset Serializers ===

# KafkaErrorSerializer is already defined above and can be reused.

class CommittedOffsetSerializer(serializers.Serializer):
    """
    Serializer for a specific partition's committed offset data.
    OFFSET_INVALID (-1001) will be passed as is; frontend can interpret it.
    """
    partition = serializers.IntegerField()
    offset = serializers.IntegerField() # Can be -1001 (OFFSET_INVALID)
    metadata = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    error = KafkaErrorSerializer(required=False, allow_null=True) # For partition-specific errors during fetch

class CommittedOffsetListSerializer(serializers.Serializer):
    """
    Serializer for the response from the get_committed_offsets service.
    Handles both success ({"data": [...]}) and error ({"error": ...}) structures.
    """
    data = CommittedOffsetSerializer(many=True, required=False)
    error = serializers.CharField(required=False)
    # details can be a string or a structured dict (e.g. from KafkaException)
    details = serializers.JSONField(required=False, allow_null=True)


    def validate(self, data):
        if 'data' not in data and 'error' not in data:
            raise serializers.ValidationError("Response must include 'data' or 'error'.")
        if 'data' in data and 'error' in data:
            raise serializers.ValidationError("Response cannot include both 'data' and 'error'.")
        return data

    def to_representation(self, instance):
        # Handle error structure from the service
        if "error" in instance and "data" not in instance:
            # ErrorSerializer expects 'details' to be a simple string if provided.
            # The service might return a dict for 'details' from KafkaException.
            # We can pass it as is if ErrorSerializer's 'details' field can handle it,
            # or convert to string here.
            # For simplicity, let's ensure details is a string if using the basic ErrorSerializer.
            # However, our CommittedOffsetListSerializer has details as JSONField, so it's more flexible.

            # If we want to reuse the exact ErrorSerializer for the error part:
            # temp_instance_for_error_serializer = {
            #     "error": instance.get("error"),
            #     "details": str(instance.get("details")) # Ensure details is string for ErrorSerializer
            # }
            # error_serializer = ErrorSerializer(data=temp_instance_for_error_serializer)
            # if error_serializer.is_valid():
            #     return error_serializer.data
            # return {"error": instance.get("error"), "details": str(instance.get("details"))} # Fallback

            # Using this serializer's own error/details fields:
            return super().to_representation(instance)


        # Handle success structure
        if "data" in instance:
            return super().to_representation(instance)

        # Fallback for unexpected structures
        return {"error": "Invalid committed offsets data structure.", "details": str(instance)}


# === Metrics Serializers ===

class DatapointSerializer(serializers.Serializer):
    """
    Represents a single data point in a time series.
    The actual field name for timestamp might be 't' or 'timestamp' or 'ts'.
    The actual field name for value might be 'v' or 'value' or the metric name itself.
    Based on Confluent Cloud Metrics API examples, it's often "timestamp" and "value".
    Additional fields from group_by (like "metric.topic", "metric.partition") will also be present at this level.
    """
    timestamp = serializers.DateTimeField()
    value = serializers.FloatField()
    # Allow any other fields that might come from group_by clauses
    # For example, if grouped by metric.topic, that field will appear here.
    # Using allow_unknown=True in Meta or explicitly defining them if known is an option.
    # For simplicity, we rely on DRF's default behavior or explicit definition in MetricSeriesSerializer.

    # Let's add common group_by fields explicitly if they are expected at the datapoint level in the response
    # According to docs (e.g. query for consumer lag), grouped fields appear alongside timestamp & value.
    # Example: {"metric.consumer_group_id": "group_1", "metric.topic": "test_topic_1", "timestamp": "...", "value": 0.0}

    # These fields will be dynamically included by DRF if they exist in the input data
    # and are not explicitly defined here. If we want to validate them, they should be defined.
    # For now, keeping it simple to timestamp and value, assuming other fields are passed through.

class MetricResourceSerializer(serializers.Serializer):
    """
    Represents the resource labels associated with a metric series.
    This structure might vary depending on the metric.
    Example from docs: "resource.kafka.id": "lkc-xxxx"
    For consumer lag, it might be: "metric.topic": "topicA", "metric.partition": "0", "metric.consumer_group_id": "group1"
    This will be a flat dictionary of labels.
    """
    # This can be a flexible DictField or define specific common resource labels
    # For now, let's assume the service layer might flatten this into the MetricSeriesSerializer directly
    # or that datapoints themselves contain these labels if grouped.
    # The Confluent Metrics API response usually flattens these group_by labels into each data point object.
    # So, DatapointSerializer might need to be more flexible or these labels are part of the MetricSeries.
    # Let's assume the service shapes the data so that series are distinct by labels,
    # and datapoints just contain timestamp/value.
    # No, the examples show grouped fields at the same level as timestamp/value in the data array.
    # So, DatapointSerializer should be more dynamic or have these common fields.
    # I'll make DatapointSerializer more dynamic by just defining timestamp/value and letting others pass.
    pass # Not strictly needed if labels are part of each datapoint as per API response examples.


class MetricSeriesSerializer(serializers.Serializer):
    """
    Represents a series of data points for a specific metric and set of resource labels.
    The Confluent Cloud API response structure is typically a flat list of datapoints where
    each datapoint object contains the metric name (if querying multiple), timestamp, value,
    and any grouping labels.

    This serializer will adapt to the typical response from Confluent's /query endpoint,
    which is a list of datapoints. Each datapoint effectively represents a point in a series.
    The service layer will return a structure like:
    {
        "data": [
            {"timestamp": "...", "value": 123, "metric.topic": "A", "metric.partition": "0"},
            {"timestamp": "...", "value": 456, "metric.topic": "A", "metric.partition": "1"},
        ]
    }
    So, this serializer might just be DatapointSerializer(many=True) effectively.
    Let's redefine to handle the structure from `get_cluster_throughput` which is:
    {"data": {"received_bytes": [...datapoints...], "sent_bytes": [...datapoints...]}}
    Or for consumer_lag: {"data": [...datapoints...]} where datapoints include group labels.
    """
    # This serializer is tricky because the structure of "data" varies.
    # For consumer_lag, service returns: {"data": [datapoints_with_labels]}
    # For throughput, service returns: {"data": {"received_bytes": [datapoints], "sent_bytes": [datapoints]}}

    # Let's create a generic MetricDataPointSerializer that can have extra fields.
    # This means MetricSeriesSerializer might not be needed if the view directly uses
    # DatapointSerializer(many=True) for simple list responses.

    # For the throughput case (dictionary of metric lists):
    received_bytes = DatapointSerializer(many=True, required=False)
    sent_bytes = DatapointSerializer(many=True, required=False)
    # For the consumer_lag case (direct list of datapoints with labels):
    # This will be handled by a different top-level serializer if the structure is just a list.

    # Given the plan, MetricsResponseSerializer will wrap this.
    # If the service returns {"data": [list of datapoints with labels]}, then MetricsResponseSerializer
    # would have a field like `series_data = DatapointSerializer(many=True, required=False)`.
    # If the service returns {"data": {"metric_name1": [datapoints], "metric_name2": [datapoints]}},
    # then MetricsResponseSerializer would use this MetricSeriesSerializer for its 'data' field.

class MetricsResponseSerializer(serializers.Serializer):
    """
    General purpose serializer for metrics API responses from our service.
    It can handle two main structures from the service layer:
    1. For single metric queries (like consumer_lag): {"data": [datapoint_objects_with_labels]}
    2. For multi-metric queries (like throughput): {"data": {"metric_name1": [datapoints], "metric_name2": [datapoints]}}
    """
    # Structure 1: A list of datapoints, where each datapoint includes labels.
    # We can use allow_empty=True for data if needed.
    data_list = DatapointSerializer(many=True, required=False, source='data')

    # Structure 2: A dictionary of metric names to lists of datapoints.
    data_dict = MetricSeriesSerializer(required=False, source='data')

    error = serializers.CharField(required=False)
    details = serializers.JSONField(required=False, allow_null=True)

    def to_representation(self, instance):
        # instance is the raw dict from the service, e.g. {"data": ...} or {"error": ...}
        if "error" in instance:
            # Let ErrorSerializer (if defined and simple) or this handle it.
            # This serializer's error/details fields will pick it up.
            return super().to_representation(instance)

        if "data" in instance:
            data_content = instance["data"]
            if isinstance(data_content, list):
                # It's structure 1 (e.g., consumer lag)
                # We need to ensure only data_list is populated
                # This requires careful handling or distinct serializers.
                # A simple way: just pass 'instance' to super() and let DRF figure it out
                # based on 'source' and what's present.
                # However, DRF will try to validate both data_list and data_dict against instance['data'].
                # This will fail if instance['data'] is a list (for data_dict) or a dict (for data_list).

                # Simpler approach: have the view choose the serializer.
                # Or, this serializer becomes more of a dynamic pass-through.
                # For now, let's make this serializer very basic for the 'data' part if it's complex.
                # Option: make 'data' a flexible serializers.JSONField() here and let frontend parse.

                # Let's try to be explicit.
                # This to_representation needs to decide which field (data_list or data_dict) was intended.
                # This is usually bad practice for a single serializer.
                # It's better to have two different response serializers or have the view choose.

                # Given the plan, let's assume views will choose the right top-level serializer,
                # or the service will always return a consistent structure that one serializer can handle.
                # The current service `get_cluster_throughput` returns `{"data": {"metric_name": [...]}}`
                # and `get_consumer_lag` returns `{"data": [...]}` (a list).

                # Let's simplify MetricsResponseSerializer for now to handle ONE type of 'data'
                # and create another for the dict-of-lists case if needed by a view.
                # Or, the view for throughput can construct the response itself from multiple calls to a
                # simpler MetricSeriesSerializer.

                # For now, assume MetricsResponseSerializer is for list-based data primarily.
                # The MetricSeriesSerializer above is for the dict case.
                # The view for throughput will use MetricSeriesSerializer directly on the 'data' part.
                # The view for consumer_lag will use MetricsResponseSerializer where data_list is used.

                # Re-thinking: MetricsResponseSerializer should be the generic wrapper for success/error.
                # The 'data' field within it can be different.
                # Let's make 'data' a JSONField and the specific views/serializers can use more specific
                # serializers for their 'data' part if needed for strict validation/transformation.

                # Simplified:
                # self.fields['data'] = DatapointSerializer(many=True) if isinstance(data_content, list) else MetricSeriesSerializer()
                # This dynamic field change is not standard.

                # Let's assume the service output for `data` is always a list of "series",
                # where a series has a name/labels and a list of datapoints.
                # Confluent API returns flat list of datapoints with labels mixed in.
                # Our `DatapointSerializer` already handles this (timestamp, value + other dynamic fields).
                # So if `instance['data']` is a list of these, `data_list` (source='data') should work.

                # If `instance['data']` is a dict (like for throughput), `data_dict` (source='data') should work.
                # DRF will try to match `instance['data']` to `data_list` and `data_dict`. One will fail validation if wrong type.
                # This is not ideal.

                # Simplest for now:
                # The views will decide. ConsumerLagView will use a serializer that expects a list of datapoints.
                # ClusterThroughputView will use a serializer that expects a dict of metric_name:[datapoints].

                # So, this MetricsResponseSerializer is for the case where 'data' is a list of datapoints:
                # Used by ConsumerLagView.
                if isinstance(data_content, list):
                    return {"data": DatapointSerializer(data_content, many=True).data}
                elif isinstance(data_content, dict): # For throughput
                     return {"data": MetricSeriesSerializer(data_content).data}


        return super().to_representation(instance) # For error cases or if data is not list/dict

# We need a specific serializer for the throughput response structure if we want typed parsing.
class ThroughputDataSerializer(serializers.Serializer):
    received_bytes = DatapointSerializer(many=True, required=False)
    sent_bytes = DatapointSerializer(many=True, required=False)
    # Can add other specific throughput metrics here if fetched

class ThroughputResponseSerializer(serializers.Serializer):
    data = ThroughputDataSerializer(required=False)
    error = serializers.CharField(required=False)
    details = serializers.JSONField(required=False, allow_null=True) # Allow dict for details

    def to_representation(self, instance):
        if "error" in instance:
            # Let this serializer handle the error fields directly
            return {"error": instance.get("error"), "details": instance.get("details")}
        return super().to_representation(instance)


# General list-of-datapoints response (for consumer lag)
class GenericMetricListResponseSerializer(serializers.Serializer):
    data = DatapointSerializer(many=True, required=False)
    error = serializers.CharField(required=False)
    details = serializers.JSONField(required=False, allow_null=True)

    def to_representation(self, instance):
        if "error" in instance:
            return {"error": instance.get("error"), "details": instance.get("details")}
        return super().to_representation(instance)


# === Topic Detail Serializers ===

class ConfigEntrySerializer(serializers.Serializer):
    name = serializers.CharField()
    value = serializers.CharField(allow_null=True) # Value can be null
    source = serializers.CharField() # e.g., "DEFAULT_CONFIG", "DYNAMIC_TOPIC_CONFIG"
    is_sensitive = serializers.BooleanField()
    is_default = serializers.BooleanField()
    read_only = serializers.BooleanField()

class TopicDetailsSerializer(serializers.Serializer):
    """
    Serializer for comprehensive details of a single Kafka topic.
    """
    name = serializers.CharField()
    partitions = PartitionMetadataSerializer(many=True, required=False, default=list)
    # Config is represented as a dictionary of ConfigEntrySerializer in the service output for easier lookup.
    # For the API, it might be better as a list of objects if the key (config name) is already in the object.
    # The service returns: topic_details["config"] = { "config_name_A": {"name":"config_name_A", ...}, ... }
    # If we want a list:
    # config = ConfigEntrySerializer(many=True, required=False, default=list)
    # But then the service needs to format it as a list.
    # Let's keep it as a dict from name to config entry object for now, as it's more direct from service.
    config = serializers.DictField(
        child=ConfigEntrySerializer(),
        required=False,
        default=dict
    )
    error = serializers.CharField(required=False, allow_null=True, allow_blank=True) # For errors collected during fetching parts

class TopicDetailsResponseSerializer(serializers.Serializer):
    """
    Wraps the TopicDetailsSerializer or an error message.
    """
    data = TopicDetailsSerializer(required=False)
    error = serializers.CharField(required=False)
    details = serializers.JSONField(required=False, allow_null=True) # For structured error details

    def to_representation(self, instance):
        # If the service returned an error string directly (e.g. bootstrap server failure)
        if "error" in instance and "data" not in instance:
            return {"error": instance.get("error"), "details": instance.get("details")}
        # If service returned data (which might itself contain a partial error string)
        return super().to_representation(instance)
