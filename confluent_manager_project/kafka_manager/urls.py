from django.urls import path
from .views import (
    ClusterListView,
    TopicListView,
    CommittedOffsetsView,
    ConsumerGroupLagView,
    ClusterThroughputView,
    TopicDetailView
)

app_name = 'kafka_manager'

urlpatterns = [
    # Cluster Management
    path('clusters/', ClusterListView.as_view(), name='cluster-list'),

    # Topic Management
    path('clusters/<str:cluster_id>/topics/', TopicListView.as_view(), name='topic-list'),
    path('clusters/<str:cluster_id>/topics/<str:topic_name>/', TopicDetailView.as_view(), name='topic-detail'),

    # Offset Management
    path('clusters/<str:cluster_id>/groups/<str:group_id>/topics/<str:topic_name>/committed-offsets/',
         CommittedOffsetsView.as_view(), name='committed-offsets-list'),

    # Metrics
    path('clusters/<str:cluster_id>/metrics/throughput/',
         ClusterThroughputView.as_view(), name='cluster-throughput-metrics'),
    path('clusters/<str:cluster_id>/groups/<str:group_id>/metrics/consumer-lag/',
         ConsumerGroupLagView.as_view(), name='consumer-lag-metrics'),
]
