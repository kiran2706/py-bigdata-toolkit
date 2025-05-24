# Standard Library Imports
import logging
from datetime import datetime

# Third-Party Imports
import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    GoogleCloudOptions,
    StandardOptions,
    WorkerOptions,
    SetupOptions,
    DebugOptions,
)

# Local Imports
from dataflow.datastore_mass_deletion.custom_transforms.custom_datastore_io import (
    DatastoreQueryBuilder,
    DatastoreEntityReader,
    DatastoreEntityBatchDeleter,
)
from helper_functions.loggers.logging_utils import configure_logging

# -----------------------------------------------------------------------------
# Configuration Section
# -----------------------------------------------------------------------------
DATASTORE_CONFIG = {
    "PROJECT_ID": "gcp-project-id",
    "DATAFLOW_BUCKET_NAME": "dataflow-bucket-name",
    "REGION": "location",
    "RUNNER": "DataflowRunner",  # Use "DirectRunner" for local testing
}

# -----------------------------------------------------------------------------
# Logging Setup
# -----------------------------------------------------------------------------
configure_logging(service_name="datastore-mass-deletion-dataflow", level="WARNING")
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Pipeline Options Definition
# -----------------------------------------------------------------------------
class DatastoreDeletionOptions(PipelineOptions):
    """
    Custom pipeline options for Datastore Mass Deletion.

    Extends PipelineOptions to include specific arguments for:
      - datastore_kind: The kind of Datastore entities to delete.
      - datastore_project: The GCP project containing the Datastore.
      - query_filter: Optional filter for targeting specific entities.
      - custom_filter_column: Optional column for additional filtering.
    """

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            "--datastore_kind",
            type=str,
            help="Datastore kind to delete",
            default="Jobs",
        )
        parser.add_value_provider_argument(
            "--datastore_project",
            type=str,
            help="GCP project name",
            default="rankings-microservice-qa",
        )
        parser.add_value_provider_argument(
            "--query_filter",
            type=str,
            help="Datastore GQL query filter (optional)",
            default="ready_to_delete=True",
        )
        parser.add_value_provider_argument(
            "--custom_filter_column",
            type=str,
            help="Custom filter column for additional filtering (optional)",
            default="False",
        )

# -----------------------------------------------------------------------------
# Pipeline Options Initialization
# -----------------------------------------------------------------------------
pipeline_options = PipelineOptions()
gcloud_options = pipeline_options.view_as(GoogleCloudOptions)
gcloud_options.job_name = f"mass-deletion-datastore-v{datetime.utcnow():%Y%m%d-%H%M%S}"
gcloud_options.project = DATASTORE_CONFIG["PROJECT_ID"]
gcloud_options.staging_location = f"gs://{DATASTORE_CONFIG['DATAFLOW_BUCKET_NAME']}/staging"
gcloud_options.temp_location = f"gs://{DATASTORE_CONFIG['DATAFLOW_BUCKET_NAME']}/temp"
gcloud_options.region = DATASTORE_CONFIG["REGION"]
gcloud_options.dataflow_service_options = ["enable_prime"]

pipeline_options.view_as(StandardOptions).runner = DATASTORE_CONFIG["RUNNER"]
pipeline_options.view_as(StandardOptions).streaming = False
pipeline_options.view_as(DebugOptions).number_of_worker_harness_threads = 2
pipeline_options.view_as(WorkerOptions).max_num_workers = 4
pipeline_options.view_as(WorkerOptions).autoscaling_algorithm = "THROUGHPUT_BASED"
pipeline_options.view_as(DebugOptions).experiments = [
    "enable_batch_vmr",
    "enable_vertical_memory_autoscaling",
    "auto_use_sibling_sdk_workers=False",
    "use_runner_v2",
]
pipeline_options.view_as(SetupOptions).save_main_session = True

dataflow_options = pipeline_options.view_as(DatastoreDeletionOptions)

# -----------------------------------------------------------------------------
# Pipeline Definition
# -----------------------------------------------------------------------------
def run() -> None:
    """
    Main pipeline execution function.

    This pipeline performs the following steps:
      1. Reads entities from Cloud Datastore using a query.
      2. Optionally filters entities based on custom criteria.
      3. Extracts entity keys for deletion.
      4. Deletes entities in batches.
      5. Logs deletion statistics.

    Runtime parameters are provided via ValueProvider arguments for flexibility.
    """
    with beam.Pipeline(options=dataflow_options) as pipeline:
        # Access pipeline options
        options = dataflow_options

        # Step 1: Read from Datastore using a query
        datastore_query = (
            pipeline
            | "CreateSeed" >> beam.Create([None])  # Create a seed element to start the pipeline
            | "BuildQuery" >> beam.ParDo(
                DatastoreQueryBuilder(
                    options.datastore_project,
                    options.datastore_kind,
                    options.query_filter,
                )
            )
        )
        entities = datastore_query | "ReadEntities" >> DatastoreEntityReader()

        # Step 2: Filter entities (optional, based on custom logic)
        # TO:DO Implement the filter function to handle custom filtering logic
        filtered_entities = (
            entities
            | "FilterEntities" >> beam.Filter(
                filter_function,
                custom_filter_column_vp=options.custom_filter_column,
                cutoff_date_vp=options.cutoff_date,
            )
        )

        # Step 3: Extract keys and batch for deletion
        batched_keys = (
            filtered_entities
            | "ExtractKeys" >> beam.Map(lambda entity: entity.key)
            | "BatchKeys" >> beam.BatchElements(min_batch_size=20000, max_batch_size=20000)
        )

        # Step 4: Delete entities in batches
        deletion_results = (
            batched_keys
            | "DeleteKeyBatches" >> beam.ParDo(DatastoreEntityBatchDeleter(options.datastore_project))
        )

        # Step 5: Log deletion statistics
        _ = (
            deletion_results
            | "CountDeleted" >> beam.CombineGlobally(sum)
            | "LogDeletionStats" >> beam.Map(lambda count: logger.info(f"Total entities deleted: {count}"))
        )
