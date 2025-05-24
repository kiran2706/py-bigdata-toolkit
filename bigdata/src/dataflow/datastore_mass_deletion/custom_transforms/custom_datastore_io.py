# Standard Library Imports
import logging
import time
from typing import List, Tuple, Any

# Third-Party Imports
import apache_beam as beam
from apache_beam import typehints
from apache_beam.io.gcp.datastore.v1new.datastoreio import ReadFromDatastore
from apache_beam.io.gcp.datastore.v1new.types import Query, Entity
from apache_beam.transforms import ParDo, PTransform, Reshuffle
from google.cloud import datastore

# Local Imports
from helper_functions.loggers.logging_utils import configure_logging

# Configure logging
configure_logging(name="datastore-mass-deletion-dataflow", level="WARNING")
logger = logging.getLogger(__name__)


@typehints.with_input_types(Query)
class DatastoreQueryBuilder(beam.DoFn):
    """
    Builds a Datastore query based on ValueProvider options.
    """

    def __init__(
        self,
        datastore_project_vp: beam.value.ValueProvider,
        datastore_kind_vp: beam.value.ValueProvider,
        query_filter_vp: beam.value.ValueProvider,
    ) -> None:
        """
        Initializes the DatastoreQueryBuilder DoFn.

        Args:
            datastore_project_vp: ValueProvider for the Datastore project ID.
            datastore_kind_vp: ValueProvider for the Datastore kind.
            query_filter_vp: ValueProvider for the query filter string.
        """
        self.datastore_project_vp = datastore_project_vp
        self.datastore_kind_vp = datastore_kind_vp
        self.query_filter_vp = query_filter_vp

    def process(self, element: Any) -> List[Query]:
        """
        Processes an element to construct and yield a Datastore query.

        Args:
            element: The input element (unused, serves as a trigger).

        Yields:
            A Datastore query object.
        """
        datastore_project: str = self.datastore_project_vp.get()
        datastore_kind: str = self.datastore_kind_vp.get()
        query_filter: str = self.query_filter_vp.get()

        # Parse the query filter string into a list of filter tuples
        if query_filter == "False":
            query_filter_value: Tuple = ()
        else:
            filtered_string: List[str] = query_filter.split("mm")
            query_filter_value: List[Tuple[str, str, str]] = [
                (first, "=", second)
                for item in filtered_string
                for first, second in [item.split("=")]
            ]

        # Create Datastore query with the specified parameters
        query: Query = Query(
            kind=datastore_kind,
            project=datastore_project,
            filters=query_filter_value,
        )

        # Log pipeline parameters for debugging
        logger.info(
            "Dataflow pipeline started with the following parameters",
            extra={
                "datastore_project": datastore_project,
                "datastore_kind": datastore_kind,
                "query_filter": query_filter_value,
            },
        )
        yield query


@typehints.with_output_types(Entity)
class DatastoreEntityReader(PTransform):
    """
    A PTransform for reading entities from Google Cloud Datastore using a PCollection of queries.

    This transform takes a PCollection of Datastore queries as input and outputs a PCollection
    of Datastore entities that match those queries.

    Usage example:
        # Create a PCollection of queries (one or more)
        datastore_query = (
            pipeline
            | "CreateSeed" >> beam.Create([None])
            | "BuildQuery" >> beam.ParDo(
                DatastoreQueryBuilder(
                    options.datastore_project,
                    options.datastore_kind,
                    options.query_filter,
                )
            )
        )

        # Read entities matching the queries
        entities = datastore_query | "ReadEntities" >> DatastoreEntityReader(num_splits=10)

    Args:
        num_splits: The number of splits for parallel processing.
    """

    def __init__(self, num_splits: int = 0) -> None:
        """
        Initializes the DatastoreEntityReader PTransform.

        Args:
            num_splits: Number of splits for parallel processing. Defaults to 0 (automatic).
        """
        super().__init__()
        self._num_splits = num_splits

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        """
        Expands the input PCollection of queries into a PCollection of Datastore entities.

        Args:
            pcoll: A PCollection of Datastore query objects.

        Returns:
            A PCollection of Datastore entity objects.
        """
        return (
            pcoll
            | "SplitQuery" >> ParDo(ReadFromDatastore._SplitQueryFn(self._num_splits))
            | Reshuffle()
            | "Read" >> ParDo(ReadFromDatastore._QueryFn())
        )


class DatastoreEntityBatchDeleter(beam.DoFn):
    """
    Deletes entities from Datastore in batches with retry logic.
    """

    def __init__(self, project_id: beam.value.ValueProvider, batch_size: int = 500, max_retries: int = 5) -> None:
        """
        Initializes the DatastoreEntityBatchDeleter DoFn.

        Args:
            project_id: The Google Cloud project ID.
            batch_size: The number of entities to delete in each batch.
            max_retries: The maximum number of times to retry a failed deletion.
        """
        self.project_id = project_id
        self.batch_size = batch_size
        self.max_retries = max_retries

    def setup(self) -> None:
        """
        Sets up the Datastore client.
        """
        self.client = datastore.Client(project=self.project_id.get())

    def process(self, key_batch: List[datastore.Key]) -> Any:
        """
        Processes a batch of entity keys for deletion.

        Args:
            key_batch: A list of Datastore entity keys to delete.

        Yields:
            The number of deleted keys for monitoring, or error information if deletion fails.
        """
        # Convert IO keys to datastore keys
        try:
            datastore_keys: List[datastore.Key] = [key.to_client_key() for key in key_batch]
        except AttributeError:
            datastore_keys: List[datastore.Key] = [key for key in key_batch]

        attempt: int = 0
        delay: float = 1.0  # Start with a 1-second delay

        while attempt < self.max_retries:
            try:
                self.client.delete_multi(datastore_keys)
                yield len(datastore_keys)  # Return number of deleted keys for monitoring
                return  # Exit the function after successful deletion
            except Exception as e:
                if "409" in str(e):  # Detect contention error
                    logging.warning(
                        f"Contention error on delete, attempt {attempt+1}/{self.max_retries}: {str(e)}"
                    )
                    attempt += 1
                    time.sleep(delay)
                    delay *= 2  # Exponential backoff
                else:
                    # For other errors, log and break out
                    logging.error(f"Error deleting batch: {str(e)}")
                    yield beam.pvalue.TaggedOutput("errors", (datastore_keys, str(e)))
                    return  # Exit the function after a non-retryable error

        # Only reach here if we exhausted retries
        logging.error(
            f"Batch deletion failed after {self.max_retries} retries for keys: {datastore_keys}"
        )
        yield beam.pvalue.TaggedOutput("errors", (datastore_keys, "Max retries exceeded"))
