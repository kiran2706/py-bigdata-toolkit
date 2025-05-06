from apache_beam import typehints
from apache_beam.io.gcp.datastore.v1new import types
from apache_beam.transforms import ParDo, PTransform, Reshuffle
from apache_beam.io.gcp.datastore.v1new.datastoreio import ReadFromDatastore


@typehints.with_output_types(types.Entity)
class CustomReadFromDatastore(PTransform):
    """
    A PTransform for reading entities from Google Cloud Datastore using a PCollection of queries.

    This transform is designed to be used in an Apache Beam pipeline where you first create or
    generate Datastore queries (for example, using a DoFn like CreateDatastoreQuery), and then
    pass those queries to this transform to read the corresponding entities from Datastore.

    Usage example:
        # Create a PCollection of queries (one or more)
        datastore_query = (
            pipeline
            | beam.Create([None])
            | beam.ParDo(
                CreateDatastoreQuery(
                    options.datastore_project,
                    options.datastore_kind,
                    options.query_filter,
                )
            )
        )

        # Read entities matching the queries
        entities = datastore_query | "ReadQuery" >> CustomReadFromDatastore(num_splits=10)

    Args:
        num_splits (int, optional): Number of splits for parallel processing. Defaults to 0 (automatic).
    """

    def __init__(self, num_splits=0):
        super().__init__()
        self._num_splits = num_splits

    def expand(self, pcoll):
        return (
            pcoll
            | "SplitQuery" >> ParDo(ReadFromDatastore._SplitQueryFn(self._num_splits))
            | Reshuffle()
            | "Read" >> ParDo(ReadFromDatastore._QueryFn())
        )