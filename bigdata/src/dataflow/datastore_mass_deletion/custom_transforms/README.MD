# Custom Datastore Transforms

This directory contains custom Apache Beam transforms for working with Google Cloud Datastore.

## CustomReadFromDatastore

A custom PTransform for reading entities from Google Cloud Datastore using a PCollection of queries. This transform is particularly useful when you need to read entities from Datastore based on dynamically generated queries.

### Features

- Supports parallel processing through query splitting
- Compatible with Apache Beam's type system
- Integrates with Google Cloud Datastore v1 API
- Supports automatic or manual query splitting

### Usage

```python
from apache_beam import beam
from apache_beam.options.pipeline_options import PipelineOptions
from custom_datastore_io import CustomReadFromDatastore

# Define your pipeline options
class DatastoreOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--datastore_project', type=str)
        parser.add_value_provider_argument('--datastore_kind', type=str)
        parser.add_value_provider_argument('--query_filter', type=str)

# Parse pipeline options
options = PipelineOptions().view_as(DatastoreOptions)

# Create a PCollection of queries
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
```

### Pipeline Options

The example uses Apache Beam's PipelineOptions to configure the Datastore connection parameters:

- `datastore_project`: The Google Cloud project ID containing your Datastore
- `datastore_kind`: The kind (entity type) to query
- `query_filter`: The filter to apply to the query

You can pass these options when running your pipeline:

```bash
python your_pipeline.py \
    --datastore_project=your-project-id \
    --datastore_kind=your-entity-kind \
    --query_filter="property = value"
```

### Parameters

- `num_splits` (int, optional): Number of splits for parallel processing. Defaults to 0 (automatic splitting).

### Implementation Details

The transform works in three main steps:
1. Splits the input queries for parallel processing
2. Reshuffles the data for better distribution
3. Reads the entities from Datastore using the split queries

### Requirements

- Apache Beam
- Google Cloud Datastore v1 API
- Python 3.x

### Notes

- The transform uses Apache Beam's type hints for better type safety
- The number of splits can be automatically determined by setting `num_splits=0`
- The transform is designed to work with the Google Cloud Datastore v1 API
