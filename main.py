import apache_beam as beam
from apache_beam.options.pipeline_options import PipelinesOptions

pipeline_options = PipelinesOptions(argv=None) # Opcoes de pipeline
pipeline = beam.Pipeline(options=pipeline_options)