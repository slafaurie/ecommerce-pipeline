from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from common.data_model import DataModel


class ETLOperator(BaseOperator):
    # it looks like start_date is a reserved in airflow. dont use it
    # Templates render in the pre-execute method on the base operator after the class is instatiated
    # that's the reason why assignment the variables to partition dates returns the template as string and not the actual value
    template_fields = ("_start","_end")

    @apply_defaults
    def __init__(self, sources_dict, output_params, transformer_callable, start, end, **kwargs ):
        super().__init__(**kwargs)
        self.sources_dict = sources_dict
        self.output_params = output_params
        self.transformer_callable = transformer_callable
        self._start = start
        self._end = end

    def add_partition_dates_to_partitioned_tables(self):
        sources_with_partition = [source + [[self._start, self._end]] for source in self.sources_dict.get("partitioned")]
        self.sources_dict["partitioned"] = sources_with_partition

    def execute(self, context):
        self.log.info("Start loading process...")
        self.log.info(f"Window to process {[self._start, self._end]}")


        # Load
        self.add_partition_dates_to_partitioned_tables()
        load_partitioned_sources = [DataModel.read_partitioned_dataframe(*inputs) for inputs in self.sources_dict.get("partitioned")]
        load_non_partitioned_sources = [DataModel.read_dataframe(*inputs) for inputs in self.sources_dict.get("full_tables")]
        all_sources = load_partitioned_sources + load_non_partitioned_sources

        # transform
        self.log.info("Start transformation process...")
        output = self.transformer_callable.curate_sources(*all_sources)

        # save
        if output.empty:
            self.log.info("No data to process")
        else:
            DataModel.write_partitioned_dataframe(output, *self.output_params)
        self.log.info("Curation done")
