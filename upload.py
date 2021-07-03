class Upload:

    def __init__(self, bigQuery, client) -> None:
        self.bigQuery = bigQuery
        self.client = client

    def ReadAndUpload(self, filename: str, datasetID: str, tableID: str, schema: list):
        
        dataset_ref = self.client.dataset(datasetID)
        table_ref = dataset_ref.table(tableID)
        job_config = self.bigQuery.LoadJobConfig(schema=schema)
        job_config.source_format = self.bigQuery.SourceFormat.CSV
        job_config.autodetect = True

        # load the csv into bigquery
        with open(filename, "rb") as source_file:
            job = self.client.load_table_from_file(source_file, table_ref, job_config=job_config)

        job.result()  # Waits for table load to complete.