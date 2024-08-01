import pandas as pd
from RazonLabs.config import Config
from datetime import datetime
import time
import duckdb
import great_expectations as ge


class RazonLabs():
    def __init__(self):
        self.INPUT_DIR = Config.INPUT_DIR
        self.TARGET_DIR = Config.TARGET_DIR
        self.context = ge.data_context.DataContext()
        self.suite_name = "etl_pipeline_suite"
        self.suite = self.context.add_or_update_expectation_suite(expectation_suite_name=self.suite_name)


    def extract_properties(self):
        try:
            sensors_meta = pd.read_csv(self.INPUT_DIR + 'Sensors.csv')
            machines_meta = pd.read_csv(self.INPUT_DIR + 'Machines.csv')
            sensor_data_20240101 = pd.read_csv(self.INPUT_DIR + '2024-01-01.csv')
            sensor_data_20240102 = pd.read_csv(self.INPUT_DIR + '2024-01-02.csv')

            # Add Expectations to the extracted data
            self.validate_data(sensors_meta, "sensors_meta")
            self.validate_data(machines_meta, "machines_meta")
            self.validate_data(sensor_data_20240101, "sensor_data_20240101")
            self.validate_data(sensor_data_20240102, "sensor_data_20240102")

            return sensors_meta, machines_meta, sensor_data_20240101, sensor_data_20240102
        except Exception as e:
            print(f"Error during data extraction: {str(e)}")
            raise



    def validate_data(self, df, data_asset_name):
        # Convert pandas DataFrame to a Great Expectations DataFrame
        ge_df = ge.from_pandas(df)

        # Example expectations - these can be customized based on your data schema
        ge_df.expect_column_to_exist('tag_name')


        if data_asset_name == "sensors_meta":
            ge_df.expect_column_values_to_be_unique('tag_name')
        elif data_asset_name in ["sensor_data_20240101", "sensor_data_20240102"]:
            ge_df.expect_column_values_to_be_of_type('value', 'float64')

        # Apply the type expectation only to sensor data files
        if data_asset_name in ["sensor_data_20240101", "sensor_data_20240102"]:
            ge_df.expect_column_values_to_be_of_type('value', 'float64')


        # Validate the data against the expectation suite
        validation_result = ge_df.validate(expectation_suite=self.suite)


        # Check if validation passed or failed
        if not validation_result.success:
            print(f"Validation failed for {data_asset_name}")
            print(validation_result)
            raise ValueError(f"Data validation failed for {data_asset_name}")
        else:
            print(f"Validation passed for {data_asset_name}")



    def transform_properties(self, sensors_meta, machines_meta, sensor_data_20240101, sensor_data_20240102, from_date, to_date):

        try:
            sensor_data = pd.concat([sensor_data_20240101, sensor_data_20240102])
            merged_sensor_data_with_sensors_meta = pd.merge(sensor_data, sensors_meta, on='tag_name')
            merged_with_machines_data = pd.merge(merged_sensor_data_with_sensors_meta, machines_meta, on='machine_code')

            merged_with_machines_data['sample_time'] = merged_with_machines_data['timestamp'].apply(
                lambda x: int(time.mktime(datetime.strptime(x, "%Y-%m-%dT%H:%M:%S").timetuple()) * 1e6))

            merged_with_machines_data['value'] = pd.to_numeric(merged_with_machines_data['value'], errors='coerce')
            merged_with_machines_data.fillna(0, inplace=True)
            current_timestamp = datetime.now().isoformat()

            merged_with_machines_data['inserted_at'] = current_timestamp
            ordered_columns = [
                'machine_code','timestamp', 'machine_name', 'component_code',
                'coordinate', 'sample_time', 'value', 'inserted_at'
            ]

            result = merged_with_machines_data[ordered_columns]

            # Save the result as a Parquet file (optional, as it was previously)
            result.to_parquet(self.TARGET_DIR + 'processed_data.parquet', index=False)

            # Convert to a DuckDB table
            con = duckdb.connect(database=':memory:')
            con.execute("CREATE TABLE processed_data AS SELECT * FROM result")

            # Run the SQL query on DuckDB
            query = f"""
            WITH daily_avg AS (
                SELECT
                    machine_name,
                    coordinate,
                    CAST(timestamp AS DATE) AS date,
                    AVG(value) AS value_avg,
                    LAG(value_avg) OVER (PARTITION BY machine_name, coordinate ORDER BY date) AS previous_day_value_avg,
                    COUNT(*) AS samples_cnt
                FROM 
                    processed_data
                WHERE date BETWEEN '{from_date}' AND '{to_date}'
                GROUP BY 
                    machine_name, coordinate, date
            ),

            daily_avg_comparison AS (
                SELECT 
                    machine_name,
                    coordinate,
                    value_avg,
                    (value_avg - previous_day_value_avg) AS increase_in_value,
                    samples_cnt
                FROM 
                    daily_avg
                WHERE previous_day_value_avg IS NOT NULL    
            )

            SELECT 
                machine_name,
                coordinate,
                value_avg,
                increase_in_value,
                samples_cnt
            FROM 
                daily_avg_comparison
            QUALIFY ROW_NUMBER() OVER (PARTITION BY machine_name ORDER BY increase_in_value DESC) = 1;
            """

            query_result = con.execute(query).fetchdf()

            # Print the query result for debugging
            print(query_result)

            # Save the result as a CSV file
            query_result.to_csv(self.TARGET_DIR + 'increase_report.csv', index=False)



            return query_result

        except Exception as e:
            print(f"Error during data extraction: {str(e)}")
            raise

    def validate_query_result(self, query_result):
        # Convert pandas DataFrame to a Great Expectations DataFrame
        ge_df = ge.from_pandas(query_result)


        # Add expectations
        ge_df.expect_column_values_to_be_unique('machine_name')
        ge_df.expect_column_values_to_not_be_null('value_avg')
        ge_df.expect_column_values_to_not_be_null('machine_name')

        # Validate the data against the expectation suite
        validation_result = ge_df.validate(expectation_suite=self.suite)

        # Check if validation passed or failed
        if not validation_result.success:
            print("Validation failed for query_result")
            print(validation_result)
            raise ValueError("Query result validation failed")
        else:
            print("Validation passed for query_result")


    def run_etl_pipeline(self, from_date, to_date):
        sensors_meta, machines_meta, sensor_data_20240101, sensor_data_20240102 = self.extract_properties()
        result = self.transform_properties(sensors_meta, machines_meta, sensor_data_20240101, sensor_data_20240102, from_date, to_date)
        self.validate_query_result(result)  # Validate the query result
        # Additional loading or processing can go here

if __name__ == "__main__":
    razonlabs_instance = RazonLabs()
    razonlabs_instance.run_etl_pipeline('2024-01-01', '2024-01-02')
