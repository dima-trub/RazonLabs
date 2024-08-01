# main.py
import warnings
from RazonLabs.etl import RazonLabs
def main():
    # Create an instance of the RazonLabs class
    warnings.filterwarnings("ignore", category=UserWarning)
    razonlabs_instance = RazonLabs()

    from_date = '2024-01-01'
    to_date = '2024-01-02'
    try:
        # Call the ETL pipeline
        razonlabs_instance.run_etl_pipeline(from_date, to_date)
        print("ETL pipeline executed successfully.")
    except Exception as e:
        print(f"Error during ETL pipeline execution: {str(e)}")

if __name__ == "__main__":
    # Execute the main function
    main()