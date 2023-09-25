# Hotel Booking Demand ETL and Testing

This repository contains an Apache Spark ETL (Extract, Transform, Load) script for processing hotel booking demand data and a set of unit tests for verifying the functionality of the script.

## Getting Started

Follow these instructions to set up and run the ETL script and tests on your local machine.

### Prerequisites

- Python 3.x
- Apache Spark 2.4.x or later
- PySpark

### Installation

1. Clone the repository to your local machine:

   ```shell
   git clone https://github.com/Shashwatpandey4/bookings-etl.git
   ```

2. Navigate to the project directory:

   ```shell
   cd hotel-booking-etl
   ```

3. Create a virtual environment:

   ```shell
   python -m venv venv
   ```

4. Activate the virtual environment:

   On Linux/macOS:
   ```shell
   source venv/bin/activate
   ```

   On Windows (PowerShell):
   ```shell
   .\venv\Scripts\Activate
   ```

5. Install the required packages from the `requirements.txt` file:

   ```shell
   pip install -r requirements.txt
   ```

### Running the ETL Script

To run the ETL script and process the hotel booking demand data, follow these steps:

1. Make sure you have Apache Spark installed on your system.

2. Run the script using `spark-submit`:

   ```shell
   spark-submit script.py
   ```

   This will execute the ETL script, which will read the data, perform transformations, and save the result as a Parquet file in the `output` directory.

### Running Unit Tests

To run the unit tests to verify the functionality of the ETL functions, follow these steps:

1. Ensure you have activated the virtual environment (if you created one).

2. Run the following command to execute the tests:

   ```shell
   pytest tests.py
   ```

   This will run the unit tests and display the test results in the terminal.

## Folder Structure

- `data/`: Contains the hotel booking demand dataset 
- `etl/`: Contains the ETL functions and the main ETL script (`script.py`).
- `tests/`: Contains unit tests for the ETL functions (`tests.py`).
- `output/`: The Parquet output directory where the processed data is saved.

