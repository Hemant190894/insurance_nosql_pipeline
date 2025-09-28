from faker import Faker
import random
import time
import pymongo
import os
from load_dotenv import load_dotenv
from datetime import datetime
from logger_utility import setup_logger # Import the new logging utility

# Initialize Faker for synthetic data generation
fake = Faker()
# Global logger instance (will be initialized in __main__)
logger = None 

def load_configuration():
    """
    Loads environment variables from the .env file and performs basic validation.
    
    Returns:
        tuple: (mongo_uri, sleep_time, db_name)
    """
    load_dotenv()
    
    mongo_uri = os.getenv("MONGO_URI")
    # Default database name is used if MONGO_DB_NAME is not set in .env
    db_name = os.getenv("MONGO_DB_NAME", "insurance-pipeline") 
    
    try:
        # Default sleep time is 5 seconds if not set or invalid
        sleep_time = int(os.getenv("SLEEP_TIME", 5))
    except ValueError:
        logger.warning("SLEEP_TIME in .env is not an integer. Defaulting to 5 seconds.")
        sleep_time = 5
        
    if not mongo_uri:
        logger.critical("MONGO_URI environment variable is not set. Please check your .env file.")
        raise ValueError("MONGO_URI environment variable is not set.")
        
    logger.info("Configuration loaded successfully.")
    logger.debug(f"DB Name: {db_name}, Sleep Time: {sleep_time}s")
    return mongo_uri, sleep_time, db_name

def setup_mongo_connection(mongo_uri, db_name):
    """
    Connects to MongoDB Atlas and returns the database object and collection objects.
    
    Args:
        mongo_uri (str): The full MongoDB connection string.
        db_name (str): The name of the database to connect to.
        
    Returns:
        tuple: (customers_collection, claims_collection)
    """
    logger.info("Attempting to connect to MongoDB Atlas...")
    try:
        # Use server selection timeout to fail fast if connection cannot be established
        client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        
        # The ismaster command is a quick way to verify connection and authentication
        client.admin.command('ismaster') 
        logger.info("Successfully connected to MongoDB Atlas.")
        
        db = client[db_name]
        customers_col = db["customers"]
        claims_col = db["claims"]
        
        return customers_col, claims_col
        
    except pymongo.errors.ConnectionFailure as e:
        logger.critical(f"Connection error: Could not connect to MongoDB Atlas. Check your MONGO_URI and network status. Error: {e}")
        raise e
    except Exception as e:
        logger.critical(f"An unexpected critical error occurred during MongoDB connection: {e}")
        raise e

def generate_insurance_data():
    """
    Generates a new, simulated customer record and an associated claim record.
    
    Returns:
        tuple: (customer_dict, claim_dict)
    """
    customer_id = f"CUST-{random.randint(100, 999)}"
    
    customer = {
        "customer_id": customer_id,
        "name": fake.name(),
        "state": fake.state_abbr(),
        "policy_type": random.choice(["Auto", "Home", "Health"]),
        "timestamp": datetime.now() # Add current server timestamp for context
    }

    claim = {
        "claim_id": f"CLM-{random.randint(1000, 9999)}",
        "customer_id": customer_id, # Link back to the customer
        "date": fake.date_between(start_date='-1y', end_date='today').isoformat(),
        "amount": round(random.uniform(100, 20000), 2),
        "claim_type": random.choice(["Accident", "Theft", "Fire", "Liability"]),
        "is_fraud": random.random() < 0.05, # 5% chance of being fraud
        "status": "Submitted" # Initial status
    }
    
    logger.debug(f"Generated data for CUST:{customer_id} and CLM:{claim['claim_id']}")
    return customer, claim

def run_simulation(customers_col, claims_col, sleep_time):
    """
    Runs the infinite loop to generate and insert data into MongoDB.
    """
    logger.info(f"Starting real-time simulation. Inserting data every {sleep_time} seconds...")
    try:
        while True:
            # 1. Generate Data
            customer, claim = generate_insurance_data()

            # 2. Insert into MongoDB
            try:
                # Insert customer first
                customers_col.insert_one(customer)
                claims_col.insert_one(claim)
                
                # 3. Log success
                logger.info(f"-> Inserted claim {claim['claim_id']} for customer {customer['customer_id']} ({customer['policy_type']})")

                # 4. Calculate and log the current counts in both collections
                customer_count = customers_col.count_documents({})
                claim_count = claims_col.count_documents({})
                
                logger.info(f"Customer Count == {customer_count}")
                logger.info(f"Claim Count == {claim_count}")

            except pymongo.errors.DuplicateKeyError as dke:
                logger.warning(f"Duplicate key error detected (likely customer ID collision). Skipping insertion. Error: {dke}")
            except pymongo.errors.WriteConcernError as wce:
                logger.error(f"MongoDB Write Concern Error: Data insertion might not be fully confirmed. Error: {wce}")
            except Exception as insert_e:
                logger.error(f"Failed to insert data into MongoDB due to an unknown error: {insert_e}")


            # 5. Wait for the next cycle
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        logger.info("Simulation stopped by user (Ctrl+C). Exiting.")
    except Exception as e:
        logger.critical(f"A critical error occurred during the simulation loop: {e}")


if __name__ == "__main__":
    # 1. Initialize Logger (This also triggers the log file cleanup)
    logger = setup_logger()
    logger.info("--- Insurance Data Simulator Application Starting ---")
    
    try:
        # 2. Load configuration
        mongo_uri, sleep_time, db_name = load_configuration()
        
        # 3. Set up connection
        customers_col, claims_col = setup_mongo_connection(mongo_uri, db_name)
        
        # 4. Run the loop
        run_simulation(customers_col, claims_col, sleep_time)
        
    except ValueError as ve:
        # Catch configuration errors and log them as critical
        logger.critical(f"Application terminated due to Configuration Error: {ve}")
    except pymongo.errors.ConnectionFailure:
        # Catch connection failures (setup_mongo_connection already logged the critical details)
        logger.critical("Application terminated due to MongoDB connection failure.")
    except Exception as e:
        logger.critical(f"A fatal and unhandled error occurred during application setup: {e}")
    finally:
        logger.info("--- Insurance Data Simulator Application Terminated ---")
