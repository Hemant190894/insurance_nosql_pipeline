import logging
import logging.handlers
import os
from datetime import datetime, timedelta
import shutil

# --- Configuration Constants ---
LOG_DIR = "logs"
ARCHIVE_DIR = os.path.join(LOG_DIR, "archive")
LOG_FILENAME = f"simulator_{datetime.now().strftime('%Y-%m-%d')}.log"
LOG_FILEPATH = os.path.join(LOG_DIR, LOG_FILENAME)
LOG_LEVEL = logging.INFO # Set default logging level

def setup_logger():
    """
    Sets up a centralized logger that writes to a daily file and the console.
    
    Returns:
        logging.Logger: The configured logger instance.
    """
    # 1. Create Directories if they don't exist
    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(ARCHIVE_DIR, exist_ok=True)

    # 2. Configure Logger
    logger = logging.getLogger('InsuranceSimulatorLogger')
    logger.setLevel(LOG_LEVEL)

    # Prevent duplicate handlers if called multiple times
    if logger.hasHandlers():
        logger.handlers.clear()

    # 3. Define Log Format
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(module)s.%(funcName)s:%(lineno)d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 4. File Handler (writes logs to the daily file)
    file_handler = logging.FileHandler(LOG_FILEPATH, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # 5. Stream Handler (writes logs to the console)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    
    # Run cleanup immediately on setup
    cleanup_old_logs()

    return logger

def cleanup_old_logs(days_to_keep=7):
    """
    Archives log files older than a specified number of days (default 7).
    """
    cutoff_date = datetime.now() - timedelta(days=days_to_keep)
    
    for filename in os.listdir(LOG_DIR):
        if filename.endswith(".log"):
            filepath = os.path.join(LOG_DIR, filename)
            
            # Skip the currently active log file
            if filename == LOG_FILENAME:
                continue
                
            try:
                # Extract file creation time
                file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                
                if file_time < cutoff_date:
                    # Archive the old file
                    archive_path = os.path.join(ARCHIVE_DIR, filename)
                    shutil.move(filepath, archive_path)
                    print(f"Archived old log file: {filename}")
                    
            except Exception as e:
                # Log this cleanup error internally
                logging.getLogger('InsuranceSimulatorLogger').error(f"Error during log cleanup for {filename}: {e}")

# Call setup_logger once in the module to configure the basic logging 
# for any errors that might occur during module import/cleanup.
# The main application will call setup_logger() explicitly.
if __name__ == '__main__':
    # Example usage if this file is run directly
    temp_logger = setup_logger()
    temp_logger.info("Test log entry.")
    temp_logger.warning("Old logs should be archived now.")
    temp_logger.debug("This debug message should not appear if LOG_LEVEL is INFO.")
