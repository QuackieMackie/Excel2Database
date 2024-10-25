from utils.config_util import load_config
from utils.logging_util import setup_logger
from utils.db_util import get_db_connection
from dotenv import load_dotenv
from utils.main_util import import_tasks

import tasks

def main():
    load_dotenv()

    config = load_config()
    log_file = config['LOGGING']['log_dir']
    logger = setup_logger(log_file)

    task_modules = import_tasks(tasks)

    connection = None
    try:
        connection = get_db_connection(config)
        # Call the function for each task module loaded
        for module_name, module in task_modules.items():
            task_func = getattr(module, module_name, None)
            if callable(task_func):
                task_func(connection, config, logger)
        logger.info("Database update completed successfully. Please restart the server for the changes to take effect.")
    except Exception as e:
        logger.error(f"Error in main process: {e}")
    finally:
        if connection:
            connection.close()
            logger.info("Database connection closed.")


if __name__ == "__main__":
    main()
