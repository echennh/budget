import os
import time


def delete_file_if_old(file_path: str='', days_old: int=1):
    """
    Check if a file was created within the last 24 hours and delete it if not.

    :param file_path: Path to the file
    :param days_old: How many days ago the file can have been created before it is considered "old"
    """
    # First check if the file exists
    if not os.path.exists(file_path):
        print(f"File Not Found, not proceeding to check if it is old because it does not exist.")
        return

    # Get the current time and the file creation time
    current_time = time.time()
    file_creation_time = os.path.getctime(file_path)

    # Calculate the time difference in seconds
    time_difference = current_time - file_creation_time

    # Check if the file is older than 24 hours (24 hours * 60 minutes * 60 seconds)
    if time_difference > days_old * 24 * 60 * 60:
        # Delete the file
        os.remove(file_path)
        print(f"File '{file_path}' was older than 24 hours and has been deleted.")
    else:
        print(f"File '{file_path}' was created within the last 24 hours.")

