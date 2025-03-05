# Description: Test sending an email notification using the NotificationManager class.
#
# To run this script, you need to set the environment variable ieasyhydroforecast_env_file_path to the path of your .env file.
# You can do this by running the following command in the terminal (adapt path to your .env):
# ieasyhydroforecast_env_file_path=/path/to/your/.env python apps/pipeline/tests/test_email_notification.py

import os
import sys

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.notification_manager import NotificationManager
from src.environment import Environment

# Load environment variables
# Initialize the Environment class with the path to your .env file
env_file_path = os.getenv('ieasyhydroforecast_env_file_path')
env = Environment(env_file_path)

# Test sending an email
NotificationManager.send_email(
    recipients=["marti@hydrosolutions.ch"],
    subject="Test Email from Forecast System",
    message="This is a test email to verify the notification system is working."
)