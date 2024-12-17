# auth_utils.py
# Custom authentication utilities for the iEasyForecast application written by
# Vjekoslav Večković.
import json
import os
import csv
import pandas as pd
from datetime import datetime

# Get credentials path from environment configuration
CREDENTIALS_PATH = os.getenv('ieasyforecast_configuration_path_credentials')


CURRENT_USER_PATH = "current_user.csv"
ACTIVITY_LOG_PATH = 'user_activity.csv'
AUTH_LOGS_PATH = "auth_logs.csv"


def load_credentials():
    """Load credentials from JSON file."""
    try:
        with open(CREDENTIALS_PATH, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Credentials file not found at: {CREDENTIALS_PATH}")
        return {}
    except Exception as e:
        print(f"Error loading credentials: {e}")
        return {}

def check_current_user():
    """Check if there's a user currently logged in."""
    if os.path.exists(CURRENT_USER_PATH):
        df = pd.read_csv(CURRENT_USER_PATH)
        return df['username'].iloc[0] if not df.empty else None
    return None

def save_current_user(username):
    """Save current user to CSV."""
    pd.DataFrame({'username': [username]}).to_csv(CURRENT_USER_PATH, index=False)

def remove_current_user():
    """Remove current user file."""
    if os.path.exists(CURRENT_USER_PATH):
        os.remove(CURRENT_USER_PATH)

def log_auth_event(username, event_type):
    """Log authentication events."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = pd.DataFrame({
        'timestamp': [timestamp],
        'username': [username],
        'event': [event_type]
    })

    if os.path.exists(AUTH_LOGS_PATH):
        logs = pd.read_csv(AUTH_LOGS_PATH)
        logs = pd.concat([logs, log_entry])
    else:
        logs = log_entry

    logs.to_csv(AUTH_LOGS_PATH, index=False)

def clear_auth_logs():
    """Clear authentication logs."""
    if os.path.exists(AUTH_LOGS_PATH):
        os.remove(AUTH_LOGS_PATH)

def check_auth_state():
    """Check authentication state from both files."""
    current_user = check_current_user()
    logs_exist = os.path.exists(AUTH_LOGS_PATH)

    if current_user and logs_exist:
        try:
            logs = pd.read_csv(AUTH_LOGS_PATH)
            user_logs = logs[logs['username'] == current_user]
            if not user_logs.empty:
                last_event = user_logs.iloc[-1]['event']
                return current_user if last_event == 'logged in' else None
        except Exception as e:
            print(f"Error checking auth state: {e}")
            return None
    return None

def create_activity_log():
    """Create a new activity log file with headers."""
    with open(ACTIVITY_LOG_PATH, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['timestamp', 'username', 'activity'])

def log_user_activity(username, activity):
    """Log user activity to CSV file."""
    if not os.path.exists(ACTIVITY_LOG_PATH):
        create_activity_log()

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(ACTIVITY_LOG_PATH, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([timestamp, username, activity])

def clear_activity_log():
    """Remove the activity log file."""
    if os.path.exists(ACTIVITY_LOG_PATH):
        os.remove(ACTIVITY_LOG_PATH)

def check_recent_activity(username, activity_type, minutes=1):
    """Check if a specific activity occurred in the last X minutes."""
    if not os.path.exists(ACTIVITY_LOG_PATH):
        return False

    try:
        with open(ACTIVITY_LOG_PATH, 'r') as csvfile:
            reader = list(csv.reader(csvfile))
            if len(reader) > 1:  # If there are any activities (excluding header)
                last_activity = reader[-1]
                last_timestamp = datetime.strptime(last_activity[0], '%Y-%m-%d %H:%M:%S')
                last_username = last_activity[1]
                last_activity_type = last_activity[2]

                # Check if the last activity was within the last minute
                if (datetime.now() - last_timestamp).total_seconds() < minutes * 60:
                    return last_username == username and last_activity_type == activity_type
    except Exception as e:
        print(f"Error checking activity log: {e}")

    return False

def handle_session_end(username, reason):
    """End user session with reason"""
    if username:
        log_auth_event(username, f'logged out ({reason})')
        log_user_activity(username, f'session_end_{reason}')
        remove_current_user()
        clear_auth_logs()
        clear_activity_log()