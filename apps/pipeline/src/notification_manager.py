import os

class NotificationManager:
    """Utility class to handle sending notifications at the end of the pipeline."""

    @staticmethod
    def send_email(recipients, subject, message, attachment_paths=None):
        """
        Send an email notification

        Args:
            recipients (list): List of email addresses
            subject (str): Email subject
            message (str): Email body
            attachment_paths (list, optional): List of file paths to attach
        """
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.mime.application import MIMEApplication

        # Email configuration from environment or settings
        smtp_server = os.getenv('SAPPHIRE_PIPELINE_SMTP_SERVER', 'smtp.example.com')
        smtp_port = int(os.getenv('SAPPHIRE_PIPELINE_SMTP_PORT', 587))
        smtp_username = os.getenv('SAPPHIRE_PIPELINE_SMTP_USERNAME', 'user@example.com')
        smtp_password = os.getenv('SAPPHIRE_PIPELINE_SMTP_PASSWORD', 'password')
        sender_email = os.getenv('SAPPHIRE_PIPELINE_SENDER_EMAIL', 'forecast-system@example.com')

        # Debug printing of smtp configuration
        print(f"SMTP Server: {smtp_server}")
        print(f"SMTP Port: {smtp_port}")
        print(f"SMTP Username: {smtp_username}")

        # Create message
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = ', '.join(recipients)
        msg['Subject'] = subject

        # Attach message body
        msg.attach(MIMEText(message, 'plain'))

        # Attach files if provided
        if attachment_paths:
            for file_path in attachment_paths:
                try:
                    with open(file_path, 'rb') as file:
                        attachment = MIMEApplication(file.read())
                        attachment.add_header('Content-Disposition',
                                            'attachment',
                                            filename=os.path.basename(file_path))
                        msg.attach(attachment)
                except Exception as e:
                    print(f"Failed to attach {file_path}: {str(e)}")

        # Send email
        try:
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls()  # Secure the connection
            server.login(smtp_username, smtp_password)
            server.sendmail(sender_email, recipients, msg.as_string())
            server.quit()
            print(f"Email notification sent to {', '.join(recipients)}")
            return True
        except Exception as e:
            print(f"Failed to send email: {str(e)}")
            return False

    @staticmethod
    def send_failure_notification(task_name, error_details, log_file_paths=None, additional_info=None):
        """
        Send a notification when a task fails
        
        Args:
            task_name (str): Name of the failed task
            error_details (str): Error message or details
            log_file_paths (list, optional): List of log file paths to attach
            additional_info (dict, optional): Additional information about the failure
        
        Returns:
            bool: True if notification was sent successfully, False otherwise
        """
        # Get email recipients from environment variable
        email_recipients_str = os.getenv('SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS', '')
        if email_recipients_str:
            email_recipients = [email.strip() for email in email_recipients_str.split(',')]
        else:
            email_recipients = []
            
        if not email_recipients:
            print("No email recipients configured for failure notifications")
            return False
            
        # Get organization name
        organization = os.getenv('ieasyhydroforecast_organization', 'unknown')
            
        # Create subject line
        import datetime
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        subject = f"❌ ERROR: {organization.upper()} Pipeline - {task_name} Failed - {current_time}"
        
        # Create message body
        message = [
            f"The {task_name} task in the Sapphire Forecast Pipeline has failed.",
            f"\nTimestamp: {current_time}",
            f"\nError Details:\n{error_details}",
        ]
        
        # Add additional info if provided
        if additional_info:
            message.append("\nAdditional Information:")
            for key, value in additional_info.items():
                message.append(f"  {key}: {value}")
                
        # Add information about the log files
        if log_file_paths:
            message.append("\nLog files are attached to this email.")
        
        message.append("\nThis is an automated notification.")
        
        # Send the email
        return NotificationManager.send_email(
            recipients=email_recipients,
            subject=subject,
            message="\n".join(message),
            attachment_paths=log_file_paths
        )

    @staticmethod
    def send_sms(phone_numbers, message):
        """
        Send SMS notification (implementation depends on your SMS provider)

        Args:
            phone_numbers (list): List of phone numbers to notify
            message (str): SMS message content
        """
        # Example using Twilio (you would need to install the twilio package)
        try:
            # Uncomment and customize this code if you have Twilio set up
            """
            from twilio.rest import Client

            # Twilio credentials from environment
            account_sid = env.get('TWILIO_ACCOUNT_SID')
            auth_token = env.get('TWILIO_AUTH_TOKEN')
            twilio_phone = env.get('TWILIO_PHONE_NUMBER')

            client = Client(account_sid, auth_token)

            for phone in phone_numbers:
                client.messages.create(
                    body=message,
                    from_=twilio_phone,
                    to=phone
                )
            """
            print(f"SMS notification would be sent to {', '.join(phone_numbers)}")
            return True
        except Exception as e:
            print(f"Failed to send SMS: {str(e)}")
            return False