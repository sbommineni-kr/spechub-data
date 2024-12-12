# Audit Log
# Author: sudheer bommineni
# Email: sudheer.bommineni@kroger.com
# ID: kon8383
# Date: 20241001
# Description: This script performs a union of sharepoint file and post release report and exports it to a csv file and send email
# Execution: 
# python etl/mongodb/load_export_report.py -c config/datalake.yaml -a '{"sharepoint_path":"/Users/sudheer.bomminenikroger.com/Desktop/work/git/spechub-data/data/changes_made_after_nov13_release.xlsx","post_release_report":"/Users/sudheer.bomminenikroger.com/Desktop/work/git/spechub-data/data/PROD_SB_post_release_changes_20241211.csv"}' -j mongodb -n load_export_report -v prod

from core.dl_etlbase import DLETLBase
from core.dl_logger import DLLogger
import os
import csv
from bson.timestamp import Timestamp
from datetime import datetime
from datetime import datetime, timezone
import pandas as pd

job_name = "load_export_report"

class ExportReport(DLETLBase):
    def __init__(self):
        super().__init__()
    
    def read_excel_file(self, file_path):
        """
        Read an Excel file and return its content as a list of dictionaries.
        """
        
        df = pd.read_excel(file_path)

        return df 
    
    def merge_excel_files(self, sharepoint_df, post_release_report_df):
        """
        Merges sharepoint and post-release report dataframes with smart column matching
        """
        # Define column mappings for similar columns (source -> target)
        column_mapping = {
            'Jira': 'jira',
            'SubcommodityCode': 'subCommodityCode',
            'SubCommodityName': 'subCommodityName',
            'Variation': 'variations',
            'SecondaryVariation': 'variations_secondary',  # Changed to unique name
            'Environment': 'database_name',
            'Notes/Comments': 'changes_summary',
            'Date Completed to Test': 'test_completion_date',
            'Date Completed to UAT': 'uat_completion_date',
            'Date Completed to Prod': 'prod_completion_date'
        }

        # Rename sharepoint columns to match post-release format
        sharepoint_df = sharepoint_df.rename(columns=column_mapping)

        # Get target columns from post-release report
        target_columns = list(post_release_report_df.columns)
        
        # Get remaining unique sharepoint columns
        remaining_sharepoint_cols = [col for col in sharepoint_df.columns 
                                if col not in target_columns]

        # Combine all unique columns
        all_columns = list(dict.fromkeys(target_columns + remaining_sharepoint_cols))

        # Add missing columns with 'N/A' values
        for col in all_columns:
            if col not in sharepoint_df.columns:
                sharepoint_df[col] = 'N/A'
            if col not in post_release_report_df.columns:
                post_release_report_df[col] = 'N/A'

        # Ensure column order matches in both dataframes
        sharepoint_df = sharepoint_df[all_columns]
        post_release_report_df = post_release_report_df[all_columns]

        # Merge dataframes with unique columns
        merged_df = pd.concat([sharepoint_df, post_release_report_df], ignore_index=True)
        
        return merged_df
    
    def send_email_report(self, file_path):
        """
        Sends email with the merged report as attachment
        """
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.mime.application import MIMEApplication
        import smtplib
        import os

        # Email configuration
        smtp_user = 'sudheer.bommineni@kroger.com'
        smtp_password = os.environ.get('EMAIL_PASSWORD')
        recipient = 'sudheer.bommineni@kroger.com'
        subject = 'Dec 11th data release report'
        body = """Hello All,
        These are the changes that are implemented since past release(Nov 13) to current release (Dec 11) and change_source column will tell you if its done though UI or through backend.
        
        Thanks,
        Spechub Data Team"""

        # Create message
        msg = MIMEMultipart()
        msg['From'] = f'"noreply@SpecHubDataTeam" <{smtp_user}>'  # Use authenticated email as sender
        msg['To'] = recipient
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        # Attach the file
        with open(file_path, 'rb') as f:
            attachment = MIMEApplication(f.read(), _subtype='xlsx')
            attachment.add_header('Content-Disposition', 'attachment', filename=os.path.basename(file_path))
            msg.attach(attachment)

        # Send email using SMTP
        smtp_server = 'smtp-mail.outlook.com'
        smtp_port = 587

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
            dl_log.info("Email sent successfully with the export report")

        




 

if __name__ == "__main__":
    job = ExportReport()
    dl_log = DLLogger(__name__)
    sharepoint_path = job.job_additional_args.get('sharepoint_path')
    post_release_report = job.job_additional_args.get('post_release_report')
    sharepoint_df = job.read_excel_file(sharepoint_path)
    sharepoint_df = sharepoint_df[sharepoint_df['SubcommodityCode'].notna() & (sharepoint_df['SubcommodityCode'] != '')]
    #add change_source column to above dataframe
    sharepoint_df['change_source'] = 'UI'
    post_release_report_df = pd.read_csv(post_release_report,header='infer')
    #get count
    print(sharepoint_df.shape)
    #add change_source column to above dataframe
    post_release_report_df['change_source'] = 'Backend'
    print(post_release_report_df.shape)
    merged_df = job.merge_excel_files(sharepoint_df, post_release_report_df)
    # Export the merged dataframe to a excel file
    merged_df.to_excel('data/merged_excel.xlsx', index=False)
    #send email 
    job.send_email_report('data/spechub_data_changes_dec11.xlsx')
    
    