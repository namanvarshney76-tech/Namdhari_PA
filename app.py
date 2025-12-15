#!/usr/bin/env python3
"""
Namdhari Payment Advice Processor - Streamlit App
Fixed version with proper date conversion and single source_file column
"""

import streamlit as st
import os
import json
import base64
import tempfile
import time
import logging
import traceback
import warnings
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
import re
from io import BytesIO, StringIO

# Google APIs
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import io

# Add LlamaParse import
try:
    from llama_cloud_services import LlamaExtract
    LLAMA_AVAILABLE = True
except ImportError:
    LLAMA_AVAILABLE = False
    print("WARNING: LlamaParse not available.")

warnings.filterwarnings("ignore")

# Configure Streamlit page
st.set_page_config(
    page_title="Namdhari Payment Advice Processor",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Default configuration
DEFAULT_CONFIG = {
    'mail': {
        'gdrive_folder_id': '1mpmbUTOLQN0wCwVowNDod9rrP4vJnVgN',
        'sender': 'erp@namdharis.in',
        'search_term': 'Payment Advice',
        'days_back': 30,
        'max_results': 2
    },
    'payment_advice': {
        'llama_api_key': 'llx-SepLKRXRKV2tGGkmAxBWyGkOTTTKKNMeRim67QWhwfOHW5JD',
        'llama_agent': 'Namdhari Agnent',
        'drive_folder_id': '1mpmbUTOLQN0wCwVowNDod9rrP4vJnVgN',
        'spreadsheet_id': '1jgWrYI5wBgpAbV05LuzW9AosDh8zT72yg06PUTntHGo',
        'sheet_range': 'payment_advice',
        'days_back': 30,
        'max_files': 3,
        'skip_existing': True
    },
    'workflow_log': {
        'spreadsheet_id': '1jgWrYI5wBgpAbV05LuzW9AosDh8zT72yg06PUTntHGo',
        'sheet_range': 'workflow_logs'
    }
}


def excel_date_to_string(excel_date):
    """
    Convert Excel serial date number to readable date string
    Excel dates are stored as number of days since 1899-12-30
    """
    try:
        # If it's already a string that looks like a date, return it
        if isinstance(excel_date, str):
            # Check if it's already formatted
            if '-' in excel_date or '/' in excel_date:
                return excel_date
            # Try to convert if it's a string number
            try:
                excel_date = float(excel_date)
            except:
                return excel_date
        
        # Convert Excel serial number to date
        if isinstance(excel_date, (int, float)):
            # Excel epoch starts at 1899-12-30
            excel_epoch = datetime(1899, 12, 30)
            date_value = excel_epoch + timedelta(days=int(excel_date))
            
            # Format as "1-Dec-2025"
            return date_value.strftime("%-d-%b-%Y") if os.name != 'nt' else date_value.strftime("%#d-%b-%Y")
        
        return excel_date
        
    except Exception as e:
        # If conversion fails, return original value
        return excel_date


class NamdhariPaymentAdviceProcessor:
    def __init__(self):
        self.gmail_service = None
        self.drive_service = None
        self.sheets_service = None
        
        self.gmail_scopes = ['https://www.googleapis.com/auth/gmail.readonly']
        self.drive_scopes = ['https://www.googleapis.com/auth/drive']
        self.sheets_scopes = ['https://www.googleapis.com/auth/spreadsheets']
        
        # Initialize logs in session state if not exists
        if 'logs' not in st.session_state:
            st.session_state.logs = []
        
        # Initialize config in session state if not exists
        if 'config' not in st.session_state:
            st.session_state.config = DEFAULT_CONFIG
    
    def log(self, message: str, level: str = "INFO"):
        """Add log entry with timestamp to session state"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = {
            "timestamp": timestamp, 
            "level": level.upper(), 
            "message": message
        }
        
        # Add to session state logs
        if 'logs' not in st.session_state:
            st.session_state.logs = []
        
        st.session_state.logs.append(log_entry)
        
        # Keep only last 200 logs to prevent memory issues
        if len(st.session_state.logs) > 200:
            st.session_state.logs = st.session_state.logs[-200:]
    
    def get_logs(self):
        """Get logs from session state"""
        return st.session_state.get('logs', [])
    
    def clear_logs(self):
        """Clear all logs"""
        st.session_state.logs = []
    
    def get_config(self):
        """Get configuration from session state"""
        return st.session_state.get('config', DEFAULT_CONFIG)
    
    def update_config(self, new_config: Dict):
        """Update configuration in session state"""
        st.session_state.config = new_config
    
    def authenticate_from_secrets(self, progress_bar, status_text):
        """Authenticate using Streamlit secrets with web-based OAuth flow"""
        try:
            self.log("Starting authentication process...", "INFO")
            status_text.text("Authenticating with Google APIs...")
            progress_bar.progress(10)
            
            # Check for existing token in session state
            if 'oauth_token' in st.session_state:
                try:
                    combined_scopes = list(set(self.gmail_scopes + self.drive_scopes + self.sheets_scopes))
                    creds = Credentials.from_authorized_user_info(st.session_state.oauth_token, combined_scopes)
                    if creds and creds.valid:
                        progress_bar.progress(50)
                        # Build services
                        self.gmail_service = build('gmail', 'v1', credentials=creds)
                        self.drive_service = build('drive', 'v3', credentials=creds)
                        self.sheets_service = build('sheets', 'v4', credentials=creds)
                        progress_bar.progress(100)
                        self.log("Authentication successful using cached token!", "SUCCESS")
                        status_text.text("Authentication successful!")
                        return True
                    elif creds and creds.expired and creds.refresh_token:
                        creds.refresh(Request())
                        st.session_state.oauth_token = json.loads(creds.to_json())
                        # Build services
                        self.gmail_service = build('gmail', 'v1', credentials=creds)
                        self.drive_service = build('drive', 'v3', credentials=creds)
                        self.sheets_service = build('sheets', 'v4', credentials=creds)
                        progress_bar.progress(100)
                        self.log("Authentication successful after token refresh!", "SUCCESS")
                        status_text.text("Authentication successful!")
                        return True
                except Exception as e:
                    self.log(f"Cached token invalid: {str(e)}", "WARNING")
            
            # Use Streamlit secrets for OAuth
            if "google" in st.secrets and "credentials_json" in st.secrets["google"]:
                creds_data = json.loads(st.secrets["google"]["credentials_json"])
                combined_scopes = list(set(self.gmail_scopes + self.drive_scopes + self.sheets_scopes))
                
                # Configure for web application
                flow = Flow.from_client_config(
                    client_config=creds_data,
                    scopes=combined_scopes,
                    redirect_uri=st.secrets.get("redirect_uri", st.secrets.get("REDIRECT_URI", "https://namdharipa.streamlit.app/"))
                )
                
                # Generate authorization URL
                auth_url, _ = flow.authorization_url(prompt='consent')
                
                # Check for callback code
                query_params = st.query_params
                if "code" in query_params:
                    try:
                        code = query_params["code"]
                        flow.fetch_token(code=code)
                        creds = flow.credentials
                        
                        # Save credentials in session state
                        st.session_state.oauth_token = json.loads(creds.to_json())
                        
                        progress_bar.progress(50)
                        # Build services
                        self.gmail_service = build('gmail', 'v1', credentials=creds)
                        self.drive_service = build('drive', 'v3', credentials=creds)
                        self.sheets_service = build('sheets', 'v4', credentials=creds)
                        
                        progress_bar.progress(100)
                        self.log("OAuth authentication successful!", "SUCCESS")
                        status_text.text("Authentication successful!")
                        
                        # Clear the code from URL
                        st.query_params.clear()
                        return True
                    except Exception as e:
                        self.log(f"OAuth authentication failed: {str(e)}", "ERROR")
                        st.error(f"Authentication failed: {str(e)}")
                        return False
                else:
                    # Show authorization link
                    st.markdown("### Google Authentication Required")
                    st.markdown(f"[Click here to authorize with Google]({auth_url})")
                    self.log("Waiting for user to authorize application", "INFO")
                    st.info("Click the link above to authorize, you'll be redirected back automatically")
                    st.stop()
            else:
                self.log("Google credentials missing in Streamlit secrets", "ERROR")
                st.error("Google credentials missing in Streamlit secrets")
                return False
                
        except Exception as e:
            self.log(f"Authentication failed: {str(e)}", "ERROR")
            st.error(f"Authentication failed: {str(e)}")
            return False
    
    def search_emails(self, sender: str = "", search_term: str = "", 
                     days_back: int = 7, max_results: int = 50) -> List[Dict]:
        """Search for emails with attachments"""
        try:
            query_parts = ["has:attachment"]
            
            if sender:
                query_parts.append(f'from:"{sender}"')  
            
            if search_term:
                if "," in search_term:
                    keywords = [k.strip() for k in search_term.split(",")]
                    keyword_query = " OR ".join([f'"{k}"' for k in keywords if k])
                    if keyword_query:
                        query_parts.append(f"({keyword_query})")
                else:
                    query_parts.append(f'"{search_term}"')
            
            start_date = datetime.now() - timedelta(days=days_back)
            query_parts.append(f"after:{start_date.strftime('%Y/%m/%d')}")
            
            query = " ".join(query_parts)
            self.log(f"Searching Gmail with query: {query}", "INFO")
            
            result = self.gmail_service.users().messages().list(
                userId='me', q=query, maxResults=max_results
            ).execute()
            
            messages = result.get('messages', [])
            self.log(f"Found {len(messages)} emails matching criteria", "INFO")
            
            return messages
            
        except Exception as e:
            self.log(f"Email search failed: {str(e)}", "ERROR")
            return []
    
    def get_email_details(self, message_id: str) -> Dict:
        """Get email details including sender and subject"""
        try:
            message = self.gmail_service.users().messages().get(
                userId='me', id=message_id, format='metadata'
            ).execute()
            
            headers = message['payload'].get('headers', [])
            
            details = {
                'id': message_id,
                'sender': next((h['value'] for h in headers if h['name'] == "From"), "Unknown"),
                'subject': next((h['value'] for h in headers if h['name'] == "Subject"), "(No Subject)"),
                'date': next((h['value'] for h in headers if h['name'] == "Date"), "")
            }
            
            return details
            
        except Exception as e:
            self.log(f"Failed to get email details for {message_id}: {str(e)}", "ERROR")
            return {}
    
    def sanitize_filename(self, filename: str) -> str:
        """Clean up filenames to be safe for all operating systems"""
        cleaned = re.sub(r'[<>:"/\\|?*]', '_', filename)
        if len(cleaned) > 100:
            name_parts = cleaned.split('.')
            if len(name_parts) > 1:
                extension = name_parts[-1]
                base_name = '.'.join(name_parts[:-1])
                cleaned = f"{base_name[:95]}.{extension}"
            else:
                cleaned = cleaned[:100]
        return cleaned
    
    def create_drive_folder(self, folder_name: str, parent_folder_id: Optional[str] = None) -> str:
        """Create a folder in Google Drive"""
        try:
            query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            if parent_folder_id:
                query += f" and '{parent_folder_id}' in parents"
            
            existing = self.drive_service.files().list(q=query, fields='files(id, name)').execute()
            files = existing.get('files', [])
            
            if files:
                folder_id = files[0]['id']
                self.log(f"Using existing folder: {folder_name} (ID: {folder_id})", "INFO")
                return folder_id
            
            folder_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            
            if parent_folder_id:
                folder_metadata['parents'] = [parent_folder_id]
            
            folder = self.drive_service.files().create(
                body=folder_metadata,
                fields='id'
            ).execute()
            
            folder_id = folder.get('id')
            self.log(f"Created Google Drive folder: {folder_name} (ID: {folder_id})", "INFO")
            
            return folder_id
            
        except Exception as e:
            self.log(f"Failed to create folder {folder_name}: {str(e)}", "ERROR")
            return ""
    
    def upload_to_drive(self, file_data: bytes, filename: str, folder_id: str) -> bool:
        """Upload file to Google Drive"""
        try:
            query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
            existing = self.drive_service.files().list(q=query, fields='files(id, name)').execute()
            files = existing.get('files', [])
            
            if files:
                self.log(f"File already exists, skipping: {filename}", "INFO")
                return True
            
            file_metadata = {
                'name': filename,
                'parents': [folder_id] if folder_id else []
            }
            
            media = MediaIoBaseUpload(
                io.BytesIO(file_data),
                mimetype='application/octet-stream',
                resumable=True
            )
            
            file = self.drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            
            self.log(f"Uploaded to Drive: {filename}", "INFO")
            return True
            
        except Exception as e:
            self.log(f"Failed to upload {filename}: {str(e)}", "ERROR")
            return False
    
    def process_attachment(self, message_id: str, part: Dict, sender_info: Dict, 
                          search_term: str, base_folder_id: str) -> Dict:
        """Process and upload a single attachment - Only PDFs"""
        try:
            filename = part.get("filename", "")
            if not filename:
                return {'success': False}
            
            if not filename.lower().endswith('.pdf'):
                self.log(f"Not a PDF file: {filename}", "INFO")
                return {'success': False}
            
            clean_filename = self.sanitize_filename(filename)
            original_filename = clean_filename
            final_filename = f"{message_id}_{clean_filename}"

            attachment_id = part["body"].get("attachmentId")
            if not attachment_id:
                return {'success': False}
            
            att = self.gmail_service.users().messages().attachments().get(
                userId='me', messageId=message_id, id=attachment_id
            ).execute()
            
            if not att.get("data"):
                return {'success': False}
            
            file_data = base64.urlsafe_b64decode(att["data"].encode("UTF-8"))
            
            gmail_folder_name = "Gmail_Attachments"
            
            gmail_folder_id = self.create_drive_folder(gmail_folder_name, base_folder_id)
            pdfs_folder_id = self.create_drive_folder("PDFs", gmail_folder_id)
            
            success = self.upload_to_drive(file_data, final_filename, pdfs_folder_id)
            
            if success:
                self.log(f"Processed PDF attachment: {filename} (saved as {final_filename})", "SUCCESS")
                return {
                    'success': True,
                    'message_id': message_id,
                    'original_filename': original_filename,
                    'saved_filename': final_filename,
                    'drive_folder_id': pdfs_folder_id
                }
            
            return {'success': False}
            
        except Exception as e:
            self.log(f"Failed to process attachment {part.get('filename', 'unknown')}: {str(e)}", "ERROR")
            return {'success': False}
    
    def extract_attachments_from_email(self, message_id: str, payload: Dict, 
                                     sender_info: Dict, search_term: str, 
                                     base_folder_id: str) -> List[Dict]:
        """Recursively extract all PDF attachments from an email"""
        processed_attachments = []
        
        if "parts" in payload:
            for part in payload["parts"]:
                processed_attachments.extend(self.extract_attachments_from_email(
                    message_id, part, sender_info, search_term, base_folder_id
                ))
        
        elif payload.get("filename") and "attachmentId" in payload.get("body", {}):
            result = self.process_attachment(message_id, payload, sender_info, search_term, base_folder_id)
            if result.get('success'):
                processed_attachments.append(result)
        
        return processed_attachments
    
    def process_mail_to_drive_workflow(self, config: dict, progress_callback=None, status_callback=None):
        """Process Mail to Drive workflow for Payment Advice PDF files only"""
        try:
            if status_callback:
                status_callback("Step 1/3: Searching for emails...")
            
            if progress_callback:
                progress_callback(10)
            
            emails = self.search_emails(
                sender=config['sender'],
                search_term=config['search_term'],
                days_back=config['days_back'],
                max_results=config['max_results']
            )
            
            if not emails:
                self.log("No emails found matching criteria", "INFO")
                return {
                    'success': True, 
                    'total_emails': 0,
                    'processed_emails': 0, 
                    'total_attachments': 0, 
                    'failed': 0,
                    'attachments_info': []
                }
            
            base_folder_id = config.get('gdrive_folder_id')
            if not base_folder_id:
                self.log("No base folder ID provided", "ERROR")
                return {
                    'success': False, 
                    'total_emails': len(emails),
                    'processed_emails': 0, 
                    'total_attachments': 0, 
                    'failed': len(emails),
                    'attachments_info': []
                }
            
            stats = {
                'total_emails': len(emails),
                'processed_emails': 0,
                'total_attachments': 0,
                'successful_uploads': 0,
                'failed_uploads': 0,
                'attachments_info': []
            }
            
            self.log(f"Processing {len(emails)} emails...", "INFO")
            
            if status_callback:
                status_callback(f"Step 2/3: Processing {len(emails)} emails...")
            
            if progress_callback:
                progress_callback(30)
            
            for i, email in enumerate(emails, 1):
                try:
                    if status_callback:
                        status_callback(f"Processing email {i}/{len(emails)}")
                    
                    sender_info = self.get_email_details(email['id'])
                    if not sender_info:
                        continue
                    
                    message = self.gmail_service.users().messages().get(
                        userId='me', id=email['id']
                    ).execute()
                    
                    if not message or not message.get('payload'):
                        continue
                    
                    attachments = self.extract_attachments_from_email(
                        email['id'], message['payload'], sender_info, 
                        config['search_term'], base_folder_id
                    )
                    
                    if attachments:
                        stats['total_attachments'] += len(attachments)
                        stats['successful_uploads'] += len(attachments)
                        stats['attachments_info'].extend(attachments)
                        stats['processed_emails'] += 1
                        
                        subject = sender_info.get('subject', 'No Subject')[:50]
                        self.log(f"Found {len(attachments)} PDF attachments in email: {subject}", "INFO")
                    else:
                        self.log(f"No PDF attachments found in email: {sender_info.get('subject', 'No Subject')[:50]}", "INFO")
                    
                    if progress_callback:
                        progress = 30 + ((i) / len(emails)) * 50
                        progress_callback(int(progress))
                    
                except Exception as e:
                    self.log(f"Failed to process email {email.get('id', 'unknown')}: {str(e)}", "ERROR")
                    stats['failed_uploads'] += 1
            
            if status_callback:
                status_callback("Step 3/3: Finalizing uploads...")
            
            if progress_callback:
                progress_callback(90)
            
            self.log("Mail to Drive workflow complete!", "SUCCESS")
            
            if progress_callback:
                progress_callback(100)
            
            return {
                'success': True, 
                'total_emails': stats['total_emails'],
                'processed_emails': stats['processed_emails'], 
                'total_attachments': stats['successful_uploads'],
                'failed': stats['failed_uploads'],
                'attachments_info': stats['attachments_info']
            }
            
        except Exception as e:
            self.log(f"Mail to Drive workflow failed: {str(e)}", "ERROR")
            return {
                'success': False, 
                'total_emails': 0,
                'processed_emails': 0, 
                'total_attachments': 0, 
                'failed': 0,
                'attachments_info': []
            }
    
    def list_drive_files(self, folder_id: str, days_back: int = 1, file_type: str = 'pdf') -> List[Dict]:
        """List all PDF files in a Google Drive folder filtered by creation date"""
        try:
            start_datetime = datetime.utcnow() - timedelta(days=days_back - 1)
            start_str = start_datetime.strftime('%Y-%m-%dT00:00:00Z')
            
            query = f"'{folder_id}' in parents and (mimeType='application/pdf' or name contains '.pdf') and trashed=false and createdTime >= '{start_str}'"
            
            files = []
            page_token = None

            while True:
                results = self.drive_service.files().list(
                    q=query,
                    fields="nextPageToken, files(id, name, mimeType, createdTime, modifiedTime)",
                    orderBy="createdTime desc",
                    pageToken=page_token,
                    pageSize=100
                ).execute()
                
                files.extend(results.get('files', []))
                page_token = results.get('nextPageToken', None)
                
                if page_token is None:
                    break

            self.log(f"Found {len(files)} PDF files in folder {folder_id} (last {days_back} days)", "INFO")
            
            return files
        except Exception as e:
            self.log(f"Failed to list files in folder {folder_id}: {str(e)}", "ERROR")
            return []
    
    def download_from_drive(self, file_id: str, file_name: str) -> bytes:
        """Download a file from Google Drive"""
        try:
            request = self.drive_service.files().get_media(fileId=file_id)
            file_data = request.execute()
            self.log(f"Downloaded: {file_name}", "INFO")
            return file_data
        except Exception as e:
            self.log(f"Failed to download file {file_name}: {str(e)}", "ERROR")
            return b""
    
    def append_to_google_sheet(self, spreadsheet_id: str, range_name: str, values: List[List[Any]]) -> bool:
        """Append data to a Google Sheet with retry mechanism"""
        max_retries = 3
        wait_time = 2
        
        for attempt in range(1, max_retries + 1):
            try:
                body = {'values': values}
                result = self.sheets_service.spreadsheets().values().append(
                    spreadsheetId=spreadsheet_id, 
                    range=range_name,
                    valueInputOption='USER_ENTERED', 
                    body=body,
                    insertDataOption='INSERT_ROWS'
                ).execute()
                
                updated_cells = result.get('updates', {}).get('updatedCells', 0)
                self.log(f"Appended {updated_cells} cells to Google Sheet", "INFO")
                return True
            except Exception as e:
                if attempt < max_retries:
                    self.log(f"Attempt {attempt} failed: {str(e)}", "WARNING")
                    time.sleep(wait_time)
                else:
                    self.log(f"Failed to append to Google Sheet: {str(e)}", "ERROR")
                    return False
        return False
    
    def get_sheet_headers(self, spreadsheet_id: str, sheet_range: str) -> List[str]:
        """Get existing headers from Google Sheet"""
        try:
            sheet_name = sheet_range.split('!')[0] if '!' in sheet_range else sheet_range
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1:Z1",
                majorDimension="ROWS"
            ).execute()
            values = result.get('values', [])
            return values[0] if values else []
        except Exception as e:
            self.log(f"No existing headers or error: {str(e)}", "INFO")
            return []
    
    def update_headers(self, spreadsheet_id: str, sheet_name: str, new_headers: List[str]) -> bool:
        """Update the header row with new columns"""
        try:
            body = {'values': [new_headers]}
            result = self.sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1:{chr(64 + min(len(new_headers), 26))}1",
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            self.log(f"Updated headers with {len(new_headers)} columns", "INFO")
            return True
        except Exception as e:
            self.log(f"Failed to update headers: {str(e)}", "ERROR")
            return False
    
    def get_existing_source_files(self, spreadsheet_id: str, sheet_range: str, column_name: str = "source_file_name") -> set:
        """Get set of existing source_file_name from Google Sheet"""
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=sheet_range,
                majorDimension="ROWS"
            ).execute()
            
            values = result.get('values', [])
            if not values:
                return set()
            
            headers = values[0]
            if column_name not in headers:
                self.log(f"No '{column_name}' column found in sheet", "WARNING")
                return set()
            
            name_index = headers.index(column_name)
            existing_names = {row[name_index] for row in values[1:] if len(row) > name_index and row[name_index]}
            
            self.log(f"Found {len(existing_names)} existing file names in '{column_name}' column", "INFO")
            return existing_names
            
        except Exception as e:
            self.log(f"Failed to get existing file names: {str(e)}", "ERROR")
            return set()
    
    def safe_extract(self, agent, file_path: str, retries: int = 3, wait_time: int = 2):
        """Retry-safe extraction to handle server disconnections"""
        for attempt in range(1, retries + 1):
            try:
                result = agent.extract(file_path)
                return result
            except Exception as e:
                self.log(f"Attempt {attempt} failed for {file_path}: {e}", "WARNING")
                time.sleep(wait_time)
        raise Exception(f"Extraction failed after {retries} attempts for {file_path}")
    
    def parse_extracted_data(self, extracted_data: Dict, file_info: Dict) -> List[Dict]:
        """
        Parse extracted Payment Advice data from LlamaParse
        FIXED: Only uses source_file_name (Drive filename with message ID)
        FIXED: Converts Excel date format to readable format
        """
        rows = []
        
        try:
            if hasattr(extracted_data, 'data'):
                data = extracted_data.data
            elif isinstance(extracted_data, dict):
                data = extracted_data
            else:
                data = extracted_data
            
            document_info = {}
            bill_details = []
            payment_mode_details = []
            
            for key in ['document_info', 'data', 'payment_advice', 'document']:
                if key in data and isinstance(data[key], dict):
                    document_info = data[key]
                    break
            
            for key in ['bill_details', 'bills', 'items', 'line_items']:
                if key in data and isinstance(data[key], list):
                    bill_details = data[key]
                    break
            
            for key in ['payment_mode_details', 'payment_details', 'payment']:
                if key in data and isinstance(data[key], list):
                    payment_mode_details = data[key]
                    break
            
            if not document_info:
                direct_keys = ['date', 'clearing_document_number', 'utr_number']
                if any(key in data for key in direct_keys):
                    document_info = data
            
            total_payment = 0
            if payment_mode_details:
                for payment in payment_mode_details:
                    if isinstance(payment, dict) and 'amount' in payment:
                        total_payment = payment['amount']
                        break
            
            total_bill_amount = 0
            total_net_amount = 0
            total_tds = 0
            
            for bill in bill_details:
                if isinstance(bill, dict):
                    bill_amount = bill.get('bill_amount', bill.get('amount', 0))
                    net_amount = bill.get('net_amount', bill.get('net_amount', 0))
                    tds_amount = bill.get('deduction_tds', bill.get('tds', 0))
                    
                    for val in [bill_amount, net_amount, tds_amount]:
                        if isinstance(val, str):
                            try:
                                val = float(val)
                            except:
                                val = 0
                        total_bill_amount += float(bill_amount) if isinstance(bill_amount, (int, float)) else 0
                        total_net_amount += float(net_amount) if isinstance(net_amount, (int, float)) else 0
                        total_tds += float(tds_amount) if isinstance(tds_amount, (int, float)) else 0
            
            # FIXED: Convert dates from Excel format
            doc_date = excel_date_to_string(document_info.get('date', ''))
            
            # Create base row for document-level information
            document_row = {
                "document_date": doc_date,
                "clearing_document_number": document_info.get('clearing_document_number', ''),
                "utr_number": document_info.get('utr_number', document_info.get('utr', '')),
                "total_payment_amount": total_payment,
                "total_bill_amount": total_bill_amount,
                "total_net_amount": total_net_amount,
                "total_tds_amount": total_tds,
                "payment_count": len(bill_details),
                "source_file_name": file_info.get('name', ''),  # Drive filename with message ID
                "processed_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "drive_file_id": file_info.get('id', '')
            }
            
            rows.append(document_row)
            
            # Create rows for each bill detail
            for i, bill in enumerate(bill_details, 1):
                if not isinstance(bill, dict):
                    continue
                
                # FIXED: Convert bill date from Excel format
                bill_doc_date = excel_date_to_string(bill.get('bill_document_date', bill.get('date', '')))
                    
                bill_row = {
                    "document_date": doc_date,
                    "clearing_document_number": document_info.get('clearing_document_number', ''),
                    "utr_number": document_info.get('utr_number', document_info.get('utr', '')),
                    "bill_reference_number": bill.get('bill_reference_number', bill.get('reference_number', '')),
                    "accounting_document_number": bill.get('accounting_document_number', bill.get('accounting_number', '')),
                    "bill_document_date": bill_doc_date,
                    "bill_amount": bill.get('bill_amount', bill.get('amount', 0)),
                    "deduction_tds": bill.get('deduction_tds', bill.get('tds', 0)),
                    "net_amount": bill.get('net_amount', 0),
                    "bill_sequence": i,
                    "total_bills": len(bill_details),
                    "total_payment_amount": total_payment,
                    "source_file_name": file_info.get('name', ''),  # Drive filename with message ID
                    "processed_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "drive_file_id": file_info.get('id', '')
                }
                rows.append(bill_row)
            
            self.log(f"Created {len(rows)} rows from {len(bill_details)} bills in {file_info.get('name', '')}", "INFO")
            
        except Exception as e:
            self.log(f"Failed to parse extracted data: {str(e)}", "ERROR")
        
        return rows
    
    def process_payment_advice_workflow(self, config: dict, progress_callback=None, status_callback=None):
        """
        Main workflow to process Payment Advice PDF files using LlamaParse
        FIXED: Removed duplicate source_file column, only uses source_file_name
        FIXED: Converts date formats from Excel serial to readable format
        """
        stats = {
            'total_pdfs': 0,
            'processed_pdfs': 0,
            'failed_pdfs': 0,
            'skipped_pdfs': 0,
            'rows_added': 0,
            'bills_processed': 0,
            'llama_errors': 0
        }
        
        if not LLAMA_AVAILABLE:
            self.log("LlamaParse not available. Install with: pip install llama-cloud-services", "ERROR")
            return stats
        
        try:
            if status_callback:
                status_callback("Step 1/5: Initializing LlamaParse...")
            
            if progress_callback:
                progress_callback(10)
            
            os.environ["LLAMA_CLOUD_API_KEY"] = config['llama_api_key']
            extractor = LlamaExtract()
            agent = extractor.get_agent(name=config['llama_agent'])
            
            if agent is None:
                self.log(f"Could not find agent '{config['llama_agent']}'. Check dashboard.", "ERROR")
                return stats
            
            self.log("LlamaParse Agent found and ready", "INFO")
            
            if status_callback:
                status_callback("Step 2/5: Setting up Google Drive folders...")
            
            if progress_callback:
                progress_callback(20)
            
            sheet_name = config['sheet_range'].split('!')[0] if '!' in config['sheet_range'] else config['sheet_range']
            
            gmail_folder_id = self.create_drive_folder("Gmail_Attachments", config['drive_folder_id'])
            pdfs_folder_id = self.create_drive_folder("PDFs", gmail_folder_id)
            
            existing_names = self.get_existing_source_files(config['spreadsheet_id'], config['sheet_range'], "source_file_name")
            self.log(f"Found {len(existing_names)} already processed files in sheet", "INFO")
            
            if status_callback:
                status_callback("Step 3/5: Searching for PDF files...")
            
            if progress_callback:
                progress_callback(30)
            
            pdf_files = self.list_drive_files(pdfs_folder_id, config.get('days_back', 7), 'pdf')
            stats['total_pdfs'] = len(pdf_files)
            
            # Filter based on actual Drive filename (with message ID)
            files_to_process = [f for f in pdf_files if f['name'] not in existing_names]
            stats['skipped_pdfs'] = len(pdf_files) - len(files_to_process)
            
            self.log(f"After filtering: {len(files_to_process)} PDFs to process, {stats['skipped_pdfs']} skipped", "INFO")
            
            max_files = config.get('max_files')
            if max_files is not None and len(files_to_process) > max_files:
                files_to_process = files_to_process[:max_files]
                self.log(f"Limited to {len(files_to_process)} files after max_files limit", "INFO")
            
            if not files_to_process:
                self.log("No PDF files to process after filtering", "INFO")
                return stats
            
            headers = self.get_sheet_headers(config['spreadsheet_id'], config['sheet_range'])
            headers_set = bool(headers)
            
            # FIXED: Removed duplicate "source_file" column, only keeping "source_file_name"
            default_headers = [
                "document_date", "clearing_document_number", "utr_number",
                "bill_reference_number", "accounting_document_number", "bill_document_date",
                "bill_amount", "deduction_tds", "net_amount", "bill_sequence", "total_bills",
                "total_payment_amount", "total_bill_amount", "total_net_amount", "total_tds_amount",
                "payment_count", "source_file_name", "processed_date", "drive_file_id"
            ]
            
            if not headers_set:
                headers = default_headers
                self.update_headers(config['spreadsheet_id'], sheet_name, headers)
                headers_set = True
                self.log("Created new headers for payment advice sheet", "INFO")
            
            if "source_file_name" not in headers:
                headers.append("source_file_name")
                self.update_headers(config['spreadsheet_id'], sheet_name, headers)
                self.log("Added 'source_file_name' column to headers", "INFO")
            
            # Remove old "source_file" column if it exists
            if "source_file" in headers and "source_file_name" in headers:
                headers = [h for h in headers if h != "source_file"]
                self.update_headers(config['spreadsheet_id'], sheet_name, headers)
                self.log("Removed duplicate 'source_file' column", "INFO")
            
            if status_callback:
                status_callback(f"Step 4/5: Processing {len(files_to_process)} PDF files...")
            
            for i, pdf_file in enumerate(files_to_process, 1):
                try:
                    if status_callback:
                        status_callback(f"Processing PDF {i}/{len(files_to_process)}: {pdf_file['name']}")
                    
                    current_progress = 40 + ((i) / len(files_to_process)) * 40
                    if progress_callback:
                        progress_callback(int(current_progress))
                    
                    file_data = self.download_from_drive(pdf_file['id'], pdf_file['name'])
                    if not file_data:
                        self.log(f"Failed to download {pdf_file['name']}", "ERROR")
                        stats['failed_pdfs'] += 1
                        continue
                    
                    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp_file:
                        tmp_file.write(file_data)
                        tmp_path = tmp_file.name
                    
                    try:
                        self.log(f"Sending to LlamaParse: {pdf_file['name']}", "INFO")
                        extraction_result = self.safe_extract(agent, tmp_path)
                        
                        # FIXED: Pass actual Drive filename (with message ID)
                        rows_data = self.parse_extracted_data(extraction_result, pdf_file)
                        
                        if not rows_data:
                            self.log(f"No data extracted from {pdf_file['name']}", "WARNING")
                            stats['failed_pdfs'] += 1
                            stats['llama_errors'] += 1
                            continue
                        
                        bill_rows = [r for r in rows_data if 'bill_reference_number' in r]
                        stats['bills_processed'] += len(bill_rows)
                        
                        sheet_rows = []
                        for row_dict in rows_data:
                            row_values = [row_dict.get(h, "") for h in headers]
                            sheet_rows.append(row_values)
                        
                        if self.append_to_google_sheet(config['spreadsheet_id'], config['sheet_range'], sheet_rows):
                            stats['rows_added'] += len(sheet_rows)
                            stats['processed_pdfs'] += 1
                            self.log(f"Processed {pdf_file['name']}: {len(sheet_rows)} rows added ({len(bill_rows)} bills)", "SUCCESS")
                        else:
                            stats['failed_pdfs'] += 1
                            self.log(f"Failed to append data for {pdf_file['name']}", "ERROR")
                    
                    except Exception as e:
                        self.log(f"LlamaParse extraction failed for {pdf_file['name']}: {str(e)}", "ERROR")
                        stats['failed_pdfs'] += 1
                        stats['llama_errors'] += 1
                    
                    finally:
                        if os.path.exists(tmp_path):
                            os.remove(tmp_path)
                    
                except Exception as e:
                    self.log(f"Failed to process {pdf_file.get('name', 'unknown')}: {str(e)}", "ERROR")
                    stats['failed_pdfs'] += 1
            
            if status_callback:
                status_callback("Step 5/5: Finalizing processing...")
            
            if progress_callback:
                progress_callback(90)
            
            self.log("Payment Advice Processing Complete", "SUCCESS")
            
            if progress_callback:
                progress_callback(100)
            
            return stats
            
        except Exception as e:
            self.log(f"Payment Advice workflow failed: {str(e)}", "ERROR")
            return stats
    
    def log_workflow_to_sheet(self, workflow_name: str, start_time: datetime, 
                             end_time: datetime, stats: dict):
        """Log workflow execution details to Google Sheet"""
        try:
            duration = (end_time - start_time).total_seconds()
            duration_str = f"{duration:.2f}s"
            
            if duration >= 60:
                minutes = int(duration // 60)
                seconds = int(duration % 60)
                duration_str = f"{minutes}m {seconds}s"
            
            if workflow_name == "Mail to Drive":
                log_row = [
                    start_time.strftime("%Y-%m-%d %H:%M:%S"),
                    end_time.strftime("%Y-%m-%d %H:%M:%S"),
                    duration_str,
                    workflow_name,
                    stats.get('total_emails', 0),
                    stats.get('processed_emails', 0),
                    stats.get('total_attachments', 0),
                    stats.get('failed', 0),
                    len(stats.get('attachments_info', [])),
                    "Success" if stats.get('success', False) else "Failed"
                ]
            else:
                log_row = [
                    start_time.strftime("%Y-%m-%d %H:%M:%S"),
                    end_time.strftime("%Y-%m-%d %H:%M:%S"),
                    duration_str,
                    workflow_name,
                    stats.get('total_pdfs', 0),
                    stats.get('processed_pdfs', 0),
                    stats.get('skipped_pdfs', 0),
                    stats.get('failed_pdfs', 0),
                    stats.get('rows_added', 0),
                    "Success" if stats.get('processed_pdfs', 0) > 0 else "Failed"
                ]
            
            log_config = DEFAULT_CONFIG['workflow_log']
            
            log_headers = self.get_sheet_headers(log_config['spreadsheet_id'], log_config['sheet_range'])
            if not log_headers:
                header_row = [
                    "Start Time", "End Time", "Duration", "Workflow", 
                    "Total Items Found", "Items Processed", "Items Skipped/Uploaded", 
                    "Failed Items", "Rows Added/Attachments", "Status"
                ]
                self.append_to_google_sheet(
                    log_config['spreadsheet_id'], 
                    log_config['sheet_range'], 
                    [header_row]
                )
            
            self.append_to_google_sheet(
                log_config['spreadsheet_id'],
                log_config['sheet_range'],
                [log_row]
            )
            
            self.log(f"Logged workflow: {workflow_name}", "INFO")
            
        except Exception as e:
            self.log(f"Failed to log workflow: {str(e)}", "ERROR")
    
    def run_scheduled_workflow(self, progress_callback=None, status_callback=None):
        """Run both workflows in sequence and log results"""
        try:
            overall_start = datetime.now(timezone.utc)
            
            if status_callback:
                status_callback("Starting Mail to Drive workflow...")
            
            mail_start = datetime.now(timezone.utc)
            mail_stats = self.process_mail_to_drive_workflow(
                DEFAULT_CONFIG['mail'],
                progress_callback=progress_callback,
                status_callback=status_callback
            )
            mail_end = datetime.now(timezone.utc)
            self.log_workflow_to_sheet("Mail to Drive", mail_start, mail_end, mail_stats)
            
            time.sleep(2)
            
            if status_callback:
                status_callback("Starting Payment Advice processing workflow...")
            
            sheet_start = datetime.now(timezone.utc)
            sheet_stats = self.process_payment_advice_workflow(
                DEFAULT_CONFIG['payment_advice'],
                progress_callback=progress_callback,
                status_callback=status_callback
            )
            sheet_end = datetime.now(timezone.utc)
            self.log_workflow_to_sheet("Payment Advice Processing", sheet_start, sheet_end, sheet_stats)
            
            overall_end = datetime.now(timezone.utc)
            total_duration = (overall_end - overall_start).total_seconds()
            
            self.log(f"Total Duration: {total_duration:.2f} seconds", "INFO")
            self.log(f"Gmail Search: Found {mail_stats['total_emails']} emails", "INFO")
            self.log(f"Drive Upload: Uploaded {mail_stats['total_attachments']} PDF attachments", "INFO")
            self.log(f"Filtering: {sheet_stats['skipped_pdfs']} PDFs already in sheet (filtered out)", "INFO")
            self.log(f"Processing: {sheet_stats['processed_pdfs']} PDFs processed with LlamaParse", "INFO")
            self.log(f"Data Added: {sheet_stats['rows_added']} rows added to Google Sheets", "INFO")
            
            return {
                'success': True,
                'mail_stats': mail_stats,
                'sheet_stats': sheet_stats,
                'total_duration': total_duration
            }
            
        except Exception as e:
            self.log(f"Scheduled workflow failed: {str(e)}", "ERROR")
            return {'success': False, 'error': str(e)}


def main():
    """Main Streamlit application"""
    st.title("💰 Namdhari Payment Advice Processor")
    st.markdown("### Fixed version with proper date conversion and single source_file column")
    
    # Initialize processor instance in session state
    if 'processor' not in st.session_state:
        st.session_state.processor = NamdhariPaymentAdviceProcessor()
    
    # Initialize workflow running state
    if 'workflow_running' not in st.session_state:
        st.session_state.workflow_running = False
    
    processor = st.session_state.processor
    config = processor.get_config()
    
    # Sidebar configuration
    st.sidebar.header("Configuration")
    
    # Authentication section
    st.sidebar.subheader("🔐 Authentication")
    auth_status = st.sidebar.empty()
    
    if not processor.gmail_service or not processor.drive_service or not processor.sheets_service:
        if st.sidebar.button("🚀 Authenticate with Google", type="primary"):
            progress_bar = st.sidebar.progress(0)
            status_text = st.sidebar.empty()
            
            success = processor.authenticate_from_secrets(progress_bar, status_text)
            if success:
                auth_status.success("✅ Authenticated successfully!")
                st.sidebar.success("Ready to process workflows!")
            else:
                auth_status.error("❌ Authentication failed")
            
            progress_bar.empty()
            status_text.empty()
    else:
        auth_status.success("✅ Already authenticated")
        
        # Clear authentication button
        if st.sidebar.button("🔄 Re-authenticate"):
            if 'oauth_token' in st.session_state:
                del st.session_state.oauth_token
            st.session_state.processor = NamdhariPaymentAdviceProcessor()
            st.rerun()
    
    # Main content
    if not processor.gmail_service or not processor.drive_service or not processor.sheets_service:
        st.warning("⚠️ Please authenticate first using the sidebar")
        return
    
    # Configuration form
    st.header("⚙️ Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Gmail Settings")
        gmail_sender = st.text_input(
            "Sender Email",
            value=config['mail']['sender'],
            help="Email address to search for (e.g., erp@namdharis.in)",
            key="gmail_sender"
        )
        gmail_search_term = st.text_input(
            "Search Term",
            value=config['mail']['search_term'],
            help="Text to search for in email subject/body",
            key="gmail_search_term"
        )
        gmail_days_back = st.number_input(
            "Days Back",
            min_value=1,
            max_value=365,
            value=config['mail']['days_back'],
            help="Number of days to search back",
            key="gmail_days_back"
        )
        gmail_max_results = st.number_input(
            "Max Results",
            min_value=1,
            max_value=500,
            value=config['mail']['max_results'],
            help="Maximum number of emails to process",
            key="gmail_max_results"
        )
    
    with col2:
        st.subheader("Payment Advice Settings")
        llama_api_key = st.text_input(
            "LlamaParse API Key",
            value=config['payment_advice']['llama_api_key'],
            help="Your LlamaParse API key",
            key="llama_api_key",
            type="password"
        )
        llama_agent = st.text_input(
            "LlamaParse Agent",
            value=config['payment_advice']['llama_agent'],
            help="Name of the LlamaParse agent",
            key="llama_agent"
        )
        spreadsheet_id = st.text_input(
            "Spreadsheet ID",
            value=config['payment_advice']['spreadsheet_id'],
            help="ID of the Google Sheets spreadsheet",
            key="spreadsheet_id"
        )
        sheet_range = st.text_input(
            "Sheet Range",
            value=config['payment_advice']['sheet_range'],
            help="Sheet name and range (e.g., 'payment_advice')",
            key="sheet_range"
        )
    
    # Update configuration button
    if st.button("📝 Update Configuration", type="secondary"):
        new_config = {
            'mail': {
                'gdrive_folder_id': config['mail']['gdrive_folder_id'],
                'sender': gmail_sender,
                'search_term': gmail_search_term,
                'days_back': gmail_days_back,
                'max_results': gmail_max_results
            },
            'payment_advice': {
                'llama_api_key': llama_api_key,
                'llama_agent': llama_agent,
                'drive_folder_id': config['payment_advice']['drive_folder_id'],
                'spreadsheet_id': spreadsheet_id,
                'sheet_range': sheet_range,
                'days_back': config['payment_advice']['days_back'],
                'max_files': config['payment_advice']['max_files'],
                'skip_existing': config['payment_advice']['skip_existing']
            },
            'workflow_log': config['workflow_log']
        }
        processor.update_config(new_config)
        st.success("✅ Configuration updated successfully!")
        st.rerun()
    
    st.divider()
    
    # Workflow section
    st.header("🚀 Process Payment Advices")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📧 Mail to Drive Only", disabled=st.session_state.workflow_running):
            if st.session_state.workflow_running:
                st.warning("Workflow is already running. Please wait for it to complete.")
            else:
                st.session_state.workflow_running = True
                try:
                    progress_container = st.container()
                    with progress_container:
                        st.subheader("📊 Mail to Drive Workflow")
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        def update_progress(value):
                            progress_bar.progress(value)
                        
                        def update_status(message):
                            status_text.text(message)
                        
                        current_config = processor.get_config()
                        result = processor.process_mail_to_drive_workflow(
                            current_config['mail'],
                            progress_callback=update_progress,
                            status_callback=update_status
                        )
                        
                        if result['success']:
                            st.success(f"✅ Mail to Drive workflow completed!")
                            st.info(f"**Summary:**\n"
                                   f"- Emails found: {result.get('total_emails', 0)}\n"
                                   f"- Emails processed: {result.get('processed_emails', 0)}\n"
                                   f"- PDF attachments uploaded: {result.get('total_attachments', 0)}\n"
                                   f"- Failed: {result.get('failed', 0)}")
                        else:
                            st.error(f"❌ Mail to Drive workflow failed")
                finally:
                    st.session_state.workflow_running = False
    
    with col2:
        if st.button("📄 Process PDFs Only", disabled=st.session_state.workflow_running):
            if st.session_state.workflow_running:
                st.warning("Workflow is already running. Please wait for it to complete.")
            else:
                st.session_state.workflow_running = True
                try:
                    progress_container = st.container()
                    with progress_container:
                        st.subheader("📊 PDF Processing Workflow")
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        def update_progress(value):
                            progress_bar.progress(value)
                        
                        def update_status(message):
                            status_text.text(message)
                        
                        current_config = processor.get_config()
                        result = processor.process_payment_advice_workflow(
                            current_config['payment_advice'],
                            progress_callback=update_progress,
                            status_callback=update_status
                        )
                        
                        if result['processed_pdfs'] > 0:
                            st.success(f"✅ PDF Processing workflow completed!")
                            st.info(f"**Summary:**\n"
                                   f"- PDFs found: {result.get('total_pdfs', 0)}\n"
                                   f"- PDFs processed: {result.get('processed_pdfs', 0)}\n"
                                   f"- PDFs skipped: {result.get('skipped_pdfs', 0)}\n"
                                   f"- PDFs failed: {result.get('failed_pdfs', 0)}\n"
                                   f"- Rows added: {result.get('rows_added', 0)}\n"
                                   f"- Bills processed: {result.get('bills_processed', 0)}")
                        else:
                            st.warning(f"⚠️ PDF Processing workflow completed with no new PDFs processed")
                finally:
                    st.session_state.workflow_running = False
    
    with col3:
        if st.button("🔄 Full Workflow", type="primary", disabled=st.session_state.workflow_running):
            if st.session_state.workflow_running:
                st.warning("Workflow is already running. Please wait for it to complete.")
            else:
                st.session_state.workflow_running = True
                try:
                    progress_container = st.container()
                    with progress_container:
                        st.subheader("📊 Full Workflow")
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        def update_progress(value):
                            progress_bar.progress(value)
                        
                        def update_status(message):
                            status_text.text(message)
                        
                        result = processor.run_scheduled_workflow(
                            progress_callback=update_progress,
                            status_callback=update_status
                        )
                        
                        if result['success']:
                            st.success(f"✅ Full workflow completed!")
                            mail_stats = result.get('mail_stats', {})
                            sheet_stats = result.get('sheet_stats', {})
                            
                            st.info(f"**Summary:**\n"
                                   f"- Duration: {result.get('total_duration', 0):.2f} seconds\n"
                                   f"- Emails found: {mail_stats.get('total_emails', 0)}\n"
                                   f"- PDF attachments uploaded: {mail_stats.get('total_attachments', 0)}\n"
                                   f"- PDFs processed: {sheet_stats.get('processed_pdfs', 0)}\n"
                                   f"- Rows added: {sheet_stats.get('rows_added', 0)}")
                        else:
                            st.error(f"❌ Full workflow failed: {result.get('error', 'Unknown error')}")
                finally:
                    st.session_state.workflow_running = False
    
    st.divider()
    
    # Logs section
    st.header("📋 System Logs")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("🔄 Refresh Logs", key="refresh_logs"):
            st.rerun()
    with col2:
        if st.button("🗑️ Clear Logs", key="clear_logs"):
            processor.clear_logs()
            st.success("Logs cleared!")
            st.rerun()
    
    # Display logs
    logs = processor.get_logs()
    
    if logs:
        st.subheader(f"Recent Activity ({len(logs)} entries)")
        
        # Show logs in reverse chronological order (newest first)
        for log_entry in reversed(logs[-30:]):  # Show last 30 logs
            timestamp = log_entry['timestamp']
            level = log_entry['level']
            message = log_entry['message']
            
            # Color coding based on log level
            if level == "ERROR":
                st.error(f"🔴 **{timestamp}** - {message}")
            elif level == "WARNING":
                st.warning(f"🟡 **{timestamp}** - {message}")
            elif level == "SUCCESS":
                st.success(f"🟢 **{timestamp}** - {message}")
            elif level == "DEBUG":
                st.text(f"⚫ **{timestamp}** - {message}")
            else:  # INFO
                st.info(f"ℹ️ **{timestamp}** - {message}")
    else:
        st.info("No logs available. Start a workflow to see activity logs here.")
    
    # System status
    st.subheader("🔧 System Status")
    status_cols = st.columns(4)
    
    with status_cols[0]:
        st.metric("Gmail Status", 
                 "✅ Connected" if processor.gmail_service else "❌ Not Connected")
    with status_cols[1]:
        st.metric("Drive Status", 
                 "✅ Connected" if processor.drive_service else "❌ Not Connected")
    with status_cols[2]:
        st.metric("Sheets Status", 
                 "✅ Connected" if processor.sheets_service else "❌ Not Connected")
    with status_cols[3]:
        st.metric("Workflow Status", 
                 "🟡 Running" if st.session_state.workflow_running else "🟢 Idle")


# Run the application
if __name__ == "__main__":
    main()
