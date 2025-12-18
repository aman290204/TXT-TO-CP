import os.path
import pickle
import base64
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import logging

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive.file']

def get_drive_service():
    """Shows basic usage of the Drive v3 API.
    """
    creds = None
    
    # Check for Base64 token in env var (Fix for Render/Heroku binary file issues)
    if os.environ.get('TOKEN_PICKLE'):
        try:
            print("Found TOKEN_PICKLE env var. Decoding...")
            with open('token.pickle', 'wb') as token:
                token.write(base64.b64decode(os.environ.get('TOKEN_PICKLE')))
            print("Successfully restored token.pickle from env var.")
        except Exception as e:
            print(f"Error decoding TOKEN_PICKLE env var: {e}")

    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            # Use run_local_server for easier auth if possible, or console
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('drive', 'v3', credentials=creds)
    return service

def create_folder(service, folder_name, parent_id=None):
    """Create a folder on Drive."""
    try:
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        if parent_id:
            file_metadata['parents'] = [parent_id]

        file = service.files().create(body=file_metadata,
                                      fields='id').execute()
        logging.info(f'Created folder: {folder_name} ({file.get("id")})')
        return file.get('id')
    except Exception as e:
        logging.error(f"An error occurred creating folder: {e}")
        return None

def upload_file(service, file_path, folder_id, file_name, progress_callback=None):
    """Uploads a file to a specific folder on Drive."""
    try:
        # Thread Stability Fix: Create fresh service if None passed
        if service is None:
            service = get_drive_service()

        # Check if file exists first
        if not os.path.exists(file_path):
            error_msg = f"File not found: {file_path}"
            print(f"❌ DRIVE ERROR: {error_msg}")
            logging.error(error_msg)
            return None
            
        file_metadata = {
            'name': file_name,
            'parents': [folder_id]
        }
        # Use resumable upload for large files
        media = MediaFileUpload(file_path, resumable=True)
        
        request = service.files().create(body=file_metadata,
                                         media_body=media,
                                         fields='id')
        
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                # Calculate progress
                prog = int(status.progress() * 100)
                if progress_callback:
                     # This might need to be async wrapper if calling from async loop
                     # But basic callback works for printing/logging
                     progress_callback(prog, 100) # Simulating current/total percentage

        logging.info(f'File ID: {response.get("id")}')
        print(f"✅ Uploaded to Drive: {file_name} (ID: {response.get('id')})")
        return response.get('id')
    except Exception as e:
        error_msg = f"An error occurred uploading file '{file_name}': {e}"
        print(f"❌ DRIVE ERROR: {error_msg}")
        logging.error(error_msg)
        return None
