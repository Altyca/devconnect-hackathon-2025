import streamlit as st
import pandas as pd
import tempfile
import os
import shutil
from datetime import datetime
from databricks.sdk import WorkspaceClient
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="ESG Data Hub", 
    page_icon="🌱", 
    layout="wide"
)

def create_databricks_client():
    """Create a secure connection to the cloud platform."""
    try:
        # Try to use 'hackat' profile for local development
        try:
            client = WorkspaceClient(profile='hackat')
            logger.info("Using 'hackat' profile for authentication")
            return client
        except:
            # Fallback to default authentication (for cloud deployment)
            client = WorkspaceClient()
            logger.info("Using default cloud authentication")
            return client
    except Exception as e:
        logger.error(f"Failed to create client: {str(e)}")
        return None

def upload_document_to_volume(client, file_content, file_name, base_volume_path):
    """Upload file to secure cloud storage with automatic organization."""
    temp_dir = None
    try:
        # Validate file content
        if not file_content:
            raise ValueError("File content is empty")
        
        logger.info(f"Processing upload for {file_name}")
        
        # Ensure base volume path ends with /
        if not base_volume_path.endswith('/'):
            base_volume_path += '/'
        
        # Determine subdirectory based on file extension
        file_extension = file_name.lower().split('.')[-1]
        if file_extension == 'pdf':
            volume_path = f"{base_volume_path}pdf/"
        elif file_extension == 'csv':
            volume_path = f"{base_volume_path}csv/"
        else:
            volume_path = base_volume_path  # Default to root if unknown type
        
        # Create full path for the file
        full_path = f"{volume_path}{file_name}"
        
        logger.info(f"Uploading to {full_path}")
        
        # Ensure the directory exists first
        try:
            client.files.create_directory(volume_path)
            logger.info(f"Created/verified directory: {volume_path}")
        except Exception as e:
            logger.warning(f"Could not create directory {volume_path}: {str(e)}")
        
        # Create temp file for upload
        temp_dir = tempfile.mkdtemp()
        temp_file_path = os.path.join(temp_dir, file_name)
        
        # Write file content
        with open(temp_file_path, 'wb') as f:
            if isinstance(file_content, str):
                file_content = file_content.encode('utf-8')
            f.write(file_content)
            f.flush()
            os.fsync(f.fileno())
        
        # Upload to cloud platform
        with open(temp_file_path, 'rb') as file_handle:
            client.files.upload(full_path, file_handle, overwrite=True)
        
        logger.info(f"Successfully uploaded {file_name}")
        
        # Clean up
        shutil.rmtree(temp_dir)
        
        return full_path
            
    except Exception as e:
        logger.error(f"Upload failed: {str(e)}")
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        raise e

def list_uploaded_files(client, base_volume_path):
    """List files from CSV and PDF folders."""
    try:
        files_info = {"csv": [], "pdf": []}
        
        # List CSV files
        try:
            csv_path = f"{base_volume_path}csv"
            csv_entries = list(client.files.list_directory_contents(csv_path))
            for entry in csv_entries:
                # DirectoryEntry objects: if it's not explicitly a directory, assume it's a file
                if not (hasattr(entry, 'is_directory') and entry.is_directory):
                    files_info["csv"].append({
                        "name": entry.name,
                        "path": entry.path,
                        "size": getattr(entry, 'file_size', 0),
                        "modified": getattr(entry, 'last_modified', None)
                    })
        except Exception as e:
            logger.warning(f"Could not list CSV files: {str(e)}")
        
        # List PDF files
        try:
            pdf_path = f"{base_volume_path}pdf"
            pdf_entries = list(client.files.list_directory_contents(pdf_path))
            for entry in pdf_entries:
                # DirectoryEntry objects: if it's not explicitly a directory, assume it's a file
                if not (hasattr(entry, 'is_directory') and entry.is_directory):
                    files_info["pdf"].append({
                        "name": entry.name,
                        "path": entry.path,
                        "size": getattr(entry, 'file_size', 0),
                        "modified": getattr(entry, 'last_modified', None)
                    })
        except Exception as e:
            logger.warning(f"Could not list PDF files: {str(e)}")
        
        return files_info
    except Exception as e:
        logger.error(f"Failed to list files: {str(e)}")
        return {"csv": [], "pdf": []}

def main():
    st.title("🌱 ESG Data Hub")
    st.markdown("**Upload your ESG data files securely to the cloud platform**")
    
    # Connection status
    client = create_databricks_client()
    if client:
        st.success("✅ Connected to secure cloud platform")
    else:
        st.error("❌ Connection failed - please contact IT support")
        st.stop()
    
    st.divider()
    
    # Simplified upload section
    st.header("Upload Data Files")
    
    uploaded_files = st.file_uploader(
        "Select your files",
        type=['csv', 'pdf'],
        help="Supported formats: CSV data files, PDF reports",
        accept_multiple_files=True
    )
    
    if uploaded_files:
        # Display summary of selected files
        st.write(f"**{len(uploaded_files)} file(s) selected:**")
        
        # Show file list and validate all files
        valid_files = []
        total_size = 0
        
        for uploaded_file in uploaded_files:
            file_content = uploaded_file.read()
            uploaded_file.seek(0)
            
            # Validate file
            is_valid = True
            error_msg = ""
            
            if not file_content:
                is_valid = False
                error_msg = "Empty file"
            elif uploaded_file.name.lower().endswith('.pdf') and not file_content.startswith(b'%PDF-'):
                is_valid = False
                error_msg = "Invalid PDF"
            
            if is_valid:
                valid_files.append((uploaded_file, file_content))
                total_size += uploaded_file.size
                
                # Show file info
                col1, col2, col3 = st.columns([2, 1, 1])
                with col1:
                    icon = "📊" if uploaded_file.name.lower().endswith('.csv') else "📄"
                    st.write(f"{icon} {uploaded_file.name}")
                with col2:
                    file_size_mb = uploaded_file.size / (1024 * 1024)
                    st.write(f"{file_size_mb:.2f} MB")
                with col3:
                    st.write("✅ Valid")
            else:
                # Show error for invalid files
                col1, col2, col3 = st.columns([2, 1, 1])
                with col1:
                    st.write(f"❌ {uploaded_file.name}")
                with col2:
                    st.write("")
                with col3:
                    st.write(f"❌ {error_msg}")
        
        # Show total size
        if valid_files:
            total_size_mb = total_size / (1024 * 1024)
            st.write(f"**Total size:** {total_size_mb:.2f} MB")
            
            # Upload button for all valid files
            if st.button("🚀 Upload All Files", type="primary", use_container_width=True):
                try:
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    base_volume_path = "/Volumes/hackathon_zurich_2025/a_team/landing_data/"
                    uploaded_files_list = []
                    failed_files = []
                    
                    for i, (uploaded_file, file_content) in enumerate(valid_files):
                        try:
                            status_text.text(f"Uploading {uploaded_file.name}...")
                            
                            uploaded_path = upload_document_to_volume(
                                client, 
                                file_content, 
                                uploaded_file.name, 
                                base_volume_path
                            )
                            
                            uploaded_files_list.append(uploaded_file.name)
                            
                        except Exception as e:
                            logger.error(f"Failed to upload {uploaded_file.name}: {str(e)}")
                            failed_files.append(uploaded_file.name)
                        
                        # Update progress
                        progress = (i + 1) / len(valid_files)
                        progress_bar.progress(progress)
                    
                    # Clear progress indicators
                    progress_bar.empty()
                    status_text.empty()
                    
                    # Show results
                    if uploaded_files_list:
                        st.success(f"✅ Successfully uploaded {len(uploaded_files_list)} file(s)!")
                        for filename in uploaded_files_list:
                            st.write(f"✅ {filename}")
                        st.balloons()
                    
                    if failed_files:
                        st.error(f"❌ Failed to upload {len(failed_files)} file(s):")
                        for filename in failed_files:
                            st.write(f"❌ {filename}")
                        st.write("Please try again or contact support.")
                            
                except Exception as e:
                    st.error(f"❌ Upload process failed. Please try again or contact support.")
        else:
            st.warning("⚠️ No valid files selected. Please check your files and try again.")
    
    st.divider()
    
    # File browser section
    st.header("📁 Uploaded Files")
    
    if st.button("🔄 Refresh File List", use_container_width=False):
        st.rerun()
    
    with st.spinner("Loading files..."):
        base_volume_path = "/Volumes/hackathon_zurich_2025/a_team/landing_data/"
        files_info = list_uploaded_files(client, base_volume_path)
    
    # Create tabs for different file types
    tab1, tab2 = st.tabs(["📊 CSV Data Files", "📄 PDF Reports"])
    
    with tab1:
        csv_files = files_info["csv"]
        if csv_files:
            st.write(f"**{len(csv_files)} CSV file(s) found**")
            
            # Create a simple list for better display
            for file in csv_files:
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.write(f"📊 **{file['name']}**")
                with col2:
                    st.write("CSV")
        else:
            st.info("No CSV files uploaded yet.")
    
    with tab2:
        pdf_files = files_info["pdf"]
        if pdf_files:
            st.write(f"**{len(pdf_files)} PDF file(s) found**")
            
            # Create a simple list for better display
            for file in pdf_files:
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.write(f"📄 **{file['name']}**")
                with col2:
                    st.write("PDF")
        else:
            st.info("No PDF files uploaded yet.")

if __name__ == "__main__":
    main()
