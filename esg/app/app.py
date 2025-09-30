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
    page_title="Document Uploader", 
    page_icon="�", 
    layout="wide"
)

def create_databricks_client():
    """Create a Databricks WorkspaceClient with automatic authentication."""
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
        st.error(f"Failed to create Databricks client: {str(e)}")
        return None

def upload_document_to_volume(client, file_content, file_name, base_volume_path):
    """Upload document file to Databricks volume with automatic folder organization."""
    temp_dir = None
    try:
        # Validate file content
        if not file_content:
            raise ValueError("File content is empty")
        
        # Log initial file info
        logger.info(f"Starting upload for {file_name}, content type: {type(file_content)}, size: {len(file_content)} bytes")
        
        # For PDF files, let's verify the content starts with PDF header
        if file_name.lower().endswith('.pdf'):
            if not file_content.startswith(b'%PDF-'):
                logger.warning(f"PDF file {file_name} does not start with PDF header")
                logger.info(f"First 20 bytes: {file_content[:20]}")
            else:
                logger.info(f"PDF header verified for {file_name}")
        
        # Ensure base volume path ends with /
        if not base_volume_path.endswith('/'):
            base_volume_path += '/'
        
        # Determine subdirectory based on file extension
        file_extension = file_name.lower().split('.')[-1]
        if file_extension == 'pdf':
            volume_path = f"{base_volume_path}pdf/"
        elif file_extension == 'csv':
            volume_path = f"{base_volume_path}csv/"
        elif file_extension in ['png', 'jpg', 'jpeg']:
            volume_path = f"{base_volume_path}images/"
        else:
            volume_path = base_volume_path  # Default to root if unknown type
        
        # Create full path for the file
        full_path = f"{volume_path}{file_name}"
        
        # Try a more direct approach - write content directly using workspace files API
        logger.info(f"Attempting direct upload to {full_path}")
        
        # Create temp file in a specific location with better control
        temp_dir = tempfile.mkdtemp()
        temp_file_path = os.path.join(temp_dir, file_name)
        
        # Write file with explicit binary mode and ensure all data is written
        with open(temp_file_path, 'wb') as f:
            if isinstance(file_content, str):
                file_content = file_content.encode('utf-8')
            f.write(file_content)
            f.flush()
            os.fsync(f.fileno())  # Force write to disk
        
        # Verify the written file
        actual_size = os.path.getsize(temp_file_path)
        logger.info(f"Created temp file {temp_file_path} with size {actual_size}")
        
        if actual_size != len(file_content):
            raise ValueError(f"Temp file size mismatch: expected {len(file_content)}, got {actual_size}")
        
        # Double-check PDF header in temp file
        if file_name.lower().endswith('.pdf'):
            with open(temp_file_path, 'rb') as verify_file:
                first_20 = verify_file.read(20)
                logger.info(f"Temp file PDF header: {first_20}")
                if not first_20.startswith(b'%PDF-'):
                    raise ValueError("Temp file PDF header is corrupted")
        
        # Upload using databricks files API with correct parameters
        logger.info(f"Uploading {temp_file_path} to {full_path} (size: {actual_size})")
        
        # Use the correct API signature: upload(file_path, contents, overwrite)
        # Open the temp file as a binary stream and pass it to the API
        with open(temp_file_path, 'rb') as file_handle:
            client.files.upload(full_path, file_handle, overwrite=True)
        
        logger.info(f"Successfully uploaded {file_name} to {full_path}")
        
        # Clean up temp directory
        shutil.rmtree(temp_dir)
        logger.info(f"Cleaned up temp directory: {temp_dir}")
        
        return full_path
            
    except Exception as e:
        logger.error(f"Failed to upload document: {str(e)}")
        # Clean up on error
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        raise e

def main():
    st.title("📄 Document Uploader")
    st.markdown("Upload documents to Databricks volume with **automatic folder organization**:")
    st.markdown("• PDFs → `/pdf/` • CSVs → `/csv/` • Images → `/images/`")
    
    # Check connection
    client = create_databricks_client()
    if client:
        st.success("🟢 Connected to Databricks")
    else:
        st.error("🔴 Failed to connect to Databricks")
        st.stop()
    
    st.divider()
    
    # File uploader
    st.header("📤 Upload Document")
    uploaded_file = st.file_uploader(
        "Choose a document to upload",
        type=['csv', 'pdf', 'png', 'jpg', 'jpeg'],
        help="Supported formats: CSV, PDF, PNG, JPG"
    )
    
    if uploaded_file is not None:
        # Read file content once and store it
        file_content = uploaded_file.read()
        uploaded_file.seek(0)  # Reset for any preview operations
        
        # Validate file content immediately
        if not file_content:
            st.error("❌ The uploaded file appears to be empty")
            return
        
        # For PDF files, verify it has proper PDF header
        if uploaded_file.name.lower().endswith('.pdf'):
            if not file_content.startswith(b'%PDF-'):
                st.error("❌ The uploaded file does not appear to be a valid PDF")
                st.info(f"File starts with: {file_content[:20]}")
                return
            else:
                st.success("✅ Valid PDF file detected")
        
        # Display file information
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.subheader("File Information")
            st.write(f"**Name:** {uploaded_file.name}")
            st.write(f"**Type:** {uploaded_file.type}")
            st.write(f"**Size:** {uploaded_file.size:,} bytes")
            st.write(f"**Content Size:** {len(file_content):,} bytes")
            
            # Check if sizes match
            if uploaded_file.size != len(file_content):
                st.warning(f"⚠️ Size mismatch: declared {uploaded_file.size}, read {len(file_content)}")
            
            # Preview for different file types
            if uploaded_file.name.endswith('.csv') or uploaded_file.type == 'text/csv':
                if st.checkbox("Preview CSV content"):
                    try:
                        # Create a new file-like object from the content for preview
                        import io
                        csv_content = io.StringIO(file_content.decode('utf-8'))
                        df = pd.read_csv(csv_content)
                        st.dataframe(df.head(10))
                    except Exception as e:
                        st.warning(f"Cannot preview this CSV file: {str(e)}")
            elif uploaded_file.name.endswith(('.png', '.jpg', '.jpeg')) or uploaded_file.type.startswith('image/'):
                if st.checkbox("Preview image"):
                    try:
                        # Create a new file-like object from the content for preview
                        import io
                        image_content = io.BytesIO(file_content)
                        st.image(image_content, caption=uploaded_file.name, use_column_width=True)
                    except Exception as e:
                        st.warning(f"Cannot preview this image file: {str(e)}")
            elif uploaded_file.name.endswith('.pdf'):
                st.info("PDF preview not available - file will be uploaded as-is")
                # Show PDF header info for debugging
                if file_content.startswith(b'%PDF-'):
                    pdf_version = file_content[:8].decode('ascii', errors='ignore')
                    st.code(f"PDF Header: {pdf_version}")
                    st.write(f"**First 50 bytes (hex):** {file_content[:50].hex()}")
        
        with col2:
            st.subheader("Upload Actions")
            
            if st.button("📤 Upload to Volume", type="primary", use_container_width=True):
                try:
                    with st.spinner(f"Uploading {uploaded_file.name}..."):
                        base_volume_path = "/Volumes/hackathon_zurich_2025/a_team/landing_data/"
                        
                        # Log file info for debugging
                        logger.info(f"Uploading file: {uploaded_file.name}, size: {len(file_content)} bytes, type: {type(file_content)}")
                        
                        # Verify we have content
                        if not file_content:
                            st.error("❌ File appears to be empty or could not be read")
                            return
                        
                        uploaded_path = upload_document_to_volume(
                            client, 
                            file_content, 
                            uploaded_file.name, 
                            base_volume_path
                        )
                        
                        st.success(f"✅ Successfully uploaded '{uploaded_file.name}' to {uploaded_path}")
                        st.balloons()
                            
                except Exception as e:
                    st.error(f"❌ Upload failed: {str(e)}")
    
    # Information section
    with st.expander("ℹ️ Information"):
        st.markdown("""
        **Target Volume:** `/Volumes/hackathon_zurich_2025/a_team/landing_data/`
        
        **Automatic Folder Organization:**
        - **PDF files** → `/Volumes/hackathon_zurich_2025/a_team/landing_data/pdf/`
        - **CSV files** → `/Volumes/hackathon_zurich_2025/a_team/landing_data/csv/`
        - **Image files** → `/Volumes/hackathon_zurich_2025/a_team/landing_data/images/`
        
        **Authentication:**
        - Local: Uses 'hackat' profile from ~/.databrickscfg
        - Cloud: Uses automatic Databricks authentication
        
        **Supported File Types:**
        - **CSV**: Data files with preview capability
        - **PDF**: Document files (uploaded as-is)
        - **PNG/JPG**: Image files with preview capability
        
        **Features:**
        - File preview before upload
        - Automatic folder organization by file type
        - Direct upload to Databricks volume
        - Overwrite existing files with same name
        """)

if __name__ == "__main__":
    main()
