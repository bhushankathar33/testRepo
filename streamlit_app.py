import boto3
import json
import streamlit as st
# Set the AWS credentials
import pandas as pd

# Create an S3 client
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

# Create an S3 resource
s3_resource = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

def main():
    print("bhushan")
    st.title('Data Exploration App')

    # List all the buckets in your account
    buckets = s3.list_buckets()

    # Print the names of all the buckets
    for bucket in buckets['Buckets']:
        print(list_files_s3(bucket['Name']))
    
    selected_bucket = st.sidebar.text_input('Enter S3 Bucket Name')

    files = []

    # Display files in selected S3 bucket
    if selected_bucket:
        files = list_files_s3(selected_bucket)
        st.write('Files in S3 Bucket:')
        st.write(pd.DataFrame(files))

    # File selection and preview
    selected_file_name = st.selectbox('Select File', [file['Name'] for file in files])
    if selected_file_name:
        selected_file = next((file for file in files if file['Name'] == selected_file_name), None)
        if selected_file:
            file_contents = pd.read_csv(f's3://{selected_bucket}/{selected_file_name}')
            st.write('Preview of File Contents:')
            st.write(file_contents)
        

# Function to list files on S3 bucket
def list_files_s3(bucket_name):
    bucket_resource = s3_resource.Bucket(bucket_name)
    files = []
    for obj in bucket_resource.objects.all():        
        files.append({
            'Name': obj.key,
            'Size (bytes)': obj.size,
            'Last Modified': obj.last_modified
        })
    return files

if __name__ == "__main__":
    main()
