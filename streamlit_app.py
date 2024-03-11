import boto3
import json
import streamlit as st
# Set the AWS credentials
import pandas as pd
import yaml

#config = yaml.safe_load(r'C:\Users\BHUSHAN\Downloads\hacathonWorkspace\config.yaml')

def load_config(config_path=r'C:\Users\BHUSHAN\Downloads\hacathonWorkspace\config.yaml'):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

config = load_config()

# Create an S3 client
s3 = boto3.client('s3', aws_access_key_id=config['aws_access_key_id'], aws_secret_access_key=config['aws_secret_access_key'])

# Create an S3 resource
s3_resource = boto3.resource('s3', aws_access_key_id=config['aws_access_key_id'], aws_secret_access_key=config['aws_secret_access_key'])

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
    file_contents = pd.DataFrame()

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
            file_contents = read_file_from_s3(selected_bucket,selected_file_name)
            file_contents = pd.read_csv(file_contents)
            st.write('Preview of File Contents:')
            st.write(file_contents)

            selected_rows = st.multiselect('Select Rows to Ingest', file_contents.index)
            if st.button('Ingest Selected Rows'):
                if selected_rows:
                    rows_to_ingest = file_contents.loc[selected_rows]
                    #ingest_data_into_snowflake(rows_to_ingest)
                    print("======rows_to_ingest======")
                    print(rows_to_ingest)
                    st.write(rows_to_ingest)
                    st.success('Data ingested successfully.')
                else:
                    st.warning('Please select at least one row to ingest.')    
    
    # Data profiling
    if file_contents is not None:
        selected_column = st.selectbox('Select Column', file_contents.columns)
        if selected_column:
            column_data = file_contents[selected_column]
            st.write('Data Profile:')
            st.write(column_data.describe())
    
    # Data ingestion
    if st.button('Ingest Data'):
        # Logic for ingesting data into database table
        pass
        

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

def read_file_from_s3(bucket_name, file_name):
    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
    data = obj['Body']
    return data

if __name__ == "__main__":
    main()
