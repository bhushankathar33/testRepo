import streamlit as st
import snowflake.connector
import boto3
import pandas as pd

# Snowflake Connection Parameters
snowflake_account = 'your_account'
snowflake_user = 'your_user'
snowflake_password = 'your_password'
snowflake_database = 'your_database'
snowflake_schema = 'your_schema'

# Function to connect to Snowflake
def connect_to_snowflake():
    return snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        database=snowflake_database,
        schema=snowflake_schema
    )

# Function to list Snowflake Data Stages
def list_data_stages():
    conn = connect_to_snowflake()
    cursor = conn.cursor()
    cursor.execute("SHOW STAGES")
    stages = [row[1] for row in cursor.fetchall()]
    conn.close()
    return stages

# Function to list files on S3 bucket
def list_files_s3(bucket_name):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    files = []
    for obj in bucket.objects.all():
        files.append({
            'Name': obj.key,
            'Size (bytes)': obj.size,
            'Last Modified': obj.last_modified
        })
    return files

# Main Streamlit app
def main():
    st.title('Data Exploration App')

    # Sidebar for selecting Snowflake stage and S3 bucket
    snowflake_stages = list_data_stages()
    selected_stage = st.sidebar.selectbox('Select Snowflake Stage', snowflake_stages)
    selected_bucket = st.sidebar.text_input('Enter S3 Bucket Name')

    # Display files in selected S3 bucket
    if selected_bucket:
        files = list_files_s3(selected_bucket)
        st.write('Files in S3 Bucket:')
        st.write(pd.DataFrame(files))

    # File selection and preview
    selected_file = st.selectbox('Select File', files['Name'])
    if selected_file:
        file_contents = pd.read_csv(f's3://{selected_bucket}/{selected_file}')
        st.write('Preview of File Contents:')
        st.write(file_contents)

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

if __name__ == "__main__":
    main()

