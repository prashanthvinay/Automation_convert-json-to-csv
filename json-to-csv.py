import awswrangler as wr

def lambda_handler(event, context):
    try:
        bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        s3_file_name = event["Records"][0]["s3"]["object"]["key"]
        print('Bucket Name:' + bucket_name)
        print('S3 File Name:' + s3_file_name)
      
        json_file = 's3://' + bucket_name + '/' + s3_file_name
        print('JSON File:' + json_file)
        
        data_frame = wr.s3.read_json(path=json_file, orient='records', lines=True)
        print(data_frame)
        
        new_s3_file_name = s3_file_name.replace('json', 'csv')
        write_to_path =  's3://' + bucket_name + '/' +  new_s3_file_name
        
        wr.s3.to_csv(data_frame, path=write_to_path, sep=',')
        print('CSV File uploaded to ' + write_to_path)
        
    except Exception as err:
        print(err)
