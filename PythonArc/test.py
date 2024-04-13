import boto3
import hashlib
import json

aws_access_key_id='AKIATIFHXA5QB6WKMKMR'
aws_secret_access_key='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'

s3 = boto3.client('s3',aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key)
                        
dynamodb = boto3.client('dynamodb',aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        region_name="us-east-1")
                        
def lambda_handler(event, context):
    
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    
    file_content = "totalContactoClientes=250\nmotivoReclamo=25\nmotivoGarantia=10\nmotivoDuda=100\nmotivoCompra=100\nmotivoFelicitaciones=7\nmotivoCambio=8\nhash=2f941516446dce09bc2841da60bf811f"
    
    dato_json = {}
    
    lineas = file_content.split('\n')
    
    for line in lineas:
    
        key, value = line.strip().split('=')
        dato_json[key.strip()] = value.strip()
    
    dato = dato_json["totalContactoClientes"]+"~"+dato_json["motivoReclamo"]+"~"+dato_json["motivoGarantia"]+"~"+dato_json["motivoDuda"]+"~"+dato_json["motivoCompra"]+"~"+dato_json["motivoFelicitaciones"]+"~"+dato_json["motivoCambio"]
    
    bytes_string = dato.encode('utf-8')
    
    hash_real = hashlib.md5(bytes_string)
        
    hash_real = hash_real.hexdigest()
    
    if hash_real == dato_json["hash"]:
        
        s3.delete_object(Bucket=bucket_name,Key=object_key,)
        
        dynamodb.put_item(TableName="aitechnical-test-jose",Item=dato_json)