from minio import Minio
# from io import BytesIO, StringIO
import glob
import re
import os
import numpy
from numpy import random
import json 
import math
import time
import requests
import urllib3

#########################################
############# FUNCTIONS ################
#########################################

def removeFiles(check_str,find_obj,find_count):
    #if a file name has a matching find_obj (str value)
    #and meets or exceeds find_count then mark it to remove
    check_list = [m.start() for m in re.finditer(find_obj, check_str)]
    if len(check_list) >= find_count:
        return True
    else:
        return False

def convert_size(size_bytes):
    try:
        size_bytes = int(size_bytes)
        if size_bytes == 0:
            return "0B"
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        out = "%s %s" % (s, size_name[i])
    except:
        out = size_bytes
    return out
    
def connectMinIO(endpoint,key,secret):
    client = Minio(
        endpoint,
        key, 
        secret, 
        secure=True)
    return client
    
def generate_chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def getObjectList(client,bucket):
    object_list = []
    objects = client.list_objects(bucket, recursive=True)
    total_size = 0
    for obj in objects:
        total_size+=obj.size
        object_list.append([
            obj.object_name, 
            convert_size(obj.size),
        ])
    converted_size= convert_size(total_size)
    out=f'Directory size: {converted_size}' 
    return object_list, out


def makeBucket(client,bucket_name):
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
    
        out = f"Bucket '{bucket_name}' successfully created"
        return out
    else:
        out = f"Error: Bucket '{bucket_name}'already exists"
        return out
    
    

def createUserBucket(client,f_initial, last):
    bucket_name = f'{f_initial}.{last}'
    found = client.bucket_exists(bucket_name)
    print(f"Checking if bucket for {bucket_name} exists")
    if not found:
        client.make_bucket(bucket_name)
        out = f"New bucket '{bucket_name}' successfully created"
        return out
    else:
        print(f"{bucket_name} exists")
        pass

    
def upload_local_directory_to_minio(client,local_path, bucket_name, minio_path):
    try:
        assert os.path.isdir(local_path)
        # recursive finds all sub folders and their files
        files = glob.glob(local_path + '/**',recursive=True)
        # remove folder paths and keep only files
        temp = list(set([f for f in files if removeFiles(f,'\.',1)]))
        # remove files that have more then one period as they won't work with minio
        all_files = [a for a in temp if not removeFiles(a,'\.',2)]
        rmvd_count = len(temp) - len(all_files)
        if rmvd_count > 0:
            invalid = f"{rmvd_count} invalid file will not be uploaded"
            if rmvd_count > 1:
                invalid = invalid.replace("file","files")
            print(invalid)
        #batch files in groups of 10
        file_chunks = list(generate_chunks(all_files, 10))
        print(f"Valid File count: {len(all_files)}")
        print(f"Will upload in {len(file_chunks)} batches\n")
        #allow the user to decide if they want to upload the number of files in the directory
        proceed = True
        while proceed:
            cont = input("\nProceed (Y/N)? ").strip().upper()
            if cont == 'Y' or cont == 'N': 
                proceed = False
        x = 1
        if cont == "Y":
            for chunk in file_chunks:
                print(f"--Batch {x} being uploaded--")
                for local_file in chunk:
                    local_file = local_file.replace(os.sep, "/") # Replace \ with / on Windows
                    if not os.path.isfile(local_file):
                        upload_local_directory_to_minio(client,
                            local_file, bucket_name, minio_path + "/" + os.path.basename(local_file))
                    else:
                        remote_path = os.path.join(
                            minio_path, local_file[1 + len(local_path):])
                        remote_path = remote_path.replace(
                            os.sep, "/")  # Replace \ with / on Windows
                        client.fput_object(bucket_name, remote_path, local_file)
                x+=1
        else:
            print("\nUpload cancelled\n\n")
            exit()

        print('\nSuccess: all files in directory uploaded to MinIO')
    except Exception as e:
        print(e)
        


def checkApiConnection(api_name,url):
    #force timeout after 5 seconds of trying to connect
    response = requests.request(method='GET', url=url,timeout=5)
    out = f"{api_name} Status: {response.status_code}"
    return out

#########################################
############# END FUNCTIONS #############
#########################################






def upload_():
    endpoint = input("Enter input (return if using CAOC): ") or "minio.dev.caoc.army"

    connect_to = endpoint
    if connect_to.find("https://") == -1:
        connect_to = "https://" + connect_to
    instance_name = endpoint.replace("https://","").replace("http://","")
    #checking connection to minIO location
    try:
        data = checkApiConnection(instance_name,connect_to)
    except Exception as e:
        data = f"{instance_name} Status: Unable to Reach End-Point ({e})"
    
    if data.find('Unable') == -1:
        print(f"Contact with {connect_to} successful")
        print('\n')

        key = input("Enter user Key: ").strip()

        secret = input("Enter password: ").strip()
        try:
            client = connectMinIO(endpoint, key, secret)

            f_initial =  input('First initial:').strip().lower()
            last =  input('Last name:').strip().lower()
            createUserBucket(client,f_initial, last)
            filepath = input('File path containing data:')
            bucket_name = f'{f_initial}.{last}'
            minio_path = input('Name of cloud directory:').lower()
            print('\n')
            #uploading all of the documents
            old_time =time.time()
            upload_local_directory_to_minio(client,filepath, bucket_name, minio_path)
            
            time.sleep(2)
            total_size = getObjectList(client,bucket_name)[1]

            print(total_size + ' uploaded')
            new_time=time.time()
            time_delta = new_time - old_time
            print(f'Time Elapsed: {round(time_delta,1)}s')

            bucket_loc = f"{connect_to}/minio/{bucket_name}/{minio_path}/"
            print(f"\nLocation: {bucket_loc}\n")
        
        except Exception as e:
            print(e)
    else:
        print(data)
        print('\n')
        print("Check VPN Connection")
        print('\n')
        print("For persistent issues: https://oss-ace.atlassian.net/servicedesk/customer/portals")
        print('\n')
        exit()
