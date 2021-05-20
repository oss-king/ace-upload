#dependencies
from minio import Minio
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


def convert_size(size_bytes):
    #convert bytes to readable format
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
    #make connection to MinIO
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
        


        
def getObjectList(client,bucket,folder_prefix):
    #get list of objects in a bucket
    #folder_prefix is used when specifying a folder to evaluate
    #otherwise provide folder_prefix as an empty string
    object_list = []
    if len(folder_prefix) > 0:
        objects = client.list_objects(bucket, prefix=folder_prefix,recursive=True)
    else:
        objects = client.list_objects(bucket,recursive=True)
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


    
def createUserBucket(client,f_initial, last):
    #create a bucket or determine if it already exists
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
    
def upload_file(local_file,bucket_name,minio_path,local_path,client):
    #upload a file to MinIO
    remote_path = os.path.join(minio_path, local_file[1 + len(local_path):])
    remote_path = remote_path.replace(os.sep, "/")  # Replace \ with / on Windows
    client.fput_object(bucket_name, remote_path, local_file)
    

def removeFiles(check_str,find_obj,find_count):
    #if a file name has a matching find_obj (str value)
    #and meets or exceeds find_count then mark it to remove
    check_list = [m.start() for m in re.finditer(find_obj, check_str)]
    if len(check_list) >= find_count:
        return True
    else:
        return False
    
def getFiles(local_path):
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
        
    except Exception as e:
        all_files = []
        print(e)

    return all_files


def checkApiConnection(api_name,url):
    #force timeout after 5 seconds of trying to connect
    try:
        response = requests.request(method='GET', url=url,timeout=5)
        out = f"\nContact with {api_name} successful\n\n"
    except:
        out = f"\nUnable to contact {api_name}\n\nCheck VPN or Internet Connection"
    return out


    
def uploadDirectory(client,filepath, bucket_name, minio_path,instance_name,connect_to):
    #uploads data to a new directory in MinIO
    
    #establish start time
    old_time =time.time()
    try:
        all_files = getFiles(filepath)
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
            completed_files = []
            proceed_error = True
            while proceed_error:
                for chunk in file_chunks:
                    print(f"--Batch {x} being uploaded--")
                    for local_file in chunk:
                        data = checkApiConnection(instance_name,connect_to)
                        if data.find('Unable') == -1:
                            #if we can connect
                            local_file = local_file.replace(os.sep, "/") # Replace \ with / on Windows
                            if local_file not in completed_files:
                                #if we havent already uploaded this file
                                try:
                                    #if we can connect
                                    upload_file(local_file,bucket_name,minio_path,filepath,client)
                                    completed_files.append(local_file)
                                except Exception as e:
                                    print(e)
                                    print(f"\nLost connection to {instance_name}")
                                    check = input("\nEnter Y when re-connected: ")
                                    if check.strip().lower() == 'y':
                                        # proceed_error = True
                                        upload_file(local_file,bucket_name,minio_path,filepath,client)
                                        completed_files.append(local_file)
                                        continue
                                    else:
                                        print("\nUpload cancelled\n\n")
                                        proceed_error = False
                                        break
                                        exit()                                    
                        else:
                            print(f"\nLost connection to {instance_name}")
                            check = input("\nEnter Y when re-connected: ")
                            if check.strip().lower() == 'y':
                                upload_file(local_file,bucket_name,minio_path,filepath,client)
                                completed_files.append(local_file)
                                continue
                            else:
                                print("\nUpload cancelled\n\n")
                                exit()
                                proceed_error = False
                                break
                    if x == len(file_chunks):
                        proceed_error = False
                    x+=1
            #last check to be sure we didn't lose any files
            check_files = [c for c in completed_files if c not in all_files]
            while len(check_files) > 0:
                print(check_files)
                for c in check_files:
                    data = checkApiConnection(instance_name,connect_to)
                    if data.find('Unable') == -1:
                        upload_file(c,bucket_name,minio_path,filepath,client)
                        check_files.remove(c)
                    else:
                        print(f"\nLost connection to {instance_name}")
                        check = input("\nEnter Y when re-connected: ")
                        if check.strip().lower() == 'y':
                            upload_file(c,bucket_name,minio_path,filepath,client)
                            check_files.remove(c)
                            continue
                        else:
                            print("\nUpload cancelled\n\n")
                            exit()
                            proceed_error = False
                            break
            new_time=time.time()
            time_delta = new_time - old_time
            print(f'\nTime Elapsed: {round(time_delta,1)}s')
        else:
            print("\nUpload cancelled\n\n")
            exit()
    except Exception as e:
        print(e)
    



def upload_():
    endpoint = input("Enter MinIO URL (return if using CAOC): ") or "minio.dev.caoc.army"
    connect_to = endpoint
    if connect_to.find("https://") == -1:
        connect_to = "https://" + connect_to
    instance_name = endpoint.replace("https://","").replace("http://","")
    #checking connection to minIO location
    data = checkApiConnection(instance_name,connect_to)
    if data.find('Unable') == -1:
        print(data)
        #username
        key = input("Enter user Key: ").strip()
        #password
        secret = input("Enter password: ").strip()

        try:
            #establish minio connection
            client = connectMinIO(endpoint, key, secret)
            #user details for bucket creation
            f_initial =  input('First initial: ').strip().lower()
            last =  input('Last name: ').strip().lower()

            createUserBucket(client,f_initial, last)
            bucket_name = f'{f_initial}.{last}'

            #location of files
            filepath = input('File path containing data: ')
            #directory name
            minio_path = input('Name of cloud directory: ').lower()
            print('\n')

            #conduct upload 
            uploadDirectory(client,filepath, bucket_name, minio_path,instance_name,connect_to)

            #get uploaded objects total size
            my_path = minio_path + '/'
            obj_list = getObjectList(client,bucket_name,my_path)
            file_cnt = len(obj_list[0])
            total_size = obj_list[1]
            print('\n')
            print(total_size + ' uploaded')
            print(f"Object Count: {file_cnt}")
            bucket_loc = f"{connect_to}/minio/{bucket_name}/{minio_path}/"
            print(f"\nLocation: {bucket_loc}\n")
        except Exception as e:
            print(e)
    else:
        print(data)
        print("For persistent issues: https://oss-ace.atlassian.net/servicedesk/customer/portals")
        print('\n')
        exit()