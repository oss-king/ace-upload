
# uncomment for package deployment
from ace_upload import _upload as ace

# ## use for local testing
# ## comment out for package deployment
# import _upload as ace

def main():
    print('''
            **********************************************************************************
                                        Upload Files to MinIO for ACE

            Given a file path, this executable will recursively gather any file in the 
            directory and will securely ingest them into cloud storage for aggregation to 
            ACE & brewlytics services.

            The file path must be Absolute.

            **********************************************************************************
            ''')

    ace.upload_()


if __name__ == "__main__":
    main()