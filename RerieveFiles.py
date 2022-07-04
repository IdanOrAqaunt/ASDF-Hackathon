from S3accessor import S3Accessor

bucket_name = 'si-input'

class RetrieveFiles:
    def __init__(self):
        self.__S3_accessor = S3Accessor(bucket_name)

    def get_files(self):
        organizations = list(filter(lambda x: '/' not in x.url, self.__S3_accessor.get_folders()))
        for org_name in organizations:
            if "test_" in org_name.url or "Test_" in org_name.url or "_test" in org_name.url or "TEST" in org_name.url or "demo_" in org_name.url:
                continue
            items = list(filter(lambda x: x.name == 'metadata.json', self.__S3_accessor.get_files(org_name.url)))
            if len(items) == 0:
                continue