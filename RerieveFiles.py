from S3accessor import S3Accessor
import csv, re, io
import zipfile

bucket_name = 'si-input'

class RetrieveFiles:
    def __init__(self):
        self.__S3_accessor = S3Accessor(bucket_name)

    def get_files(self):
        organizations = list(filter(lambda x: '/' not in x.url, self.__S3_accessor.get_folders()))
        for org_name in organizations:
            if "test_" in org_name.url or "Test_" in org_name.url or "_test" in org_name.url or "TEST" in org_name.url or "demo_" in org_name.url:
                continue
            items = list(filter(lambda x: x.name == 'output.zip', self.__S3_accessor.get_files(org_name.url)))
            if len(items) == 0:
                continue

            latestFile = max(items, key=lambda item: item.last_modified)

            output_file = self.__S3_accessor.download_file_content(latestFile.url)
            try:
                with zipfile.ZipFile(io.BytesIO(output_file)) as thezip:
                    for file in thezip.NameToInfo:
                        if file.split('/')[-1] == 'sc_reference.csv':
                            # with thezip.open(file, mode='r') as sc_reference:
                            sc_reference = thezip.read(file)
                            sc_reference = sc_reference.decode('utf-8').splitlines()
                            with open(org_name.name + "_sc_reference.csv", "w") as csv_file:
                                writer = csv.writer(csv_file, delimiter='\t')
                                for line in sc_reference:
                                    writer.writerow(re.split('\s+', line))
                            break
            except Exception as e:
                print("exception: ", e)
