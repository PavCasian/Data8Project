import boto3

def files(bucket, *folders):
    s3 = boto3.resource('s3')
    data_bucket = s3.Bucket(bucket)
    contents = data_bucket.objects.all()
    filenames = {}
    for x in contents:
        folder = x.key.split('/')[0]
        file = x.key.split('/')[1]
        if file == '':  # don't include empty files (e.g. '../bucket_folder/')
            continue
        elif folder in filenames:
            filenames[folder].append(file)
        else:
            filenames[folder]=[file]
    return filenames




