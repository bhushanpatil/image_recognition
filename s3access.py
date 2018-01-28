import boto3
import json
import csv

session = boto3.Session(
    aws_access_key_id='',
    aws_secret_access_key='',
    #aws_session_token=SESSION_TOKEN,
)

s3 = session.client("s3",region_name="us-east-1")
rekognition = session.client('rekognition',region_name="us-east-1")

#my_bucket = s3.Bucket('bhushan502')
bucket = 'bhushan502'
bucket_path = 'photos/2018/'
fileArr = []
for object in s3.list_objects(Bucket=bucket,Prefix=bucket_path)['Contents']:
    if object['Key'] != bucket_path: 
        tempDict = {}
        # print(object['Key'])
        # print("s3 Path:",object['Key'])
        tempDict['filename'] = object['Key']
        response = rekognition.detect_labels(
                    Image={
                        "S3Object": {
                        "Bucket": bucket,
                        "Name": object['Key'],
                        }
                    }
                )
        #print(json.dumps(response,indent=2))

        lableArr = []
        for label in response['Labels']:
            if label['Confidence'] > 90:
                lableArr.append(label['Name'])

        #print(('|').join(lableArr)) 

        tempDict['label'] = ('|').join(lableArr)

        response = rekognition.detect_faces(
                    Image={
                        "S3Object": {
                        "Bucket": bucket,
                        "Name": object['Key'],
                        }
                    },
                    Attributes=['ALL']
                )
        #print(json.dumps(response,indent=2)) #FaceDetails->AgeRange->Low,High, Gender->Value, ->Confidence

        group = 'No'
        if len(response['FaceDetails']) > 1:
            group = 'Yes'

        tempDict['isgroup'] = group

        for face in response['FaceDetails']:
            # print(face['AgeRange']['Low']) 
            # print(face['AgeRange']['High']) 
            # print(face['Gender']['Value']) 
            # print(face['Gender']['Confidence']) 
            tempDict['Age_min'] = face['AgeRange']['Low']
            tempDict['Age_max'] = face['AgeRange']['High']
            tempDict['Gender_Confidence'] = face['Gender']['Confidence']
            tempDict['Gender'] = face['Gender']['Value']

        response = rekognition.recognize_celebrities(
            Image={
                        "S3Object": {
                        "Bucket": bucket,
                        "Name": object['Key'],
                        }
                    },
        )
        #CelebrityFaces -> Name, MatchConfidence
        tempDict['isCelebrity'] = "No"
        if len(response['CelebrityFaces']) > 0:
            #print("Celebrity found")
            tempDict['isCelebrity'] = "Yes"

        for celebrity in response['CelebrityFaces']:    
            # print(celebrity['Name'])    
            # print(celebrity['MatchConfidence'])  
            tempDict['Celebrity_Name'] = celebrity['Name']
            tempDict['Celebrity_MatchConfidence'] = celebrity['MatchConfidence'] 

        #print(json.dumps(response,indent=2))

        response = rekognition.detect_text(
            Image={
                        "S3Object": {
                        "Bucket": bucket,
                        "Name": object['Key'],
                        }
                    },
        )

        tempDict['text_detected'] = "No"
        if len(response['TextDetections'])>0:  
            #print("Text Detected")
            tempDict['text_detected'] = "Yes"
        #print(json.dumps(response,indent=2))


        response = rekognition.detect_moderation_labels(
            Image={
                        "S3Object": {
                        "Bucket": bucket,
                        "Name": object['Key'],
                        }
                    },
        )

        if len(response['ModerationLabels']) > 0:
            #print("Moderation label found")
            tempDict['moderation_label_found'] = "Yes"
            tempDict['moderation_label'] = ""
            for label in response['ModerationLabels']:
                # print(label['Name'])
                # print(label['Confidence'])
                tempDict['moderation_label'] += "|"+label['Name']+":"
                tempDict['moderation_label'] += str(label['Confidence'])

        #print(json.dumps(response,indent=2))

        fileArr.append(tempDict)
        


#print(fileArr)        

with open('result.csv', 'w') as csvfile:
    # csvwritter = csv.writer(csvfile, delimiter=', ',
    #                         quotechar='|', quoting=csv.QUOTE_MINIMAL)
    fieldnames = ['filename', 'label', 'isgroup', 'Age_min', 'Age_max', 'Gender_Confidence', 'Gender', 'isCelebrity', 'Celebrity_Name', 'Celebrity_MatchConfidence', 'text_detected', 'moderation_label_found', 'moderation_label']
    csvwritter = csv.DictWriter(csvfile, fieldnames=fieldnames)
    csvwritter.writeheader()

    # csvwritter.writerow(['Spam', 'Lovely Spam', 'Wonderful Spam'])
    csvwritter.writerows(fileArr)
    # for row in fileArr:
    #     csvwritter.writerow(row)




