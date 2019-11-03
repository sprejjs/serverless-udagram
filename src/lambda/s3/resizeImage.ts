import {S3Event, SNSEvent, SNSHandler} from "aws-lambda";
import 'source-map-support/register'
import * as AWS from "aws-sdk"
import Jimp from "jimp/es"

const docClient = new AWS.DynamoDB.DocumentClient();

const s3 = new AWS.S3();
const thumbnailBucketName = process.env.THUMBNAILS_S3_BUCKET;
const imagesTable = process.env.IMAGES_TABLE;
const imageIdIndex = process.env.IMAGES_ID_INDEX;

export const handler: SNSHandler = async (event: SNSEvent) => {
    console.log("Processing SNS event", JSON.stringify(event));

    for(const snsRecord of event.Records) {
        const s3EventStr = snsRecord.Sns.Message;
        console.log('Processing S3 event', s3EventStr);

        const s3Event = JSON.parse(s3EventStr);
        await processS3Event(s3Event);
    }
};

async function processS3Event(event: S3Event) {
    for (const record of event.Records) {
        const key = record.s3.object.key;
        console.log('Processing S3 item with key: ', key);

        const result = await docClient.query( {
            TableName: imagesTable,
            IndexName: imageIdIndex,
            KeyConditionExpression: 'imageId = :imageId',
            ExpressionAttributeValues: {
                ':imageId': key
            }
        }).promise();

        const url: string = result.Items[0].imageUrl;
        console.log("Processing image with url: ", url);

        const resizedImage = await Jimp.read(url)
            .then((image: Jimp) => {
                console.log("Loaded image into memory");
                return image
                    .resize(250, Jimp.AUTO)
                    .getBufferAsync(Jimp.MIME_PNG)
            });

        console.log("Resized the image");

        const uploadResult = await s3.upload({
            Bucket: thumbnailBucketName,
            Key: key,
            Body: resizedImage,
            ContentType: 'image/png',
        }).promise();

        console.log("File uploaded successfully.", JSON.stringify(uploadResult));
    }
}
