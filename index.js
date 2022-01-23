const aws = require('aws-sdk');
const mysql = require('mysql2/promise');
const sharp = require('sharp');
const s3 = new aws.S3({ apiVersion: '2006-03-01' });

let connection = null;

mysql.createConnection({
    host     : process.env.RDS_HOSTNAME,
    user     : process.env.RDS_USERNAME,
    password : process.env.RDS_PASSWORD,
    database : process.env.RDS_DB_NAME 
}).then(con=>connection = con);

exports.handler = async(event, context) => {

    //get connection
    if(!connection){
        console.log("Nuova connessione");
        connection = await mysql.createConnection({
            host     : process.env.RDS_HOSTNAME,
            user     : process.env.RDS_USERNAME,
            password : process.env.RDS_PASSWORD,
            database : process.env.RDS_DB_NAME 
        })
    }

    //get path variables 
    if(!event.pathParameters || !event.pathParameters.idImmobile || !event.pathParameters.idFile) 
    return throwError("Parametri idImmobile ed idFile obbligatori");
    
    const { idImmobile, idFile } = event.pathParameters;
    
    if(isNaN(idImmobile) || isNaN(idFile) || idFile <= 0 || idImmobile <= 0) 
    return throwError("Parametri idImmobile ed IdFile non corretti, devono essere due numeri positivi");

    // get req body
    if(!event.body) return throwError("Necessario avere il corpo della richiesta");

    const reqBody = JSON.parse(event.body);

    if(!reqBody.rotating) return throwError("Il corpo della richiesta deve contenere il campo 'rotating'");
    
    const { rotating } = reqBody;
    
    if(rotating!==90 && rotating!==-90) return throwError("Rotazione indicata invalida, può essere solo oraria o antioraria");
    
    // get text query
    const textQuery = `SELECT codice_bucket FROM file WHERE immobile = ${idImmobile} AND id = ${idFile}`;
    
    // get codice bucket
    const results = await connection.execute(textQuery); 
    
    if(!results[0][0]) return throwError("File indicato non trovato. Operazione annullata");
    
    const codiceBucket = results[0][0].codice_bucket;
    
    if(!codiceBucket) return throwError("File indicato senza codice necessario. Operazione annullata");
        
    // get params
    let params = {
        Bucket: process.env.BUCKET_NAME,
        Key: codiceBucket,
    };

    // retrieve file
    const {ContentType, Body} = await s3.getObject(params).promise();

    // check is image
    if(!ContentType.startsWith('image')) return throwError("Si possono ruotare solo immagini");

    // rotate file
    const imageBuffered = await sharp(Body).rotate(rotating).toBuffer();

    // sovrascrivi file
    params = { ...params,
        Body: imageBuffered,
        ContentType
    }
    await s3.upload(params).promise();
        
    return {
            "statusCode": 200,
            "body": "Rotazione avvenuta",
            "isBase64Encoded": false
        }
}

const throwError = (message)=>{
    return {
        "statusCode": 400,
        "body": message,
        "isBase64Encoded": false
    }
}