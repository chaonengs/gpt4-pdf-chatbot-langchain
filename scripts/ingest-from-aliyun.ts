import { RecursiveCharacterTextSplitter } from 'langchain/text_splitter';
import { OpenAIEmbeddings } from 'langchain/embeddings';
import { PineconeStore } from 'langchain/vectorstores';
import { pinecone } from '@/utils/pinecone-client';
import { CustomPDFLoader } from '@/utils/customPDFLoader';
import { PINECONE_INDEX_NAME, PINECONE_NAME_SPACE } from '@/config/pinecone';
import { DirectoryLoader } from 'langchain/document_loaders';
import OSS from 'ali-oss';
import fs from 'fs';
import { QuickDB } from 'quick.db';
// const db = new QuickDB();
/* Name of directory to retrieve your files from */
const db = new QuickDB({ filePath: "history.sqlite" })

if (!process.env.ALI_OSS_REGION) {
  throw new Error('Missing ALI_OSS_REGION in .env file');
}
if (!process.env.ALI_OSS_BUECKET) {
  throw new Error('Missing ALI_OSS_BUECKET in .env file');
}
if (!process.env.ALI_OSS_PDF_ROOT) {
  throw new Error('Missing ALI_OSS_PDF_ROOT in .env file');
}
if (!process.env.ALI_AK_ID) {
  throw new Error('Missing ALI_AK_ID in .env file');
}
if (!process.env.ALI_AK_SECRET) {
  throw new Error('Missing ALI_AK_SECRET in .env file');
}


let store  = new OSS({
  region: process.env.ALI_OSS_REGION,
  accessKeyId: process.env.ALI_AK_ID,
  accessKeySecret: process.env.ALI_AK_SECRET,
  bucket: process.env.ALI_OSS_BUECKET
});




// objects {Array} object meta info list Each ObjectMeta will contains blow properties:

// name {String} object name on oss
// url {String} resource url
// lastModified {String} object last modified GMT date, e.g.: 2015-02-19T08:39:44.000Z
// etag {String} object etag contains ", e.g.: "5B3C1A2E053D763E1B002CC607C5A0FE"
// type {String} object type, e.g.: Normal
// size {Number} object size, e.g.: 344606
// storageClass {String} storage class type, e.g.: Standard
// owner {Object|null} object owner, including id and displayName

const filePath = 'docs/aliyun';


const embed = async () => {
    /*load raw docs from the all files in the directory */
    const directoryLoader = new DirectoryLoader(filePath, {
      '.pdf': (path) => new CustomPDFLoader(path),
    });

    // const loader = new PDFLoader(filePath);
    const rawDocs = await directoryLoader.load();

    /* Split text into chunks */
    const textSplitter = new RecursiveCharacterTextSplitter({
      chunkSize: 1000,
      chunkOverlap: 200,
    });

    const docs = await textSplitter.splitDocuments(rawDocs);
    console.log('split docs', docs);

    console.log('creating vector store...');
    /*create and store the embeddings in the vectorStore*/
    const embeddings = new OpenAIEmbeddings();
    const index = pinecone.Index(PINECONE_INDEX_NAME); //change to your own index name

    //embed the PDF documents
    await PineconeStore.fromDocuments(docs, embeddings, {
      pineconeIndex: index,
      namespace: PINECONE_NAME_SPACE,
      textKey: 'text',
    });
};

export const run = async () => {

  let result = await store.listV2({
    prefix: process.env.ALI_OSS_PDF_ROOT,
    delimiter: '/',
    'max-keys': '100'
  }, {timeout: 30000});

  for(let i = 0; i < result.objects.length; i++){
      const filename = result.objects[i].name.split('/').at(-1) || result.objects[i].name;
      console.log("processing %d / %d files: %s", i+1, result.objects.length, filename);
      if(!fs.existsSync(filePath)){
        fs.mkdirSync(filePath)
      }
      const parsed = await db.get(filename);
      if(parsed){
        console.log("parsed before, skipped: %s", filename);
      }
      if(!parsed && result.objects[i].name.endsWith(".pdf")){
        await store.get(result.objects[i].name, filePath + "/" + filename);
        try{
          await embed();
        } catch (error) {
          if(error.name == "InvalidPDFException"){
            console.log(error)
          } else {
            throw error
          }
        }
        fs.unlinkSync(filePath + "/" + filename);
        await db.set(filename, true);
      }
  }
  // result.objects.forEach(
  //   async (object) => {
  //     console.log("processing %d / %d files", i, result.objects.length);
  //     const filename = object.name.split('/').at(-1) || object.name;
  //     const parsed = await db.get(filename);
  //     if(!parsed && object.name.endsWith(".pdf")){
  //       await store.get(object.name, filePath + "/" + filename);
  //       await embed();
  //       fs.unlinkSync(filePath + "/" + filename);
  //       await db.set(filename, true);
  //     }
  //     i++;
  //   }
  // );


}




(async () => {
  await run();
  console.log('ingestion complete');
})();
