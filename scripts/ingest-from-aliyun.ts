import { RecursiveCharacterTextSplitter } from 'langchain/text_splitter';
import { OpenAIEmbeddings } from 'langchain/embeddings';
import { PineconeStore } from 'langchain/vectorstores';
import { pinecone } from '@/utils/pinecone-client';
import { CustomPDFLoader } from '@/utils/customPDFLoader';
import { PINECONE_INDEX_NAME, PINECONE_NAME_SPACE } from '@/config/pinecone';
import { DirectoryLoader } from 'langchain/document_loaders';
import { convertWordFiles } from 'convert-multiple-files-ul';

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


const downloadAndEmbedAll = async () => {
  let nextContinuationToken = await downloadAndEmbed(undefined);
  while(nextContinuationToken){
    nextContinuationToken = await downloadAndEmbed(undefined);
  }
}

const downloadAndEmbed = async (continuationToken: string | null | undefined) =>{
  let options = {
    prefix: process.env.ALI_OSS_PDF_ROOT,
    delimiter: '/',
    'max-keys': process.env.ALI_OSS_MAX_KEY || '100',
    'continuation-token': continuationToken || undefined
  };
  let result = await store.listV2(options, {timeout: 30000});

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
      else{
        if(result.objects[i].name.endsWith(".docx") || result.objects[i].name.endsWith(".doc") || result.objects[i].name.endsWith(".pdf")) {
          await store.get(result.objects[i].name, filePath + "/" + filename);
          let convertedFilePath = null;
          if(!filename.endsWith(".pdf")){
            convertedFilePath = await convertWordFiles((filePath + "/" + filename), 'pdf', filePath + "/");
          }
          try{
            await embed();
          } catch (error:any) {
            if(error.name == "InvalidPDFException"){
              console.log(error)
            } else {
              throw error
            }
          }
          fs.unlinkSync(filePath + "/" + filename);
          if(convertedFilePath){
            fs.unlinkSync(filePath + "/" + filename);
          }
          await db.set(filename, true);
        }
        
      }
  }

  return result.nextContinuationToken;
  
}


export const run = async () => {
  await downloadAndEmbedAll()
  
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
