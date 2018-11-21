package cosmosdb.mongo.samples;

import cosmosdb.mongo.samples.runnables.InsertDocumentRunnable;
import cosmosdb.mongo.samples.sdkextensions.MongoClientExtension;
import cosmosdb.mongo.samples.sdkextensions.RuCharge;
import org.bson.Document;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;

import static cosmosdb.mongo.samples.InsertionHelper.*;

public class Main {

    private static ConfigSettings configSettings=new ConfigSettings();
    private static MongoClientExtension mongoClientExtension;
    public static void main(final String[] args) throws InterruptedException, IOException, URISyntaxException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        configSettings.Init();
        InitMongoClient();
        ExecuteScenario();
    }

    public static void ExecuteScenario() throws URISyntaxException, InterruptedException, IOException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {

        Run();
    }

    private static void Run() throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        java.lang.reflect.Method method;
        method = Main.class.getDeclaredMethod(configSettings.getScenario());
        method.invoke(null);

    }

    private static void InitMongoClient()
    {
        mongoClientExtension=
                new MongoClientExtension(
                        configSettings.getUserName(),
                        configSettings.getPassword(),
                        10255,
                        true,
                        configSettings.getClientThreadsCount()
                );
    }

    private static void Reproduce_Max_Message_Issue() throws URISyntaxException, InterruptedException {
        if(!mongoClientExtension.IsCollectionExists(configSettings.getDbName(),configSettings.getCollName())) {
            mongoClientExtension.CreatePartitionedCollection(
                    configSettings.getDbName(),
                    configSettings.getCollName(),
                    configSettings.getPartitionkey());

            mongoClientExtension.UpdateRus(
                    configSettings.getDbName(),
                    configSettings.getCollName(),
                    configSettings.getRus());
        }
        for(int i=0;i<7;i++) {
            How_To_Ingest_Sample_Docs_In_Batch_Using_Execution_Service();
        }
        //mongoClientExtension.Find(configSettings.getDbName(), configSettings.getCollName());
        mongoClientExtension.GetDocumentsCount(configSettings.getDbName(), configSettings.getCollName());
    }
    private static void  How_To_Ingest_Sample_Docs_In_Batch_Using_Execution_Service() throws URISyntaxException, InterruptedException {
        int batchSize=configSettings.getBatchSize();
        int numberOfBatches=configSettings.getNumberOfBatches();
        int sampleDocumentsCount=numberOfBatches*batchSize;

        List<Document> sampleDocs=SampleDoc.GetSampleDocuments(
                sampleDocumentsCount,
                configSettings.getPartitionkey());
        System.out.println(
                "Inserting total documents: " + sampleDocumentsCount+
                ", with batch size: "+batchSize+
                " in "+numberOfBatches+" batches.");

        final long startTime = System.currentTimeMillis();
        InsertOneInParallelUsingExecutionService(
                        mongoClientExtension,
                        configSettings.getDbName(),
                        configSettings.getCollName(),
                        configSettings.getPartitionkey(),
                        sampleDocs);
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("Execution time in milli seconds: " + totalTime);
        System.out.println("Execution time in seconds: " + totalTime / 1000);

    }

}
