using System;
using System.IO;
using System.Diagnostics;
using System.Configuration;
using Microsoft.Rest;
using Microsoft.Azure.Management.DataLake.Store;
using Microsoft.Azure.Management.DataLake.StoreUploader;
using Microsoft.Azure.Management.DataLake.Analytics;
using Microsoft.Azure.Management.DataLake.Analytics.Models;
using Microsoft.IdentityModel.Clients.ActiveDirectory;

namespace HelloWorldForDataLake
{
    class Program
    {
        private static string SubscriptionID = ConfigurationManager.AppSettings["SubscriptionID"];

        // 参考: https://docs.microsoft.com/ja-jp/azure/azure-resource-manager/resource-group-create-service-principal-portal
        private static string clientID = ConfigurationManager.AppSettings["clientID"];
        private static string tenantID = ConfigurationManager.AppSettings["tenantID"];
        private static string adKey = ConfigurationManager.AppSettings["adKey"];

        private static string adlaAccountName = ConfigurationManager.AppSettings["adlaAccountName"];
        private static string adlsAccountName = ConfigurationManager.AppSettings["adlsAccountName"];

        // TODO: Make sure this exists and contains the U-SQL script.
        private static string localFolder = ConfigurationManager.AppSettings["localFolder"];

        private static DataLakeAnalyticsAccountManagementClient _adlaClient;
        private static DataLakeStoreFileSystemManagementClient _adlsFileSystemClient;
        private static DataLakeAnalyticsJobManagementClient _adlaJobClient;

        private static void Main(string[] args)
        {
            // 処理時間の計測
            var totalSw = new Stopwatch();
            totalSw.Start();

            // Connect to Azure
            Console.WriteLine("*** 1. Azure AD認証 ***");
            var creds = AuthenticateUser(
                tenantID,
                clientID,
                adKey);
            SetupClients(creds, SubscriptionID);

            Console.WriteLine("*** 2. Azure Data Lake Store へ ファイルアップロード");
            UploadFile($@"{localFolder}\SearchLog.tsv", "/Samples/Data/SearchLog.tsv"); // to ADLS

            // Submit the job
            Console.WriteLine("*** 3. U-SQL スクリプト実行");
            Guid jobId = SubmitJobByPath("SampleUSQLScript.txt", "Job from .NET SDK");
            WaitForJob(jobId);

            // Download job output
            Console.WriteLine("*** 4. U-SQL 実行結果をダウンロード");
            DownloadFile(@"/Output/SearchLog-output.csv", $@"{localFolder}\SearchLog-output.csv");

            Console.WriteLine("***** 処理終了 *****");
            Console.WriteLine("総処理時間: {0}", totalSw.Elapsed.ToString());
            Console.WriteLine("何かキーを押してください。");
            Console.ReadLine();
        }

        // Azure AD 認証
        public static TokenCredentials AuthenticateUser(string tenantId, string appClientId, string appKey)
        {
            var authContext = new AuthenticationContext($"https://login.microsoftonline.com/{tenantId}");
            var tokenAuthResult = authContext.AcquireToken("https://management.core.windows.net/", new ClientCredential(appClientId, appKey));
            return new TokenCredentials(tokenAuthResult.AccessToken);
        }

        public static void SetupClients(ServiceClientCredentials tokenCreds, string subscriptionId)
        {
            _adlaClient = new DataLakeAnalyticsAccountManagementClient(tokenCreds);
            _adlaClient.SubscriptionId = subscriptionId;
            _adlaJobClient = new DataLakeAnalyticsJobManagementClient(tokenCreds);
            _adlsFileSystemClient = new DataLakeStoreFileSystemManagementClient(tokenCreds);
        }

        public static void UploadFile(string srcFilePath, string destFilePath, bool force = true)
        {
            var parameters = new UploadParameters(srcFilePath, destFilePath, adlsAccountName, isOverwrite: force);
            var frontend = new Microsoft.Azure.Management.DataLake.StoreUploader.DataLakeStoreFrontEndAdapter(adlsAccountName, _adlsFileSystemClient);
            var uploader = new DataLakeStoreUploader(parameters, frontend);
            uploader.Execute();
        }

        public static void DownloadFile(string srcPath, string destPath)
        {
            var stream = _adlsFileSystemClient.FileSystem.Open(adlsAccountName, srcPath);
            var fileStream = new FileStream(destPath, FileMode.Create);

            stream.CopyTo(fileStream);
            fileStream.Close();
            stream.Close();
        }

        public static Guid SubmitJobByPath(string scriptPath, string jobName)
        {
            var script = File.ReadAllText(scriptPath);

            var jobId = Guid.NewGuid();
            var properties = new USqlJobProperties(script);
            var parameters = new JobInformation(jobName, JobType.USql, properties, priority: 1, degreeOfParallelism: 1, jobId: jobId);
            var jobInfo = _adlaJobClient.Job.Create(adlaAccountName, jobId, parameters);

            return jobId;
        }

        public static JobResult WaitForJob(Guid jobId)
        {
            var jobInfo = _adlaJobClient.Job.Get(adlaAccountName, jobId);
            while (jobInfo.State != JobState.Ended)
            {
                jobInfo = _adlaJobClient.Job.Get(adlaAccountName, jobId);
            }
            return jobInfo.Result.Value;
        }
    }
}