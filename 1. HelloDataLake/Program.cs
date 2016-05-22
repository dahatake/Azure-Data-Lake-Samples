using System;
using System.Collections.Generic;
using System.IO;
using System.Diagnostics;

using Microsoft.Azure.Management.DataLake.Analytics;
using Microsoft.Azure.Management.DataLake.Analytics.Models;
using Microsoft.Azure.Management.DataLake.Store;
using Microsoft.Azure.Management.DataLake.StoreUploader;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest;
using System.Threading.Tasks;

namespace HelloAzureDataLake
{
    class Program
	{
		private static DataLakeAnalyticsAccountManagementClient _adlaClient;
		private static DataLakeAnalyticsJobManagementClient _adlaJobClient;
		private static DataLakeAnalyticsCatalogManagementClient _adlaCatalogClient;
		private static DataLakeStoreAccountManagementClient _adlsClient;
		private static DataLakeStoreFileSystemManagementClient _adlsFileSystemClient;

        private static string adlaAccountName  = "<<Azure Data Lake Analytics アカウント名>>";
        private static string adlsAccountName  = "<<Azure Data Lake Store アカウント名>>";
        private static string subscriptionId   = "<<AzureのSubscription>>";

        private static void Main(string[] args)
		{
            // 処理時間の計測
            var totalSw = new Stopwatch();
            totalSw.Start();

            // Upload対象 のファイル
            string localFolderPath = @"C:\ADLStore\"; // TODO: Make sure this exists and contains the U-SQL script.


            // Azure AD上 のアプリケーション
            // 参考: https://azure.microsoft.com/ja-jp/documentation/articles/resource-group-create-service-principal-portal/
            var adTenantID = "<<Azure ADで設定したWebアプリケーションのテナントID>>";
            var adClientID = "<<Azure ADで設定したWebアプリケーションのクライアントID>>";
            var adKey = "<<Azure ADで設定したWebアプリケーションのキー>>";

            // Authenticate the user
            // For more information about applications and instructions on how to get a client ID, see: 
            //   https://azure.microsoft.com/en-us/documentation/articles/resource-group-create-service-principal-portal/
            Console.WriteLine("*** 1. Azure AD認証 ***");
            var tokenCreds = AuthenticateUser(
                adTenantID,
                adClientID,
                adKey);
            SetupClients(tokenCreds, subscriptionId); // TODO: Replace bracketed value.

            Console.WriteLine("*** 2. Azure Data Lake Store へ ファイルアップロード");
			UploadFile(localFolderPath + "SearchLog.tsv", "/Samples/Data/SearchLog.tsv");

            Console.WriteLine("*** 3. U-SQL スクリプト実行");
			Guid jobId = SubmitJobByPath("SampleUSQLScript.txt", "Azure Data Lake from C#");

			WaitForJob(jobId);

            // Download job output
            Console.WriteLine("*** 4. U-SQL 実行結果をダウンロード");
            DownloadFile("/Output/SearchLog-from-Data-Lake.csv", localFolderPath + "SearchLog-from-Data-Lake.csv");

            Console.WriteLine("***** 処理終了 *****");
            Console.WriteLine("総処理時間: {0}", totalSw.Elapsed.ToString());
            Console.WriteLine("何かキーを押してください。");
            Console.ReadLine();

        }


		// Azure AD 認証
		public static TokenCredentials AuthenticateUser(string tenantId, string appClientId, string appKey)
		{
			var authContext = new AuthenticationContext("https://login.microsoftonline.com/" + tenantId);

            var tokenAuthResult = authContext.AcquireToken("https://management.core.windows.net/", new ClientCredential(appClientId, appKey));

            return new TokenCredentials(tokenAuthResult.AccessToken);
		}

        //Set up clients
        public static void SetupClients(TokenCredentials tokenCreds, string subscriptionId)
        {
            _adlaClient = new DataLakeAnalyticsAccountManagementClient(tokenCreds);
			_adlaClient.SubscriptionId = subscriptionId;

			_adlaJobClient = new DataLakeAnalyticsJobManagementClient(tokenCreds);

			_adlaCatalogClient = new DataLakeAnalyticsCatalogManagementClient(tokenCreds);

			_adlsClient = new DataLakeStoreAccountManagementClient(tokenCreds);
			_adlsClient.SubscriptionId = subscriptionId;

			_adlsFileSystemClient = new DataLakeStoreFileSystemManagementClient(tokenCreds);
		}

		// Submit a U-SQL job by providing script contents.
		// Returns the job ID
		public static Guid SubmitJobByScript(string script, string jobName)
		{
			var jobId = Guid.NewGuid();
			var properties = new USqlJobProperties(script);
			var parameters = new JobInformation(jobName, JobType.USql, properties);

			var jobInfo = _adlaJobClient.Job.Create(adlaAccountName, jobId, parameters);

			return jobId;
		}

		// Submit a U-SQL job by providing a path to the script
		public static Guid SubmitJobByPath(string scriptPath, string jobName)
		{
			var script = File.ReadAllText(scriptPath);

			var jobId = Guid.NewGuid();
			var properties = new USqlJobProperties(script);
			var parameters = new JobInformation(jobName, JobType.USql, properties, priority: 1000, degreeOfParallelism: 1);

			var jobInfo = _adlaJobClient.Job.Create(adlaAccountName, jobId, parameters);

			return jobId;
		}

		public static JobResult WaitForJob(Guid jobId)
		{
			var jobInfo = _adlaJobClient.Job.Get(adlaAccountName, jobId);
            while (jobInfo.State != JobState.Ended)
            {
                jobInfo = _adlaJobClient.Job.Get(adlaAccountName, jobId);

                Console.WriteLine($"  *** 実行中... {jobInfo.State}");
                Task.Delay(5000).Wait();
            }
			return jobInfo.Result.Value;
		}

		// List jobs
		public static List<JobInformation> ListJobs()
		{
			var response = _adlaJobClient.Job.List(adlaAccountName);
			var jobs = new List<JobInformation>(response);

			while (response.NextPageLink != null)
			{
				response = _adlaJobClient.Job.ListNext(response.NextPageLink);
				jobs.AddRange(response);
			}

			return jobs;
		}

		// Upload a file
		public static void UploadFile(string srcFilePath, string destFilePath, bool force = true)
		{
			var parameters = new UploadParameters(srcFilePath, destFilePath, adlsAccountName, isOverwrite: force);
			var frontend = new DataLakeStoreFrontEndAdapter(adlsAccountName, _adlsFileSystemClient);
			var uploader = new DataLakeStoreUploader(parameters, frontend);
			uploader.Execute();
		}

		// Download file
		public static void DownloadFile(string srcPath, string destPath)
		{
			var stream = _adlsFileSystemClient.FileSystem.Open(adlsAccountName, srcPath);
			var fileStream = new FileStream(destPath, FileMode.Create);

			stream.CopyTo(fileStream);
			fileStream.Close();
			stream.Close();
		}
	}


}
