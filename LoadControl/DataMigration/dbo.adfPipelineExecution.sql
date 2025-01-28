declare @json nvarchar(max) = '[
    {
        "Id": 0,
        "PipelineName": "Dummy",
        "RunId": "Dummy",
        "StartTimeUtc": "1901-01-01T00:00:00"
    },
    {
        "Id": 1,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "ad89ddea-94d0-4748-b55e-1e9ce9434cdb",
        "StartTimeUtc": "2024-01-25T18:06:39.073",
        "StatusMessage": "Testing"
    },
    {
        "Id": 2,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "bb6b0a9f-7753-4823-a84d-828287ace306",
        "StartTimeUtc": "2024-01-25T18:09:03.849",
        "StatusMessage": "Testing"
    },
    {
        "Id": 3,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "6d467b6c-fd42-426f-b319-dddad668689f",
        "StartTimeUtc": "2024-01-25T18:24:45.748",
        "StatusMessage": "Testing"
    },
    {
        "Id": 4,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "bcfbd23f-5a90-4d1b-995a-f6be25f05618",
        "StartTimeUtc": "2024-01-25T18:32:28.898",
        "StatusMessage": "Testing"
    },
    {
        "Id": 5,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "569fe6aa-2bdc-41b9-86a1-a9abebfb3c06",
        "StartTimeUtc": "2024-01-25T19:05:37.636",
        "EndTimeUtc": "2024-01-25T19:06:52.455",
        "BatchCount": -1,
        "StatusMessage": "Failed"
    },
    {
        "Id": 6,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "5f6b6ed4-918d-450b-80cd-fbf5b6282a23",
        "StartTimeUtc": "2024-01-25T19:14:05.961",
        "EndTimeUtc": "2024-01-25T19:15:03.400",
        "BatchCount": 0,
        "StatusMessage": "Completed"
    },
    {
        "Id": 7,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "1aa10904-0789-4dce-9460-11aa17bd3900",
        "StartTimeUtc": "2024-01-25T19:25:01.646",
        "StatusMessage": "Testing"
    },
    {
        "Id": 8,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "ca16c6e0-484e-4578-b5b5-84c1e2affbeb",
        "StartTimeUtc": "2024-01-25T19:33:14.428",
        "EndTimeUtc": "2024-01-25T19:34:05.962",
        "BatchCount": 0,
        "StatusMessage": "Succeeded"
    },
    {
        "Id": 9,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "af13ffd2-22ff-478f-b338-b76d7796ffdf",
        "StartTimeUtc": "2024-01-25T19:41:23.517",
        "EndTimeUtc": "2024-01-25T20:01:32.272",
        "BatchCount": 4,
        "StatusMessage": "Succeeded"
    },
    {
        "Id": 10,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "2d792ea5-2e2e-42a2-b775-e47465173c8b",
        "StartTimeUtc": "2024-01-26T14:43:26.872",
        "EndTimeUtc": "2024-01-26T15:34:45.552",
        "BatchCount": 5,
        "StatusMessage": "Succeeded"
    },
    {
        "Id": 11,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "935f0284-fb50-4541-8ec4-000d7b44ceb6",
        "StartTimeUtc": "2024-01-27T14:30:15.076",
        "EndTimeUtc": "2024-01-27T14:45:17.820",
        "BatchCount": 3,
        "StatusMessage": "Succeeded"
    },
    {
        "Id": 12,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "5346698e-9b51-4c8a-b4b1-19a32b1789e8",
        "StartTimeUtc": "2024-01-29T14:30:07.700",
        "EndTimeUtc": "2024-01-29T15:09:00.680",
        "BatchCount": 6,
        "StatusMessage": "Succeeded"
    },
    {
        "Id": 13,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "ce200680-35f4-4884-b198-2951e4bbe789",
        "StartTimeUtc": "2024-01-30T14:30:03.123",
        "EndTimeUtc": "2024-01-30T14:49:28.814",
        "BatchCount": 3,
        "StatusMessage": "Succeeded"
    },
    {
        "Id": 14,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "8cf98376-3269-4655-97f9-69152cb09374",
        "StartTimeUtc": "2024-01-31T15:26:56.087",
        "EndTimeUtc": "2024-01-31T15:46:48.313",
        "BatchCount": 3,
        "StatusMessage": "Succeeded"
    },
    {
        "Id": 15,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "81937709-f9cd-46a9-820a-7d789412665e",
        "StartTimeUtc": "2024-02-01T14:30:05.213",
        "EndTimeUtc": "2024-02-01T14:43:51.729",
        "BatchCount": 3,
        "StatusMessage": "Succeeded"
    },
    {
        "Id": 16,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "bd25f6b5-d5e4-45dd-b282-14ba93d23abc",
        "StartTimeUtc": "2024-02-02T14:30:03.303",
        "EndTimeUtc": "2024-02-02T14:53:12.324",
        "BatchCount": 3,
        "StatusMessage": "Succeeded"
    },
    {
        "Id": 17,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "6b5593c2-b3df-4807-a76d-f55d4a32f4f8",
        "StartTimeUtc": "2024-02-03T14:30:09.073",
        "EndTimeUtc": "2024-02-03T14:43:41.895",
        "BatchCount": 3,
        "StatusMessage": "Succeeded"
    },
    {
        "Id": 18,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "95ebcd8f-d7bc-4745-a69f-a559e3422c56",
        "StartTimeUtc": "2024-02-04T14:30:11.133",
        "EndTimeUtc": "2024-02-04T14:44:04.994",
        "BatchCount": 3,
        "StatusMessage": "Succeeded"
    },
    {
        "Id": 19,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "19ad110a-0979-485f-af45-b92e6c9b79d5",
        "StartTimeUtc": "2024-02-05T14:30:02.510",
        "EndTimeUtc": "2024-02-05T14:49:16.239",
        "BatchCount": 3,
        "StatusMessage": "Succeeded"
    },
    {
        "Id": 20,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "3d0ee803-8b58-4332-ac41-40b3880048fa",
        "StartTimeUtc": "2024-02-06T14:30:08.167",
        "EndTimeUtc": "2024-02-06T14:57:55.263",
        "BatchCount": 3,
        "StatusMessage": "Succeeded"
    },
    {
        "Id": 21,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "5a2a7312-8288-4d6f-85c7-ca0b2f6ac549",
        "StartTimeUtc": "2024-02-07T14:30:07.692",
        "EndTimeUtc": "2024-02-07T14:31:11.482",
        "BatchCount": -1,
        "StatusMessage": "Failed"
    },
    {
        "Id": 22,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "8d61f1f8-2954-4cc5-929f-55c39ff44f68",
        "StartTimeUtc": "2024-02-07T18:09:08.015",
        "EndTimeUtc": "2024-02-07T18:10:05.877",
        "BatchCount": -1,
        "StatusMessage": "Failed"
    },
    {
        "Id": 23,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "1c085625-dfaf-460c-b111-72b9b5489aba",
        "StartTimeUtc": "2024-02-07T18:19:00.141",
        "EndTimeUtc": "2024-02-07T18:19:46.027",
        "BatchCount": -1,
        "StatusMessage": "Failed"
    },
    {
        "Id": 24,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "047150d2-3004-44db-96f8-9828221d88a5",
        "StartTimeUtc": "2024-02-07T18:21:07.929",
        "EndTimeUtc": "2024-02-07T18:22:04.630",
        "BatchCount": -1,
        "StatusMessage": "Failed"
    },
    {
        "Id": 25,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "f45c8b13-f384-4365-95e1-77c485349185",
        "StartTimeUtc": "2024-02-07T18:26:02.173",
        "EndTimeUtc": "2024-02-07T18:26:41.184",
        "BatchCount": -1,
        "StatusMessage": "Failed"
    },
    {
        "Id": 26,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "8692fe16-7794-4663-a6d4-e8d38dfe4236",
        "StartTimeUtc": "2024-02-07T18:30:36.403",
        "StatusMessage": "In Progress"
    },
    {
        "Id": 27,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "68890db7-cce6-45df-bdc0-3e60c37ff97f",
        "StartTimeUtc": "2024-02-08T14:30:10.777",
        "EndTimeUtc": "2024-02-08T14:54:42.927",
        "BatchCount": 6,
        "StatusMessage": "Succeeded"
    },
    {
        "Id": 28,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "557ed040-64c0-480b-afc2-d8d5c945f442",
        "StartTimeUtc": "2024-02-08T18:54:15.536",
        "EndTimeUtc": "2024-02-08T18:55:18.482",
        "BatchCount": 0,
        "StatusMessage": "Succeeded"
    },
    {
        "Id": 29,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "df1a3f3d-89b8-4439-8b86-c87390fa25a7",
        "StartTimeUtc": "2024-02-08T20:06:54.895",
        "EndTimeUtc": "2024-02-08T20:24:06.316",
        "BatchCount": 3,
        "StatusMessage": "Succeeded"
    },
    {
        "Id": 30,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "6647caeb-2c2a-4955-945c-07d72f689be5",
        "StartTimeUtc": "2024-02-08T20:31:56.589",
        "StatusMessage": "In Progress"
    },
    {
        "Id": 31,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "143c4459-fa58-451b-b418-b4564b801f3b",
        "StartTimeUtc": "2024-02-08T20:57:16.631",
        "StatusMessage": "In Progress"
    },
    {
        "Id": 32,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "2c2709fe-30d9-4859-8170-b13a6461b873",
        "StartTimeUtc": "2024-02-08T21:33:27.336",
        "EndTimeUtc": "2024-02-08T21:50:19.534",
        "BatchCount": 3,
        "StatusMessage": "Succeeded"
    },
    {
        "Id": 33,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "076629a5-bb6a-4501-89f3-048f5f8a2433",
        "StartTimeUtc": "2024-02-09T14:30:11.325",
        "EndTimeUtc": "2024-02-09T14:47:09.430",
        "BatchCount": 3,
        "StatusMessage": "Succeeded"
    },
    {
        "Id": 34,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "b33eb784-090b-492e-994b-156244d24cc8",
        "StartTimeUtc": "2024-02-10T14:30:14.092",
        "EndTimeUtc": "2024-02-10T14:44:22.332",
        "BatchCount": 3,
        "StatusMessage": "Succeeded"
    },
    {
        "Id": 35,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "5208182d-8c50-414e-bb7b-cf41b68b2493",
        "StartTimeUtc": "2024-02-11T14:30:07.928",
        "EndTimeUtc": "2024-02-11T14:40:38.654",
        "BatchCount": 3,
        "StatusMessage": "Succeeded"
    },
    {
        "Id": 36,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "399fd255-3bb6-40a5-89e2-466d1ee3ee87",
        "StartTimeUtc": "2024-02-12T14:30:05.019",
        "EndTimeUtc": "2024-02-12T14:57:57.447",
        "BatchCount": 3,
        "StatusMessage": "Succeeded"
    },
    {
        "Id": 37,
        "PipelineName": "getGoogleAnalytics",
        "RunId": "e1274ecf-6fd0-45c5-9522-5e5b846c04ce",
        "StartTimeUtc": "2024-02-13T14:30:07.326",
        "EndTimeUtc": "2024-02-13T15:11:07.953",
        "BatchCount": 3,
        "StatusMessage": "Succeeded"
    }
]'

set identity_insert dbo.adfPipelineExecution on 

insert into dbo.adfPipelineExecution (Id, PipelineName, RunId, StartTimeUtc, EndTimeUtc, BatchCount, StatusMessage)
select      Id
        ,   PipelineName
        ,   RunId
        ,   StartTimeUtc
        ,   EndTimeUtc
        ,   BatchCount
        ,   StatusMessage
from        openjson(@json) 
with    (   Id	INT '$.Id',
            PipelineName	nvarchar(255) '$.PipelineName',
            RunId			varchar(255) '$.RunId',
            StartTimeUtc	datetime2 (3) '$.StartTimeUtc',
            EndTimeUtc		datetime2 (3) '$.EndTimeUtc',
            BatchCount		int '$.BatchCount',
            StatusMessage	varchar(25) '$.StatusMessage') j
where not exists (select Id from dbo.adfPipelineExecution x where x.Id = j.Id)

set identity_insert dbo.adfPipelineExecution off

update dbo.adfPipelineExecution set StatusMessage = StatusMessage + ' (Sandbox)' where StatusMessage not like '%(Sandbox)'
