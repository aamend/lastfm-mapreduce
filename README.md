LastFM public data set
======================

MapReduce jobs against LastFM 1K users dataset (http://www.last.fm/)
http://www.dtic.upf.edu/~ocelma/MusicRecommendationDataset/lastfm-1K.html

## Why Hadoop & MapReduce ?

For only 1000 users, data file is about 600MB large. A sequential analysis would take a lot of time to complete, so let's distribute the work on an Hadoop environment. Furthermore, a Tab separated format will be easily analyzed on MapReduce job without having to preformat data.

## Unique Users

### Exercise: 
*Create a list of unique users, and the number of distinct songs played by each user*

### MapReduce:

* On Mapper phase, for each user as key, output the song as value 
* On Combiner phase, remove the duplicates. Output the user as key and the distinct (local) songs as value
* On Reducer phase, remove the duplicates and output the user as key together with the total number of distinct songs as value

###Execution

`hadoop jar lastfm-1.0-SNAPSHOT.jar com.aamend.hadoop.lastfm.UniqueUsers -D mapred.reduce.tasks=<number of reducers> <input> <output>`

* (Required) Input Directory
* (Required) Output Directory
* (Optional) The number of reducers (default 1). Suggested value is 1 for 1K user dataset

## Top N songs

### Exercise:
*Create a list of the top 100 played songs (artist and title) in the dataset, with the number of times each song was played.*

### MapReduce:

2 MapReduce jobs will be required here. Job will be chained together from Driver code
* Count the distinct songs based on traId
* Sort data (topN)

#### 1st job

* On Mapper phase, for each song as key, output the value 1
* On Combiner phase, for each song, sum up the '1' values. Output the song tuple as key and the sum as value.
* On Reducer phase, for each song, sum up the '1' values. Output the song tuple as key and the sum as value.

Be aware that key is a Tuple (including Id and Name). One need to use a custom partitioner based on tuple's traId to make sure all data belonging to a same traId will be sent to the same reducer.

#### 2nd job

Use 1st job output as input.
* On Mapper phase, for each song (tuple including trackId, trackName and ArtistName) and counter, output the value as key and the key as value
* On Combiner phase, output the TopN songs (local TopN), counter as key and song as value
* On Reducer phase, output the TopN songs (global TopN), song as key and counter as value

I decided to let the Hadoop framework deal with the sort phase. By outputting counter as a key **on a single reducer**, I let the reduce sort phase sort my records (i.e. by song popularity). I simply need to output only the first 100 records from the reducers to get my top100 songs. Be aware the default sort is Ascending. I had to create a Custom Comparator to sort data descending (top results).

###Execution

`hadoop jar lastfm-1.0-SNAPSHOT.jar com.aamend.hadoop.lastfm.TopSongs -D mapred.reduce.tasks=<number of reducers> -D top.n=<N> <input> <output>`

* (Required) Input Directory
* (Required) Output Directory
* (Required) The N parameter for topN
* (Optional) The number of reducers (default 1) for the 1st job. Second job uses only 1

## Top N sessions

### Exercise:
*Say we define a user’s “session” of Last.fm usage to be comprised of one or more songs played by that user, where each song is started within 20 minutes of the previous song’s start time. Create a list of the top 100 longest sessions, with the following information about each session: userid, timestamp of first and last songs in the session, and the list of songs played in the session (in order of play).*

### MapReduce:

2 MapReduce jobs will be required here. Job will be chained together from Driver code
* Build sessions
* Sort data (topN)

#### 1st job

* On Mapper phase, for each user, output the song and timestamp
* On Reducer phase, for each song belonging to each user, create a session according to above definition. Output each session as a custom Tuple

#### 2nd job

Use 1st job output as input.
* On Mapper phase, for each session tuple, output the number of records this session has as a key and the session as value. Like for TopSongs job, Hadoop will deal with the sort phase.
* On Combiner phase, output only the TopN sessions (local TopN). Session size as key and session as value.
* On Reducer phase, output the TopN sessions (global TopN), Rank in the topN as key and session as value

###Execution

`hadoop jar lastfm-1.0-SNAPSHOT.jar com.aamend.hadoop.lastfm.TopSessions -D mapred.reduce.tasks=<number of reducers> -D top.n=<N> <input> <output>`

* (Required) Input Directory
* (Required) Output Directory
* (Required) The N parameter for topN
* (Optional) The number of reducers (default 1) for the 1st job. Second job uses only 1

## High availability

### Exercise

*Load the results from task 3 into a highly available data-store that can be queried.*

Provide:
* a. The code you used to load data into the store.
* b. A sample query that you used to retrieve the longest session.
* c. The raw output from the data-store.

Data being on HDFS, the defacto data store that can be queried would be HBase. However, above query shows that such a KV store would not be really efficient. We do not want all data for a given rowID (userId), but we look at session values (such as number of songs). And then, what if one would like to get queries such as:

* find all sessions for UserId1
* find all topNSessions, worstNSessions
* find all sessions started between Date1 and Date2
* find all sessions including Song1 

For that purpose, we could use ElasticSearch, that index all fields of Session documents.


### a. ETL

Loading data from Hadoop to ElasticSearch cluster is quite easy using below dependency.

```
<dependency>
    <groupId>org.elasticsearch</groupId>
    <artifactId>elasticsearch-hadoop-mr</artifactId>
    <version>${elasticsearch.version}</version>
</dependency>
```

We create a new MapReduce job against TopN session data created earlier. We use `ESOutputFormat`, and MapWritable as `value`. We supply the ES conf from the Hadoop configuration.

`hadoop jar lastfm-1.0-SNAPSHOT-jar-with-dependencies.jar com.aamend.hadoop.lastfm.TopSessionsETL -D es.nodes=localhost:9200 -D es.resource=radio/sessions input`

Documents will be indexed on ES under index `radio/sessions`

### b. Query

Get the longest session.. Find first the maximum size of any session
```
curl -XGET 'http://localhost:9200/radio/sessions/_search?search_type=count' -d '
{
"aggs" : 
    {
    "max_session" : 
        { 
        "max" : 
            { 
            "field" : "count" 
            }
        }
    }
}'
```
... And retrieve the session with this session size
```
curl -XGET 'http://localhost:9200/radio/sessions/_search?' -d '
{
"fields" : ["user", "start", "stop", "count"], 
"query" : 
    {
    "term" : 
        { 
        "count" : "4969" 
        }
    }
}'
```
Alternatively, this should work as well. Sort the data and retrieve first row
```
curl -XGET 'http://localhost:9200/radio/sessions/_search?size=1' -d '
{
"fields" : ["user", "start", "stop", "count"],
"query" : 
    {
    "match_all" : {}
    },
"sort" : [
    {
    "count" : 
        {
        "order" : "desc", "mode" : "avg"
        }
    }
    ]
}' 
```

### c. Output

```
{
  "_source": {
    "stop": 1133914596000,
    "user": "user_000949",
    "tracks": [
      "117f4438-64e5-4ef9-bebf-908f2d14a7f0",
      "c14cc283-80da-40f8-a838-b880ccbcf50a",
      "951a2bef-a129-4762-9e16-22c84c5d438e",
      "9b63aa59-89bd-404d-b7bf-80a1e49d144a",
      ...
      "8038cce3-f643-4d74-af17-609dffd30d8e",
      "8f9dec01-c479-4ad4-96b5-0950a97c1a91",
      "2d7d6714-8b41-4894-af3a-1cf0a092a7ee"
    ],
    "id": 181,
    "start": 1133739654000,
    "count": 1385
  },
  "found": true,
  "_version": 1,
  "_id": "m2V39AKbQhi--lL_B5DNHw",
  "_type": "sessions",
  "_index": "radio"
}
```


## Build

`mvn clean package`

This will create a fat JAR including all dependencies required for the project execution (ESOutputFormat)

## Authors

Antoine Amend <antoine.amend@tagman.com>




