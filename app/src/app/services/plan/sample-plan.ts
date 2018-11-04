export const SAMPLE_EXECUTION_PLAN = `[
  {
    "Plan": {
      "Node Type": "Nested Loop",
      "Parallel Aware": false,
      "Join Type": "Inner",
      "Startup Cost": 0.28,
      "Total Cost": 402.74,
      "Plan Rows": 1101,
      "Plan Width": 69,
      "Actual Startup Time": 0.021,
      "Actual Total Time": 1.844,
      "Actual Rows": 1101,
      "Actual Loops": 1,
      "Output": ["publisher.publisherid", "publisher.publishername", "school.schoolid", "school.schoolname"],
      "Inner Unique": true,
      "Shared Hit Blocks": 3316,
      "Shared Read Blocks": 0,
      "Shared Dirtied Blocks": 0,
      "Shared Written Blocks": 0,
      "Local Hit Blocks": 0,
      "Local Read Blocks": 0,
      "Local Dirtied Blocks": 0,
      "Local Written Blocks": 0,
      "Temp Read Blocks": 0,
      "Temp Written Blocks": 0,
      "Expected Query": [{
        "start": 14,
        "end": 23,
        "match": "PUBLISHER"
      }, {
        "start": 26,
        "end": 32,
        "match": "SCHOOL"
      }, {
        "start": 39,
        "end": 78,
        "match": "SCHOOL.SCHOOLID = PUBLISHER.PUBLISHERID"
      }],
      "Plans": [{
          "Node Type": "Seq Scan",
          "Parent Relationship": "Outer",
          "Parallel Aware": false,
          "Relation Name": "school",
          "Schema": "public",
          "Alias": "school",
          "Startup Cost": 0.00,
          "Total Cost": 22.01,
          "Plan Rows": 1101,
          "Plan Width": 42,
          "Actual Startup Time": 0.011,
          "Actual Total Time": 0.163,
          "Actual Rows": 1101,
          "Actual Loops": 1,
          "Output": ["school.schoolid", "school.schoolname"],
          "Shared Hit Blocks": 11,
          "Shared Read Blocks": 0,
          "Shared Dirtied Blocks": 0,
          "Shared Written Blocks": 0,
          "Local Hit Blocks": 0,
          "Local Read Blocks": 0,
          "Local Dirtied Blocks": 0,
          "Local Written Blocks": 0,
          "Temp Read Blocks": 0,
          "Temp Written Blocks": 0,
          "Expected Query": [{
            "start": 26,
            "end": 32,
            "match": "SCHOOL"
          }]
        },
        {
          "Node Type": "Index Scan",
          "Parent Relationship": "Inner",
          "Parallel Aware": false,
          "Scan Direction": "Forward",
          "Index Name": "publisher_pkey",
          "Relation Name": "publisher",
          "Schema": "public",
          "Alias": "publisher",
          "Startup Cost": 0.28,
          "Total Cost": 0.35,
          "Plan Rows": 1,
          "Plan Width": 27,
          "Actual Startup Time": 0.001,
          "Actual Total Time": 0.001,
          "Actual Rows": 1,
          "Actual Loops": 1101,
          "Output": ["publisher.publisherid", "publisher.publishername"],
          "Index Cond": "(publisher.publisherid = school.schoolid)",
          "Rows Removed by Index Recheck": 0,
          "Shared Hit Blocks": 3305,
          "Shared Read Blocks": 0,
          "Shared Dirtied Blocks": 0,
          "Shared Written Blocks": 0,
          "Local Hit Blocks": 0,
          "Local Read Blocks": 0,
          "Local Dirtied Blocks": 0,
          "Local Written Blocks": 0,
          "Temp Read Blocks": 0,
          "Temp Written Blocks": 0,
          "Expected Query": [{
            "start": 14,
            "end": 23,
            "match": "PUBLISHER"
          }, {
            "start": 39,
            "end": 78,
            "match": "SCHOOL.SCHOOLID = PUBLISHER.PUBLISHERID"
          }]
        }
      ]
    },
    "Planning Time": 0.104,
    "Triggers": [],
    "Execution Time": 1.957
  }
]`;

export const SAMPLE_QUERY = `SELECT * FROM PUBLISHER, SCHOOL WHERE SCHOOL.schoolID = PUBLISHER.publisherID`;
