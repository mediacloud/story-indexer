# Elasticsearch Configuration Files

By default ILM checks every 10 minutes if there’s any action to execute. In our example we had to use very small intervals (seconds, minutes), for testing.
Execute the following command to execute ILM

Test ILM policy

`{
    "name": "mediacloud-lifecycle-policy",
    "policy": {
      "phases": {
        "hot": {
          "actions": {
            "rollover": {
              "max_age": "10s"
            }
          }
        }
      }
    },
    "_meta": {
      "description": "built-in ILM policy using the hot phase with a rollover of 10s,
      "managed": true
    }
}
`

PUT _cluster/settings { "transient": { "indices.lifecycle.poll_interval": "15s"  } }