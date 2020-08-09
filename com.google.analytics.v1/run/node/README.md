# Test locally
npm start

# Deploy Cloud Functions from shell
gcloud functions deploy com_google_analytics_v1_Hit --region europe-west1 --runtime nodejs10 --trigger-http --max-instances 5

# Deploy Cloud Run from shell
gcloud builds submit --config=cloudbuild.yaml . --substitutions=TAG_NAME=0.0.1



curl --header "Content-Type: application/json"   --request POST   --data '{
     "message": {
       "attributes":{"namespace":"com.google.analytics.v1.transformed","name":"Entity"},
       "data":"MDIwMjAtMDgtMDdUMDY6NDA6NDkuODkyWgIxAigyNTU0MjQzNzUuMTU5NjYwNTEzNAACEHBhZ2V2aWV3AgAAAhQyd2c3djE5UThCGlVBLTIzMzQwNTY2LTEAAAIChgFVbmxpbWl0ZWQgcGVyc2lzdGVudCBkaXNrIGluIGdvb2dsZSBjbG91ZCBzaGVsbCDCtyByb2JlcnRzYWhsaW4uY29tApIBaHR0cHM6Ly9yb2JlcnRzYWhsaW4uY29tL3VubGltaXRlZC1wZXJzaXN0ZW50LWRpc2staW4tZ29vZ2xlLWNsb3VkLXNoZWxsLwIgcm9iZXJ0c2FobGluLmNvbQJiL3VubGltaXRlZC1wZXJzaXN0ZW50LWRpc2staW4tZ29vZ2xlLWNsb3VkLXNoZWxsLwKSAWh0dHBzOi8vcm9iZXJ0c2FobGluLmNvbS91bmxpbWl0ZWQtcGVyc2lzdGVudC1kaXNrLWluLWdvb2dsZS1jbG91ZC1zaGVsbC8AAAAAAgLoAU1vemlsbGEvNS4wIChYMTE7IENyT1MgYWFyY2g2NCAxMzA5OS44NS4wKSBBcHBsZVdlYktpdC81MzcuMzYgKEtIVE1MLCBsaWtlIEdlY2tvKSBDaHJvbWUvODQuMC40MTQ3LjExMCBTYWZhcmkvNTM3LjM2AgACAAICDDI0LWJpdAIQMTUzNng4NjQCAgRTRQIEYWICEnN0b2NraG9sbQAAAAAAAgAAAAAAAAAAAAAAAAACAAAAAA==",
       "messageId": "136969346945"
     },
     "subscription": "projects/myproject/subscriptions/mysubscription"
   }'   "http://localhost:8080/"