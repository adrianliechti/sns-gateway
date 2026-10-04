module github.com/adrianliechti/sns-gateway/tests/sdk

go 1.27.1

require (
	github.com/adrianliechti/sns-gateway v0.0.0
	github.com/aws/aws-sdk-go-v2 v1.47.0
	github.com/aws/aws-sdk-go-v2/service/sns v1.34.0
)

require (
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.34 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.34 // indirect
	github.com/aws/smithy-go v1.28.1 // indirect
)

replace github.com/adrianliechti/sns-gateway => ../..
