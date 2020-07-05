package aws

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kafka"
	"github.com/gruntwork-io/terratest/modules/testing"
	"github.com/stretchr/testify/require"
)

// GetMskCluster fetches information about specified MSK cluster.
func GetMskCluster(t testing.TestingT, region string, arn string) *kafka.ClusterInfo {
	cluster, err := GetMskClusterE(t, region, arn)
	require.NoError(t, err)
	return cluster
}

// GetMskClusterE fetches information about specified MSK cluster.
func GetMskClusterE(t testing.TestingT, region string, arn string) (*kafka.ClusterInfo, error) {
	client := NewMskClient(t, region)
	input := &kafka.DescribeClusterInput{
		ClusterArn: aws.String(arn),
	}
	output, err := client.DescribeCluster(input)
	if err != nil {
		return nil, err
	}

	return output.ClusterInfo, nil
}

// GetMskConfigName fetches information about specified configuration name of MSK.
func GetMskConfigName(t testing.TestingT, region string, arn string) *string {
	name, err := GetMskConfigNameE(t, region, arn)
	require.NoError(t, err)
	return name
}

// GetMskConfigNameE fetches information about specified configuration name of MSK.
func GetMskConfigNameE(t testing.TestingT, region string, arn string) (*string, error) {
	client := NewMskClient(t, region)
	input := &kafka.DescribeConfigurationInput{
		Arn: aws.String(arn),
	}
	output, err := client.DescribeConfiguration(input)
	if err != nil {
		return nil, err
	}

	return output.Name, nil
}

// GetMskConfigLatestRevision fetches information about latest revision number of MSK configuration.
func GetMskConfigLatestRevision(t testing.TestingT, region string, arn string) *int64 {
	revision, err := GetMskConfigLatestRevisionE(t, region, arn)
	require.NoError(t, err)
	return revision
}

// GetMskConfigLatestRevisionE fetches information about latest revision number of MSK configuration.
func GetMskConfigLatestRevisionE(t testing.TestingT, region string, arn string) (*int64, error) {
	client := NewMskClient(t, region)
	input := &kafka.DescribeConfigurationInput{
		Arn: aws.String(arn),
	}
	output, err := client.DescribeConfiguration(input)
	if err != nil {
		return nil, err
	}

	return output.LatestRevision.Revision, nil
}

// GetMskServerProperties fetches information about server properites of MSK Brokers.
func GetMskServerProperties(t testing.TestingT, region string, arn string, revision int64) []byte {
	properites, err := GetMskServerPropertiesE(t, region, arn, revision)
	require.NoError(t, err)
	return properites
}

// GetMskServerPropertiesE fetches information about server properites of MSK Brokers.
func GetMskServerPropertiesE(t testing.TestingT, region string, arn string, revision int64) ([]byte, error) {
	client := NewMskClient(t, region)
	input := &kafka.DescribeConfigurationRevisionInput{
		Arn:      aws.String(arn),
		Revision: aws.Int64(revision),
	}
	output, err := client.DescribeConfigurationRevision(input)
	if err != nil {
		return nil, err
	}

	return output.ServerProperties, nil
}

// GetMskBrokerString fetches information about brokers connection string of MSK.
func GetMskBrokerString(t testing.TestingT, region string, arn string) *string {
	brokerString, err := GetMskBrokerStringE(t, region, arn)
	require.NoError(t, err)
	return brokerString
}

// GetMskBrokerStringE fetches information about brokers connection string of MSK.
func GetMskBrokerStringE(t testing.TestingT, region string, arn string) (*string, error) {
	client := NewMskClient(t, region)
	input := &kafka.GetBootstrapBrokersInput{
		ClusterArn: aws.String(arn),
	}
	output, err := client.GetBootstrapBrokers(input)
	if err != nil {
		return nil, err
	}

	return output.BootstrapBrokerString, nil
}

// GetMskBrokerStringTLS fetches information about brokers TLS connection string of MSK.
func GetMskBrokerStringTLS(t testing.TestingT, region string, arn string) *string {
	brokerStringTLS, err := GetMskBrokerStringTLSE(t, region, arn)
	require.NoError(t, err)
	return brokerStringTLS
}

// GetMskBrokerStringTLSE fetches information about brokers TLS connection string of MSK.
func GetMskBrokerStringTLSE(t testing.TestingT, region string, arn string) (*string, error) {
	client := NewMskClient(t, region)
	input := &kafka.GetBootstrapBrokersInput{
		ClusterArn: aws.String(arn),
	}
	output, err := client.GetBootstrapBrokers(input)
	if err != nil {
		return nil, err
	}

	return output.BootstrapBrokerStringTls, nil
}

// NewMskClient creates en MSK client.
func NewMskClient(t testing.TestingT, region string) *kafka.Kafka {
	client, err := NewMskClientE(t, region)
	require.NoError(t, err)
	return client
}

// NewMskClientE creates en MSK client.
func NewMskClientE(t testing.TestingT, region string) (*kafka.Kafka, error) {
	sess, err := NewAuthenticatedSession(region)
	if err != nil {
		return nil, err
	}
	return kafka.New(sess), nil
}
