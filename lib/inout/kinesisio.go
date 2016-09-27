package inout

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"

	"github.com/ocdogan/fluentgo/lib/config"
)

type kinesisIO struct {
	awsIO
	client   *kinesis.Kinesis
	connFunc func() *kinesis.Kinesis
}

func newKinesisIO(manager InOutManager, config *config.InOutConfig) *kinesisIO {
	if config == nil {
		return nil
	}

	awsio := newAwsIO(manager, config)
	if awsio == nil {
		return nil
	}

	kio := &kinesisIO{
		awsIO: *awsio,
	}

	kio.connFunc = kio.funcGetClient

	return kio
}

func (kio *kinesisIO) funcGetClient() *kinesis.Kinesis {
	if kio.client == nil {
		defer recover()
		kio.client = kinesis.New(session.New(), kio.getAwsConfig())
	}
	return kio.client
}
