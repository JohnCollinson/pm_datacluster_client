package client_test

import (
	"context"
	"errors"
	"testing"
	"time"
        "fmt"
	"github.com/EventStore/EventStore-Client-Go/client"
	direction "github.com/EventStore/EventStore-Client-Go/direction"
	client_errors "github.com/EventStore/EventStore-Client-Go/errors"
	messages "github.com/EventStore/EventStore-Client-Go/messages"
	stream_revision "github.com/EventStore/EventStore-Client-Go/streamrevision"
	uuid "github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
)

func createTestEvent() messages.ProposedEvent {
	return messages.ProposedEvent{
		EventID:      uuid.Must(uuid.NewV4()),
		EventType:    "TestEvent",
		ContentType:  "application/octet-stream",
		UserMetadata: []byte{0xd, 0xe, 0xa, 0xd},
		Data:         []byte{0xb, 0xe, 0xe, 0xf},
	}
}
func TestAppendToStreamSingleEventNoStream(t *testing.T) {
	fmt.Printf("TestAppendToStreamSingleEventNoStream : ")
	container := GetEmptyDatabase()
	defer container.Close()

	client := CreateTestClient(container, t)
	defer client.Close()
	testEvent := messages.ProposedEvent{
		EventID:      uuid.FromStringOrNil("38fffbc2-339e-11ea-8c7b-784f43837872"),
		EventType:    "TestEvent",
		ContentType:  "application/octet-stream",
		UserMetadata: []byte{0xd, 0xe, 0xa, 0xd},
		Data:         []byte{0xb, 0xe, 0xe, 0xf},
	}
	proposedEvents := []messages.ProposedEvent{
		testEvent,
	}

	streamID, _ := uuid.NewV4()
	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	defer cancel()
	_, err := client.AppendToStream(context, streamID.String(), stream_revision.StreamRevisionNoStream, proposedEvents)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	events, err := client.ReadStreamEvents(context, direction.Forwards, streamID.String(), stream_revision.StreamRevisionStart, 1, false)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}
	assert.Equal(t, int32(1), int32(len(events)), "Expected the correct number of messages to be returned")
	assert.Equal(t, testEvent.EventID, events[0].EventID)
	assert.Equal(t, testEvent.EventType, events[0].EventType)
	assert.Equal(t, streamID.String(), events[0].StreamID)
	assert.Equal(t, testEvent.Data, events[0].Data)
	assert.Equal(t, testEvent.UserMetadata, events[0].UserMetadata)
	fmt.Printf("OK\n")
}

func TestAppendWithInvalidStreamRevision(t *testing.T) {
	fmt.Printf("TestAppendWithInvalidStreamRevision : ")
	container := GetEmptyDatabase()
	defer container.Close()

	client := CreateTestClient(container, t)
	defer client.Close()
	events := []messages.ProposedEvent{
		createTestEvent(),
	}

	streamID, _ := uuid.NewV4()
	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	defer cancel()
	_, err := client.AppendToStream(context, streamID.String(), stream_revision.StreamRevisionStreamExists, events)

	if errors.Is(err, client_errors.ErrWrongExpectedStreamRevision) {
		fmt.Printf("OK. Wrong stream revision as expected.\n")
	} else {
		t.Fatalf("Fail: Expected WrongExpectedVersion, got >%+v<", err)
	}
}

func TestAppendToSystemStreamWithIncorrectCredentials(t *testing.T) {
	fmt.Printf("TestAppendToSystemStreamWithIncorrectCredentials : ")
	container := GetEmptyDatabase()
	defer container.Close()

	config := client.NewDefaultConfiguration()
	config.Address = container.Endpoint
	config.Username = "bad_user"
	config.Password = "bad_password"
	config.SkipCertificateVerification = true

	client, err := client.NewClient(config)
	// NewClient() always returns an error of nil. 
	// It just sets up the config ready for the connect() method
	if err != nil {
		t.Fatalf("Unexpected failure setting up test connection: %s", err.Error())
	}

	err = client.Connect()
	if err != nil {
		t.Fatalf("Unexpected failure connecting: %s", err.Error())
	}
	defer client.Close()
	
	events := []messages.ProposedEvent{
		createTestEvent(),
	}
	streamID, _ := uuid.NewV4()
	
	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	defer cancel()
	
	_, err = client.AppendToStream(context, streamID.String(), stream_revision.StreamRevisionAny, events)

	if errors.Is(err, client_errors.ErrUnauthenticated) {
		fmt.Printf("OK\n")
	}
}
