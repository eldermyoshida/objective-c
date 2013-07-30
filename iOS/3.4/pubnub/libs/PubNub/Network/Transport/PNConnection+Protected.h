//
//  PNConnection+Protected.h
//  pubnub
//
//  This header file used by library internal
//  components which require to access to some
//  methods and properties which shouldn't be
//  visible to other application components
//
//  Created by Sergey Mamontov.
//
//

#import "PNConnection.h"
#import "PNBaseRequest+Protected.h"


#pragma mark - Structures

// This enum represent possible stream states
typedef enum _PNSocketStreamState {

    // Stream not configured
    PNSocketStreamNotConfigured,

    // Stream configured by connection manager
    PNSocketStreamReady,

    // Stream is connecting at this moment
    PNSocketStreamConnecting,

    // Stream connected to the origin server
    // over socket (secure if configured)
    PNSocketStreamConnected,

    // Stream failure (not connected) because
    // of error
    PNSocketStreamError
} PNSocketStreamState;

// This enum represent current connection security level policy
typedef enum _PNConnectionSSLConfigurationLevel {

    // This option will check all information on
    // remote origin SSL certificate to ensure in
    // authority
    PNConnectionSSLConfigurationStrict,

    // This option will skip most of validations
    // and as fact will allow to work with server
    // which uses invalid SSL certificate or certificate
    // from another server
    PNConnectionSSLConfigurationBarelySecure,

    // This option will tell that connection should
    // be opened w/o SSL (if user won't to discard
    // security options)
    PNConnectionSSLConfigurationInsecure,
} PNConnectionSSLConfigurationLevel;

// This enum represents overall connection state (combined state from read/write stream)
typedef enum _PNConnectionState {

    PNConnectionCreated,

    // In this state connection doesn't has any configured and ready to work streams
    PNConnectionNotConfigured,

    // In this state connection hasn't been configured because of some error
    PNConnectionConfigurationError,

    // Same as PNConnectionConfigurationError but issued on connect attempt
    PNConnectionConfigurationErrorOnConnect,

    // Connection has been configured, but connection to remote host has been established
    PNConnectionNotConnected,

    // Connection is trying to establish connection to remote host
    PNConnectionConnecting,

    // Connection is trying to re-establish connection to remote host.
    // This state is set because of internal library logic to resume connection operation
    PNConnectionReconnecting,

    // Connection is trying to re-establish connection to remote host because previously it
    // was terminated on error.
    // This state is set because of internal library logic to resume connection operation
    PNConnectionReconnectingOnError,

    // Connection is trying to resume it's operation after suspension
    PNConnectionResuming,

    // Connection established to the remote host
    PNConnectionConnected,

    // Connection disconnection from remote host
    PNConnectionDisconnecting,,

    // Connection is suspending
    PNConnectionSuspending,

    // Connection disconnected from remote host (sockets closed and destroyed). Mostly this state will be
    // set because of user asked to close connection.
    // This state is almost the same as PNConnectionNotConfigured except state itself
    PNConnectionDisconnected,

    // Connection has been suspended
    PNConnectionSuspended
} PNConnectionState;

// This enum represents current connection data sending layer state
typedef enum _PNConnectionDataSendingState {

    // Connection data sending layer is waiting for new portion of data from
    // data delegates
    PNConnectionDataWaitingForNewData,

    // Connection data sending layer is processing data at this moment
    PNConnectionDataSending,

    // Connection data sending layer report that its operation has been interrupted
    PNConnectionDataSendingInterrupted
} PNConnectionDataSendingState;

// Structure describes list of available connection identifiers
struct PNConnectionIdentifiersStruct {
    
    // Used to identify connection which is used
    // for: subscriptions and presence observing
    __unsafe_unretained NSString *messagingConnection;
    
    // Used for another set of calls to the PubNub
    // service
    __unsafe_unretained NSString *serviceConnection;
};

extern struct PNConnectionIdentifiersStruct PNConnectionIdentifiers;


@interface PNConnection (Protected)


#pragma mark - Class methods

+ (void)resetConnectionsPool;


#pragma mark - Instance methods

- (BOOL)connectByUserRequest:(BOOL)isConnectingByUserRequest;

- (void)closeConnectionByUserRequest:(BOOL)isDisconnectingByUserRequest;

#pragma mark -


@end