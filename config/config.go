package config

import (
	"encoding"
	"time"

	"github.com/filecoin-project/go-address"

	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/venus/pkg/types"
)

// API contains configs for API endpoint
type API struct {
	// Binding address for the Lotus API
	ListenAddress       string
	RemoteListenAddress string
	Secret              string
	Timeout             Duration
}

// Libp2p contains configs for libp2p
type Libp2p struct {
	// Binding address for the libp2p host - 0 means random port.
	// Format: multiaddress; see https://multiformats.io/multiaddr/
	ListenAddresses []string
	// Addresses to explicitally announce to other peers. If not specified,
	// all interface addresses are announced
	// Format: multiaddress
	AnnounceAddresses []string
	// Addresses to not announce
	// Format: multiaddress
	NoAnnounceAddresses []string
	ProtectedPeers      []string

	PrivateKey string
}

type ConnectConfig struct {
	Url   string
	Token string
}

type Node ConnectConfig
type Messager ConnectConfig
type Market ConnectConfig
type AuthNode ConnectConfig

type Common struct {
	API    API
	Libp2p Libp2p
}

type Signer struct {
	SignerType string `toml:"Type"` // wallet/gateway
	Url        string
	Token      string
}

type Mysql struct {
	ConnectionString string
	MaxOpenConn      int
	MaxIdleConn      int
	ConnMaxLifeTime  string
	Debug            bool
}

type Journal struct {
	Path string
}

const (
	// RetrievalPricingDefault configures the node to use the default retrieval pricing policy.
	RetrievalPricingDefaultMode = "default"
	// RetrievalPricingExternal configures the node to use the external retrieval pricing script
	// configured by the user.
	RetrievalPricingExternalMode = "external"
)

type RetrievalPricing struct {
	Strategy string // possible values: "default", "external"

	Default  *RetrievalPricingDefault
	External *RetrievalPricingExternal
}

type RetrievalPricingExternal struct {
	// Path of the external script that will be run to price a retrieval deal.
	// This parameter is ONLY applicable if the retrieval pricing policy strategy has been configured to "external".
	Path string
}

type RetrievalPricingDefault struct {
	// VerifiedDealsFreeTransfer configures zero fees for data transfer for a retrieval deal
	// of a payloadCid that belongs to a verified piecestorage deal.
	// This parameter is ONLY applicable if the retrieval pricing policy strategy has been configured to "default".
	// default value is true
	VerifiedDealsFreeTransfer bool
}

type AddressConfig struct {
	DealPublishControl []User

	// DisableWorkerFallback disables usage of the worker address for messages
	// sent automatically, if control addresses are configured.
	// A control address that doesn't have enough funds will still be chosen
	// over the worker address if this flag is set.
	DisableWorkerFallback bool
}

func (ac AddressConfig) Address() []address.Address {
	addrs := make([]address.Address, len(ac.DealPublishControl))
	for index, miner := range ac.DealPublishControl {
		addrs[index] = address.Address(miner.Addr)
	}
	return addrs
}

type DAGStoreConfig struct {
	// Path to the dagstore root directory. This directory contains three
	// subdirectories, which can be symlinked to alternative locations if
	// need be:
	//  - ./transients: caches unsealed deals that have been fetched from the
	//    storage subsystem for serving retrievals.
	//  - ./indices: stores shard indices.
	//  - ./datastore: holds the KV store tracking the state of every shard
	//    known to the DAG store.
	// Default value: <LOTUS_MARKETS_PATH>/dagstore (split deployment) or
	// <LOTUS_MINER_PATH>/dagstore (monolith deployment)
	RootDir string

	// The maximum amount of indexing jobs that can run simultaneously.
	// 0 means unlimited.
	// Default value: 5.
	MaxConcurrentIndex int

	// The maximum amount of unsealed deals that can be fetched simultaneously
	// from the storage subsystem. 0 means unlimited.
	// Default value: 0 (unlimited).
	MaxConcurrentReadyFetches int

	// The maximum number of simultaneous inflight API calls to the storage
	// subsystem.
	// Default value: 100.
	MaxConcurrencyStorageCalls int

	// The time between calls to periodic dagstore GC, in time.Duration string
	// representation, e.g. 1m, 5m, 1h.
	// Default value: 1 minute.
	GCInterval Duration
}

type PieceStorage struct {
	Fs        FsPieceStorage
	S3        S3PieceStorage
	PreSignS3 PreSignS3PieceStorage
}

type PreSignS3PieceStorage struct {
	Enable bool
}

type FsPieceStorage struct {
	Enable bool
	Path   string
}

type S3PieceStorage struct {
	Enable   bool
	EndPoint string

	AccessKey string
	SecretKey string
	Token     string
}

type User struct {
	Addr    Address
	Account string
}

// StorageMiner is a miner config
type MarketConfig struct {
	Home `toml:"-"`

	Common

	Node     Node
	Messager Messager
	Signer   Signer
	AuthNode AuthNode

	Mysql Mysql

	PieceStorage  PieceStorage
	Journal       Journal
	AddressConfig AddressConfig
	DAGStore      DAGStoreConfig

	StorageMiners           []User
	RetrievalPaymentAddress User

	// When enabled, the miner can accept online deals
	ConsiderOnlineStorageDeals bool
	// When enabled, the miner can accept offline deals
	ConsiderOfflineStorageDeals bool
	// When enabled, the miner can accept retrieval deals
	ConsiderOnlineRetrievalDeals bool
	// When enabled, the miner can accept offline retrieval deals
	ConsiderOfflineRetrievalDeals bool
	// When enabled, the miner can accept verified deals
	ConsiderVerifiedStorageDeals bool
	// When enabled, the miner can accept unverified deals
	ConsiderUnverifiedStorageDeals bool
	// A list of Data CIDs to reject when making deals
	PieceCidBlocklist []cid.Cid
	// Maximum expected amount of time getting the deal into a sealed sector will take
	// This includes the time the deal will need to get transferred and published
	// before being assigned to a sector
	ExpectedSealDuration Duration
	// Maximum amount of time proposed deal StartEpoch can be in future
	MaxDealStartDelay Duration
	// When a deal is ready to publish, the amount of time to wait for more
	// deals to be ready to publish before publishing them all as a batch
	PublishMsgPeriod Duration
	// The maximum number of deals to include in a single PublishStorageDeals
	// message
	MaxDealsPerPublishMsg uint64
	// The maximum collateral that the provider will put up against a deal,
	// as a multiplier of the minimum collateral bound
	MaxProviderCollateralMultiplier uint64

	// The maximum number of parallel online data transfers (piecestorage+retrieval)
	SimultaneousTransfers uint64

	// A command used for fine-grained evaluation of piecestorage deals
	// see https://docs.filecoin.io/mine/lotus/miner-configuration/#using-filters-for-fine-grained-storage-and-retrieval-deal-acceptance for more details
	Filter string
	// A command used for fine-grained evaluation of retrieval deals
	// see https://docs.filecoin.io/mine/lotus/miner-configuration/#using-filters-for-fine-grained-storage-and-retrieval-deal-acceptance for more details
	RetrievalFilter string

	RetrievalPricing *RetrievalPricing

	MaxPublishDealsFee     types.FIL
	MaxMarketBalanceAddFee types.FIL
}

type MarketClientConfig struct {
	Home `toml:"-"`
	Common

	Node     Node
	Messager Messager
	Signer   Signer

	Market Market // reserve

	// The maximum number of parallel online data transfers (piecestorage+retrieval)
	SimultaneousTransfers uint64
	DefaultMarketAddress  Address
}

var _ encoding.TextMarshaler = (*Duration)(nil)
var _ encoding.TextUnmarshaler = (*Duration)(nil)

// Duration is a wrapper type for Duration
// for decoding and encoding from/to TOML
type Duration time.Duration

// UnmarshalText implements interface for TOML decoding
func (dur *Duration) UnmarshalText(text []byte) error {
	d, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	*dur = Duration(d)
	return err
}

func (dur Duration) MarshalText() ([]byte, error) {
	d := time.Duration(dur)
	return []byte(d.String()), nil
}

// Address is a wrapper type for Address
// for decoding and encoding from/to TOML
type Address address.Address

// UnmarshalText implements interface for TOML decoding
func (addr *Address) UnmarshalText(text []byte) error {
	d, err := address.NewFromString(string(text))
	if err != nil {
		return err
	}
	*addr = Address(d)
	return err
}

func (dur Address) MarshalText() ([]byte, error) {
	return []byte(address.Address(dur).String()), nil
}

func ConvertConfigAddress(addrs []Address) []address.Address {
	addrs2 := make([]address.Address, len(addrs))
	for index, addr := range addrs {
		addrs2[index] = address.Address(addr)
	}
	return addrs2
}
