package main

import (
	"context"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	rmnet "github.com/filecoin-project/go-fil-markets/retrievalmarket/network"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-fil-markets/storagemarket/impl/storedask"
	"github.com/filecoin-project/venus-market/api"
	"github.com/filecoin-project/venus-market/api/impl"
	"github.com/filecoin-project/venus-market/builder"
	cli2 "github.com/filecoin-project/venus-market/cli"
	"github.com/filecoin-project/venus-market/clients"
	"github.com/filecoin-project/venus-market/config"
	"github.com/filecoin-project/venus-market/dealfilter"
	"github.com/filecoin-project/venus-market/fundmgr"
	"github.com/filecoin-project/venus-market/journal"
	"github.com/filecoin-project/venus-market/metrics"
	"github.com/filecoin-project/venus-market/models"
	"github.com/filecoin-project/venus-market/network"
	"github.com/filecoin-project/venus-market/paychmgr"
	"github.com/filecoin-project/venus-market/piece"
	"github.com/filecoin-project/venus-market/retrievaladapter"
	"github.com/filecoin-project/venus-market/rpc"
	"github.com/filecoin-project/venus-market/sealer"
	"github.com/filecoin-project/venus-market/storageadapter"
	"github.com/filecoin-project/venus-market/types"
	"github.com/filecoin-project/venus-market/utils"
	"github.com/filecoin-project/venus/app/client/apiface"
	"github.com/filecoin-project/venus/pkg/constants"
	_ "github.com/filecoin-project/venus/pkg/crypto/bls"
	_ "github.com/filecoin-project/venus/pkg/crypto/secp"
	metrics2 "github.com/ipfs/go-metrics-interface"
	"github.com/urfave/cli/v2"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
	"os"
)

// Invokes are called in the order they are defined.
//nolint:golint
const (
	InitJournalKey     builder.Invoke = 3
	HandleDealsKey     builder.Invoke = 4
	HandleRetrievalKey builder.Invoke = 5
	ExtractApiKey      builder.Invoke = 10
)

func main() {
	app := &cli.App{
		Name:                 "venus-market",
		Usage:                "venus-market",
		Version:              constants.UserVersion(),
		EnableBashCompletion: true,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "repo",
				Value: "~/.venusmarket",
			},
		},
		Commands: []*cli.Command{
			{
				Name:   "run",
				Usage:  "run market daemon",
				Action: daemon,
			},
			cli2.PiecesCmd,
			cli2.RetrievalDealsCmd,
			cli2.StorageDealsCmd,
			cli2.ActorCmd,
			cli2.NetCmd,
		},
	}

	app.Setup()
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func prepare(cctx *cli.Context) (*config.MarketConfig, error) {
	cfg := config.DefaultMarketConfig
	cfg.HomeDir = cctx.String("repo")
	cfgPath, err := cfg.ConfigPath()
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(cfgPath); os.IsNotExist(err) {
		//create
		err = config.SaveConfig(cfg)
		if err != nil {
			return nil, xerrors.Errorf("save config to %s %w", cfgPath, err)
		}
	} else if err == nil {
		//loadConfig
		err = config.LoadConfig(cfgPath, cfg)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, err
	}
	return cfg, nil
}

func daemon(cctx *cli.Context) error {
	utils.SetupLogLevels()
	ctx := cctx.Context
	cfg, err := prepare(cctx)
	if err != nil {
		return err
	}

	resAPI := &impl.MarketNodeImpl{}
	shutdownChan := make(chan struct{})
	_, err = builder.New(ctx,
		//config
		config.ConfigServerOpts(cfg),
		//clients
		builder.Override(new(apiface.FullNode), clients.NodeClient),
		builder.Override(new(clients.IMessager), clients.MessagerClient),
		builder.Override(new(clients.ISinger), clients.NewWalletClient),
		builder.Override(new(clients.IStorageMiner), clients.NewStorageMiner),
		clients.ClientsOpts,
		//defaults
		// global system journal.
		builder.Override(new(journal.DisabledEvents), journal.EnvDisabledEvents),
		builder.Override(new(journal.Journal), journal.OpenFilesystemJournal),

		builder.Override(new(metrics.MetricsCtx), func() context.Context {
			return metrics2.CtxScope(context.Background(), "venus-market")
		}),

		builder.Override(new(types.ShutdownChan), make(chan struct{})),
		//database
		models.DBOptions(true),
		network.NetworkOpts(true, cfg.SimultaneousTransfers),
		piece.PieceOpts(cfg),
		fundmgr.FundMgrOpts,
		sealer.SealerOpts,
		paychmgr.PaychOpts,

		// Markets (retrieval deps)
		builder.Override(new(config.RetrievalPricingFunc), RetrievalPricingFunc(cfg)),

		// Markets (retrieval)
		builder.Override(new(retrievalmarket.RetrievalProviderNode), retrievaladapter.NewRetrievalProviderNode),
		builder.Override(new(rmnet.RetrievalMarketNetwork), RetrievalNetwork),
		builder.Override(new(retrievalmarket.RetrievalProvider), RetrievalProvider), //save to metadata /retrievals/provider
		builder.Override(new(config.RetrievalDealFilter), RetrievalDealFilter(nil)),
		builder.Override(HandleRetrievalKey, HandleRetrieval),

		// Markets (piecestorage)
		builder.Override(new(network.ProviderDataTransfer), NewProviderDAGServiceDataTransfer), //save to metadata /datatransfer/provider/transfers
		builder.Override(new(*storedask.StoredAsk), NewStorageAsk),                             //   save to metadata /deals/provider/piecestorage-ask/latest
		builder.Override(new(config.StorageDealFilter), BasicDealFilter(nil)),
		builder.Override(new(storagemarket.StorageProvider), StorageProvider),
		builder.Override(new(*storageadapter.DealPublisher), storageadapter.NewDealPublisher(cfg)),
		builder.Override(HandleDealsKey, HandleDeals),

		// Config (todo: get a real property system)
		builder.Override(new(config.ConsiderOnlineStorageDealsConfigFunc), NewConsiderOnlineStorageDealsConfigFunc),
		builder.Override(new(config.SetConsiderOnlineStorageDealsConfigFunc), NewSetConsideringOnlineStorageDealsFunc),
		builder.Override(new(config.ConsiderOnlineRetrievalDealsConfigFunc), NewConsiderOnlineRetrievalDealsConfigFunc),
		builder.Override(new(config.SetConsiderOnlineRetrievalDealsConfigFunc), NewSetConsiderOnlineRetrievalDealsConfigFunc),
		builder.Override(new(config.StorageDealPieceCidBlocklistConfigFunc), NewStorageDealPieceCidBlocklistConfigFunc),
		builder.Override(new(config.SetStorageDealPieceCidBlocklistConfigFunc), NewSetStorageDealPieceCidBlocklistConfigFunc),
		builder.Override(new(config.ConsiderOfflineStorageDealsConfigFunc), NewConsiderOfflineStorageDealsConfigFunc),
		builder.Override(new(config.SetConsiderOfflineStorageDealsConfigFunc), NewSetConsideringOfflineStorageDealsFunc),
		builder.Override(new(config.ConsiderOfflineRetrievalDealsConfigFunc), NewConsiderOfflineRetrievalDealsConfigFunc),
		builder.Override(new(config.SetConsiderOfflineRetrievalDealsConfigFunc), NewSetConsiderOfflineRetrievalDealsConfigFunc),
		builder.Override(new(config.ConsiderVerifiedStorageDealsConfigFunc), NewConsiderVerifiedStorageDealsConfigFunc),
		builder.Override(new(config.SetConsiderVerifiedStorageDealsConfigFunc), NewSetConsideringVerifiedStorageDealsFunc),
		builder.Override(new(config.ConsiderUnverifiedStorageDealsConfigFunc), NewConsiderUnverifiedStorageDealsConfigFunc),
		builder.Override(new(config.SetConsiderUnverifiedStorageDealsConfigFunc), NewSetConsideringUnverifiedStorageDealsFunc),
		builder.Override(new(config.SetExpectedSealDurationFunc), NewSetExpectedSealDurationFunc),
		builder.Override(new(config.GetExpectedSealDurationFunc), NewGetExpectedSealDurationFunc),
		builder.Override(new(config.SetMaxDealStartDelayFunc), NewSetMaxDealStartDelayFunc),
		builder.Override(new(config.GetMaxDealStartDelayFunc), NewGetMaxDealStartDelayFunc),

		builder.If(cfg.Filter != "",
			builder.Override(new(config.StorageDealFilter), BasicDealFilter(dealfilter.CliStorageDealFilter(cfg.Filter))),
		),

		builder.If(cfg.RetrievalFilter != "",
			builder.Override(new(config.RetrievalDealFilter), RetrievalDealFilter(dealfilter.CliRetrievalDealFilter(cfg.RetrievalFilter))),
		),
		builder.Override(new(*storageadapter.DealPublisher), storageadapter.NewDealPublisher(cfg)),
		builder.Override(new(storagemarket.StorageProviderNode), storageadapter.NewProviderNodeAdapter(cfg)),

		builder.Override(new(types.ShutdownChan), shutdownChan),
		func(s *builder.Settings) error {
			s.Invokes[ExtractApiKey] = builder.InvokeOption{
				Priority: 10,
				Option:   fx.Populate(resAPI),
			}
			return nil
		},
	)
	if err != nil {
		return xerrors.Errorf("initializing node: %w", err)
	}
	finishCh := utils.MonitorShutdown(shutdownChan)

	return rpc.ServeRPC(ctx, cfg, &cfg.API, api.MarketFullNode(resAPI), finishCh, 1000, "")
}