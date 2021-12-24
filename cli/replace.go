package cli

import (
	"fmt"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/venus/pkg/messagepool"
	types2 "github.com/filecoin-project/venus/pkg/types"
	"github.com/ipfs/go-cid"
	xerrors "github.com/pkg/errors"
	"github.com/urfave/cli/v2"
	"strconv"
)

var MpoolReplaceCmd = &cli.Command{
	Name:  "replace",
	Usage: "replace a message in the mempool",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "gas-feecap",
			Usage: "gas feecap for new message (burn and pay to miner, attoFIL/GasUnit)",
		},
		&cli.StringFlag{
			Name:  "gas-premium",
			Usage: "gas price for new message (pay to miner, attoFIL/GasUnit)",
		},
		&cli.Int64Flag{
			Name:  "gas-limit",
			Usage: "gas limit for new message (GasUnit)",
		},
		&cli.BoolFlag{
			Name:  "auto",
			Usage: "automatically reprice the specified message",
		},
		&cli.StringFlag{
			Name:  "fee-limit",
			Usage: "Spend up to X FIL for this message in units of FIL. Previously when flag was `max-fee` units were in attoFIL. Applicable for auto mode",
		},
	},
	ArgsUsage: "<from nonce> | <message-cid>",
	Action: func(cctx *cli.Context) error {
		smapi, mcloser, err := NewMarketNode(cctx)
		if err != nil {
			return err
		}
		defer mcloser()

		fnapi, closer, err := NewFullNode(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)

		var from address.Address
		var nonce uint64
		switch cctx.Args().Len() {
		case 1:
			mcid, err := cid.Decode(cctx.Args().First())
			if err != nil {
				return err
			}

			msg, err := fnapi.ChainGetMessage(ctx, mcid)
			if err != nil {
				return fmt.Errorf("could not find referenced message: %w", err)
			}

			from = msg.From
			nonce = msg.Nonce
		case 2:
			f, err := address.NewFromString(cctx.Args().Get(0))
			if err != nil {
				return err
			}

			n, err := strconv.ParseUint(cctx.Args().Get(1), 10, 64)
			if err != nil {
				return err
			}

			from = f
			nonce = n
		default:
			return cli.ShowCommandHelp(cctx, cctx.Command.Name)
		}

		ts, err := fnapi.ChainHead(ctx)
		if err != nil {
			return xerrors.Errorf("getting chain head: %w", err)
		}

		pending, err := fnapi.MpoolPending(ctx, ts.Key())
		if err != nil {
			return err
		}

		var found *types2.SignedMessage
		for _, p := range pending {
			if p.Message.From == from && p.Message.Nonce == nonce {
				found = p
				break
			}
		}

		if found == nil {
			return fmt.Errorf("no pending message found from %s with nonce %d", from, nonce)
		}

		msg := found.Message

		if cctx.Bool("auto") {
			minRBF := messagepool.ComputeMinRBF(msg.GasPremium)

			var mss *types2.MessageSendSpec
			if cctx.IsSet("fee-limit") {
				maxFee, err := types.ParseFIL(cctx.String("fee-limit"))
				if err != nil {
					return fmt.Errorf("parsing max-spend: %w", err)
				}
				mss = &types2.MessageSendSpec{
					MaxFee: abi.TokenAmount(maxFee),
				}
			}

			// msg.GasLimit = 0 // TODO: need to fix the way we estimate gas limits to account for the messages already being in the mempool
			msg.GasFeeCap = abi.NewTokenAmount(0)
			msg.GasPremium = abi.NewTokenAmount(0)
			retm, err := fnapi.GasEstimateMessageGas(ctx, &msg, mss, types2.EmptyTSK)
			if err != nil {
				return fmt.Errorf("failed to estimate gas values: %w", err)
			}

			msg.GasPremium = big.Max(retm.GasPremium, minRBF)
			msg.GasFeeCap = big.Max(retm.GasFeeCap, msg.GasPremium)

			mff := func() (abi.TokenAmount, error) {
				return abi.TokenAmount(types.MustParseFIL("0.07")), nil
			}

			messagepool.CapGasFee(mff, &msg, mss)
		} else {
			if cctx.IsSet("gas-limit") {
				msg.GasLimit = cctx.Int64("gas-limit")
			}
			msg.GasPremium, err = types.BigFromString(cctx.String("gas-premium"))
			if err != nil {
				return fmt.Errorf("parsing gas-premium: %w", err)
			}
			// TODO: estimate fee cap here
			msg.GasFeeCap, err = types.BigFromString(cctx.String("gas-feecap"))
			if err != nil {
				return fmt.Errorf("parsing gas-feecap: %w", err)
			}
		}

		smsg, err := smapi.WalletSignMessage(ctx, msg.From, &msg)
		if err != nil {
			return fmt.Errorf("failed to sign message: %w", err)
		}

		cid, err := fnapi.MpoolPush(ctx, smsg)
		if err != nil {
			return fmt.Errorf("failed to push new message to mempool: %w", err)
		}

		fmt.Println("new message cid: ", cid)
		return nil
	},
}
