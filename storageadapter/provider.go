package storageadapter

// this file implements storagemarket.StorageProviderNode

import (
	"bytes"
	"context"
	"github.com/filecoin-project/venus-market/fundmgr"
	"github.com/filecoin-project/venus-market/piece"
	"github.com/filecoin-project/venus/app/client/apiface"
	"github.com/filecoin-project/venus/pkg/constants"
	"github.com/filecoin-project/venus/pkg/wallet"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
	"io"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/shared"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/exitcode"
	market2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/market"

	vCrypto "github.com/filecoin-project/venus/pkg/crypto"
	"github.com/filecoin-project/venus/pkg/events"
	"github.com/filecoin-project/venus/pkg/events/state"
	"github.com/filecoin-project/venus/pkg/types"
	"github.com/filecoin-project/venus/pkg/types/specactors/builtin/market"
	"github.com/filecoin-project/venus/pkg/types/specactors/builtin/miner"

	"github.com/filecoin-project/venus-market/config"
	"github.com/filecoin-project/venus-market/metrics"
	"github.com/filecoin-project/venus-market/utils"
)

var defaultMaxProviderCollateralMultiplier = uint64(2)
var log = logging.Logger("storageadapter")

type ProviderNodeAdapter struct {
	apiface.FullNode

	fundMgr *fundmgr.FundManager
	ev      *events.Events

	dealPublisher *DealPublisher

	storage                     piece.IPieceStorage
	extendPieceMeta             piece.ExtendPieceStore
	addBalanceSpec              *types.MessageSendSpec
	maxDealCollateralMultiplier uint64
	dsMatcher                   *dealStateMatcher
	scMgr                       *SectorCommittedManager
}

func NewProviderNodeAdapter(fc *config.MarketConfig) func(mctx metrics.MetricsCtx, lc fx.Lifecycle, node apiface.FullNode, dealPublisher *DealPublisher, fundMgr *fundmgr.FundManager, storage piece.IPieceStorage, extendPieceMeta piece.ExtendPieceStore) storagemarket.StorageProviderNode {
	return func(mctx metrics.MetricsCtx, lc fx.Lifecycle, full apiface.FullNode, dealPublisher *DealPublisher, fundMgr *fundmgr.FundManager, storage piece.IPieceStorage, extendPieceMeta piece.ExtendPieceStore) storagemarket.StorageProviderNode {
		ctx := metrics.LifecycleCtx(mctx, lc)

		ev, err := events.NewEvents(ctx, full)
		if err != nil {
			//todo add error return
			log.Warn(err)
		}
		na := &ProviderNodeAdapter{
			FullNode:        full,
			ev:              ev,
			dealPublisher:   dealPublisher,
			dsMatcher:       newDealStateMatcher(state.NewStatePredicates(state.WrapFastAPI(full))),
			storage:         storage,
			extendPieceMeta: extendPieceMeta,
			fundMgr:         fundMgr,
		}
		if fc != nil {
			na.addBalanceSpec = &types.MessageSendSpec{MaxFee: abi.TokenAmount(fc.MaxMarketBalanceAddFee)}
			na.maxDealCollateralMultiplier = fc.MaxProviderCollateralMultiplier
		}
		na.maxDealCollateralMultiplier = defaultMaxProviderCollateralMultiplier
		na.scMgr = NewSectorCommittedManager(ev, na, &apiWrapper{api: full})
		return na
	}
}

func (n *ProviderNodeAdapter) PublishDeals(ctx context.Context, deal storagemarket.MinerDeal) (cid.Cid, error) {
	return n.dealPublisher.Publish(ctx, deal.ClientDealProposal)
}

func (n *ProviderNodeAdapter) OnDealComplete(ctx context.Context, deal storagemarket.MinerDeal, pieceSize abi.UnpaddedPieceSize, pieceData io.Reader) (*storagemarket.PackingResult, error) {
	pieceCid := deal.ClientDealProposal.Proposal.PieceCID
	has, err := n.storage.Has(pieceCid.String())
	if err != nil {
		return nil, xerrors.Errorf("failed to get piece cid data %w", err)
	}

	if !has {
		wLen, err := n.storage.SaveTo(ctx, pieceCid.String(), pieceData)
		if err != nil {
			return nil, err
		}
		if wLen != int64(pieceSize) {
			return nil, xerrors.Errorf("save piece expect len %d but got %d", pieceSize, wLen)
		}
		log.Infof("success to write file %s to piece storage", pieceCid)
	}

	/*	storagemarket.MinerDeal{
			Client:             deal.Client,
			ClientDealProposal: deal.ClientDealProposal,
			ProposalCid:        deal.ProposalCid,
			State:              deal.State,
			Ref:                deal.Ref,
			PublishCid:         deal.PublishCid,
			DealID:             deal.DealID,
			FastRetrieval:      deal.FastRetrieval,
		},
			deal.Proposal.PieceSize.Unpadded(),
			paddedReader,*/

	err = n.extendPieceMeta.UpdateDealOnComplete(pieceCid, deal.ClientDealProposal, deal.Ref, *deal.PublishCid, deal.DealID, deal.FastRetrieval)
	if err != nil {
		return nil, err
	}

	log.Infof("add deal to piece meta data %s", pieceCid)
	return &storagemarket.PackingResult{
		SectorNumber: 0,
		Offset:       0,
		Size:         pieceSize.Padded(),
	}, nil
	//todo wait until assign deals
	/*if deal.PublishCid == nil {
		return nil, xerrors.Errorf("deal.PublishCid can't be nil")
	}

	sdInfo := marketTypes.PieceDealInfo{
		DealID:       deal.DealID,
		DealProposal: &deal.Proposal,
		PublishCid:   deal.PublishCid,
		DealSchedule: marketTypes.DealSchedule{
			StartEpoch: deal.ClientDealProposal.Proposal.StartEpoch,
			EndEpoch:   deal.ClientDealProposal.Proposal.EndEpoch,
		},
		KeepUnsealed: deal.FastRetrieval,
	}

	p, offset, err := n.secb.AddPiece(ctx, pieceSize, pieceData, sdInfo)
	curTime := constants.Clock.Now()
	for constants.Clock.Since(curTime) < addPieceRetryTimeout {
		if !xerrors.Is(err, sealing.ErrTooManySectorsSealing) {
			if err != nil {
				log.Errorf("failed to addPiece for deal %d, err: %v", deal.DealID, err)
			}
			break
		}
		select {
		case <-constants.Clock.After(addPieceRetryWait):
			p, offset, err = n.secb.AddPiece(ctx, pieceSize, pieceData, sdInfo)
		case <-ctx.Done():
			return nil, xerrors.New("context expired while waiting to retry AddPiece")
		}
	}

	if err != nil {
		return nil, xerrors.Errorf("AddPiece failed: %s", err)
	}
	log.Warnf("New Deal: deal %d", deal.DealID)

	return &storagemarket.PackingResult{
		SectorNumber: p,
		Offset:       offset,
		Size:         pieceSize.Padded(),
	}, nil*/
}

func (n *ProviderNodeAdapter) VerifySignature(ctx context.Context, sig crypto.Signature, addr address.Address, input []byte, encodedTs shared.TipSetToken) (bool, error) {
	addr, err := n.StateAccountKey(ctx, addr, types.EmptyTSK)
	if err != nil {
		return false, err
	}

	err = vCrypto.Verify(&sig, addr, input)
	return err == nil, err
}

func (n *ProviderNodeAdapter) GetMinerWorkerAddress(ctx context.Context, maddr address.Address, tok shared.TipSetToken) (address.Address, error) {
	tsk, err := types.TipSetKeyFromBytes(tok)
	if err != nil {
		return address.Undef, err
	}

	mi, err := n.StateMinerInfo(ctx, maddr, tsk)
	if err != nil {
		return address.Address{}, err
	}
	return mi.Worker, nil
}

func (n *ProviderNodeAdapter) GetProofType(ctx context.Context, maddr address.Address, tok shared.TipSetToken) (abi.RegisteredSealProof, error) {
	tsk, err := types.TipSetKeyFromBytes(tok)
	if err != nil {
		return 0, err
	}

	mi, err := n.StateMinerInfo(ctx, maddr, tsk)
	if err != nil {
		return 0, err
	}

	nver, err := n.StateNetworkVersion(ctx, tsk)
	if err != nil {
		return 0, err
	}

	return miner.PreferredSealProofTypeFromWindowPoStType(nver, mi.WindowPoStProofType)
}

//todo need to know which type of message change signature
func (n *ProviderNodeAdapter) SignBytes(ctx context.Context, signer address.Address, b []byte) (*crypto.Signature, error) {
	signer, err := n.StateAccountKey(ctx, signer, types.EmptyTSK)
	if err != nil {
		return nil, err
	}
	//todo  change func signature to get real sign type
	msg := types.UnsignedMessage{}
	err = msg.UnmarshalCBOR(bytes.NewReader(b))
	mType := wallet.MTChainMsg
	if err != nil {
		mType = wallet.MTUnknown
	}
	localSignature, err := n.FullNode.WalletSign(ctx, signer, b, wallet.MsgMeta{
		Type: mType,
	})
	if err != nil {
		return nil, err
	}
	return localSignature, nil
}

func (n *ProviderNodeAdapter) ReserveFunds(ctx context.Context, wallet, addr address.Address, amt abi.TokenAmount) (cid.Cid, error) {
	return n.fundMgr.Reserve(ctx, wallet, addr, amt)
}

func (n *ProviderNodeAdapter) ReleaseFunds(ctx context.Context, addr address.Address, amt abi.TokenAmount) error {
	return n.fundMgr.Release(addr, amt)
}

// Adds funds with the StorageMinerActor for a piecestorage participant.  Used by both providers and clients.
func (n *ProviderNodeAdapter) AddFunds(ctx context.Context, addr address.Address, amount abi.TokenAmount) (cid.Cid, error) {
	// (Provider Node API)
	smsg, err := n.MpoolPushMessage(ctx, &types.Message{
		To:     market.Address,
		From:   addr,
		Value:  amount,
		Method: market.Methods.AddBalance,
	}, n.addBalanceSpec)
	if err != nil {
		return cid.Undef, err
	}

	return smsg.Cid(), nil
}

func (n *ProviderNodeAdapter) GetBalance(ctx context.Context, addr address.Address, encodedTs shared.TipSetToken) (storagemarket.Balance, error) {
	tsk, err := types.TipSetKeyFromBytes(encodedTs)
	if err != nil {
		return storagemarket.Balance{}, err
	}

	bal, err := n.StateMarketBalance(ctx, addr, tsk)
	if err != nil {
		return storagemarket.Balance{}, err
	}

	return utils.ToSharedBalance(bal), nil
}

// TODO: why doesnt this method take in a sector ID?
func (n *ProviderNodeAdapter) LocatePieceForDealWithinSector(ctx context.Context, dealID abi.DealID, encodedTs shared.TipSetToken) (sectorID abi.SectorNumber, offset abi.PaddedPieceSize, length abi.PaddedPieceSize, err error) {
	panic("depresated")
	/*refs, err := n.secb.GetRefs(dealID)
	if err != nil {
		return 0, 0, 0, err
	}
	if len(refs) == 0 {
		return 0, 0, 0, xerrors.New("no sector information for deal ID")
	}

	// TODO: better strategy (e.g. look for already unsealed)
	var best marketTypes.SealedRef
	var bestSi marketTypes.SectorInfo
	for _, r := range refs {
		si, err := n.secb.SectorBuilder.SectorsStatus(ctx, r.SectorID, false)
		if err != nil {
			return 0, 0, 0, xerrors.Errorf("getting sector info: %w", err)
		}
		if si.State == marketTypes.SectorState(sealing.Proving) {
			best = r
			bestSi = si
			break
		}
	}
	if bestSi.State == marketTypes.SectorState(sealing.UndefinedSectorState) {
		return 0, 0, 0, xerrors.New("no sealed sector found")
	}
	return best.SectorID, best.Offset, best.Size.Padded(), nil*/
}

func (n *ProviderNodeAdapter) DealProviderCollateralBounds(ctx context.Context, size abi.PaddedPieceSize, isVerified bool) (abi.TokenAmount, abi.TokenAmount, error) {
	bounds, err := n.StateDealProviderCollateralBounds(ctx, size, isVerified, types.EmptyTSK)
	if err != nil {
		return abi.TokenAmount{}, abi.TokenAmount{}, err
	}

	// The maximum amount of collateral that the provider will put into escrow
	// for a deal is calculated as a multiple of the minimum bounded amount
	max := types.BigMul(bounds.Min, types.NewInt(n.maxDealCollateralMultiplier))

	return bounds.Min, max, nil
}

// TODO: Remove dealID parameter, change publishCid to be cid.Cid (instead of pointer)
func (n *ProviderNodeAdapter) OnDealSectorPreCommitted(ctx context.Context, provider address.Address, dealID abi.DealID, proposal market2.DealProposal, publishCid *cid.Cid, cb storagemarket.DealSectorPreCommittedCallback) error {
	return n.scMgr.OnDealSectorPreCommitted(ctx, provider, market.DealProposal(proposal), *publishCid, cb)
}

// TODO: Remove dealID parameter, change publishCid to be cid.Cid (instead of pointer)
func (n *ProviderNodeAdapter) OnDealSectorCommitted(ctx context.Context, provider address.Address, dealID abi.DealID, sectorNumber abi.SectorNumber, proposal market2.DealProposal, publishCid *cid.Cid, cb storagemarket.DealSectorCommittedCallback) error {
	return n.scMgr.OnDealSectorCommitted(ctx, provider, sectorNumber, market.DealProposal(proposal), *publishCid, func(err error) {
		cb(err)
		_Err := n.extendPieceMeta.UpdateDealStatus(dealID, "Proving")
		if _Err != nil {
			log.Errorw("update deal status %w", _Err)
		}
	})
}

func (n *ProviderNodeAdapter) GetChainHead(ctx context.Context) (shared.TipSetToken, abi.ChainEpoch, error) {
	head, err := n.ChainHead(ctx)
	if err != nil {
		return nil, 0, err
	}

	return head.Key().Bytes(), head.Height(), nil
}

func (n *ProviderNodeAdapter) WaitForMessage(ctx context.Context, mcid cid.Cid, cb func(code exitcode.ExitCode, bytes []byte, finalCid cid.Cid, err error) error) error {
	receipt, err := n.StateWaitMsg(ctx, mcid, 2*constants.MessageConfidence, constants.LookbackNoLimit, true)
	if err != nil {
		return cb(0, nil, cid.Undef, err)
	}
	ctx.Done()
	return cb(receipt.Receipt.ExitCode, receipt.Receipt.ReturnValue, receipt.Message, nil)
}

func (n *ProviderNodeAdapter) WaitForPublishDeals(ctx context.Context, publishCid cid.Cid, proposal market2.DealProposal) (*storagemarket.PublishDealsWaitResult, error) {
	// Wait for deal to be published (plus additional time for confidence)
	receipt, err := n.StateWaitMsg(ctx, publishCid, 2*constants.MessageConfidence, constants.LookbackNoLimit, true)
	if err != nil {
		return nil, xerrors.Errorf("WaitForPublishDeals errored: %w", err)
	}
	if receipt.Receipt.ExitCode != exitcode.Ok {
		return nil, xerrors.Errorf("WaitForPublishDeals exit code: %s", receipt.Receipt.ExitCode)
	}

	// The deal ID may have changed since publish if there was a reorg, so
	// get the current deal ID
	head, err := n.ChainHead(ctx)
	if err != nil {
		return nil, xerrors.Errorf("WaitForPublishDeals failed to get chain head: %w", err)
	}

	res, err := n.scMgr.dealInfo.GetCurrentDealInfo(ctx, head.Key(), (*market.DealProposal)(&proposal), publishCid)
	if err != nil {
		return nil, xerrors.Errorf("WaitForPublishDeals getting deal info errored: %w", err)
	}

	return &storagemarket.PublishDealsWaitResult{DealID: res.DealID, FinalCid: receipt.Message}, nil
}

func (n *ProviderNodeAdapter) GetDataCap(ctx context.Context, addr address.Address, encodedTs shared.TipSetToken) (*abi.StoragePower, error) {
	tsk, err := types.TipSetKeyFromBytes(encodedTs)
	if err != nil {
		return nil, err
	}

	sp, err := n.StateVerifiedClientStatus(ctx, addr, tsk)
	return sp, err
}

func (n *ProviderNodeAdapter) OnDealExpiredOrSlashed(ctx context.Context, dealID abi.DealID, onDealExpired storagemarket.DealExpiredCallback, onDealSlashed storagemarket.DealSlashedCallback) error {
	head, err := n.ChainHead(ctx)
	if err != nil {
		return xerrors.Errorf("client: failed to get chain head: %w", err)
	}

	sd, err := n.StateMarketStorageDeal(ctx, dealID, head.Key())
	if err != nil {
		return xerrors.Errorf("client: failed to look up deal %d on chain: %w", dealID, err)
	}

	// Called immediately to check if the deal has already expired or been slashed
	checkFunc := func(ctx context.Context, ts *types.TipSet) (done bool, more bool, err error) {
		if ts == nil {
			// keep listening for events
			return false, true, nil
		}

		// Check if the deal has already expired
		if sd.Proposal.EndEpoch <= ts.Height() {
			onDealExpired(nil)
			return true, false, nil
		}

		// If there is no deal assume it's already been slashed
		if sd.State.SectorStartEpoch < 0 {
			onDealSlashed(ts.Height(), nil)
			return true, false, nil
		}

		// No events have occurred yet, so return
		// done: false, more: true (keep listening for events)
		return false, true, nil
	}

	// Called when there was a match against the state change we're looking for
	// and the chain has advanced to the confidence height
	stateChanged := func(ts *types.TipSet, ts2 *types.TipSet, states events.StateChange, h abi.ChainEpoch) (more bool, err error) {
		// Check if the deal has already expired
		if ts2 == nil || sd.Proposal.EndEpoch <= ts2.Height() {
			onDealExpired(nil)
			return false, nil
		}

		// Timeout waiting for state change
		if states == nil {
			log.Error("timed out waiting for deal expiry")
			return false, nil
		}

		changedDeals, ok := states.(state.ChangedDeals)
		if !ok {
			panic("Expected state.ChangedDeals")
		}

		deal, ok := changedDeals[dealID]
		if !ok {
			// No change to deal
			return true, nil
		}

		// Deal was slashed
		if deal.To == nil {
			onDealSlashed(ts2.Height(), nil)
			return false, nil
		}

		return true, nil
	}

	// Called when there was a chain reorg and the state change was reverted
	revert := func(ctx context.Context, ts *types.TipSet) error {
		// TODO: Is it ok to just ignore this?
		log.Warn("deal state reverted; TODO: actually handle this!")
		return nil
	}

	// Watch for state changes to the deal
	match := n.dsMatcher.matcher(ctx, dealID)

	// Wait until after the end epoch for the deal and then timeout
	timeout := (sd.Proposal.EndEpoch - head.Height()) + 1
	if err := n.ev.StateChanged(checkFunc, stateChanged, revert, int(constants.MessageConfidence)+1, timeout, match); err != nil {
		return xerrors.Errorf("failed to set up state changed handler: %w", err)
	}

	return nil
}

var _ storagemarket.StorageProviderNode = &ProviderNodeAdapter{}
