package types

import (
	"fmt"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-fil-markets/filestore"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	market "github.com/filecoin-project/specs-actors/actors/builtin/market"
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
	cbg "github.com/whyrusleeping/cbor-gen"
	xerrors "golang.org/x/xerrors"
	"io"
)

type MinerDeal struct {
	market.ClientDealProposal
	ProposalCid           cid.Cid
	AddFundsCid           *cid.Cid
	PublishCid            *cid.Cid
	Miner                 peer.ID
	Client                peer.ID
	State                 storagemarket.StorageDealStatus
	PiecePath             filestore.Path
	MetadataPath          filestore.Path
	SlashEpoch            abi.ChainEpoch
	FastRetrieval         bool
	Message               string
	FundsReserved         abi.TokenAmount
	Ref                   *storagemarket.DataRef
	AvailableForRetrieval bool

	DealID       abi.DealID
	CreationTime cbg.CborTime

	TransferChannelId *datatransfer.ChannelID
	SectorNumber      abi.SectorNumber

	InboundCAR string

	Offset abi.PaddedPieceSize
	Length abi.PaddedPieceSize
}

func (t *MinerDeal) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	if _, err := w.Write([]byte{180}); err != nil {
		return err
	}

	scratch := make([]byte, 9)

	// t.ClientDealProposal (market.ClientDealProposal) (struct)
	if len("ClientDealProposal") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"ClientDealProposal\" was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len("ClientDealProposal"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("ClientDealProposal")); err != nil {
		return err
	}

	if err := t.ClientDealProposal.MarshalCBOR(w); err != nil {
		return err
	}

	// t.ProposalCid (cid.Cid) (struct)
	if len("ProposalCid") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"ProposalCid\" was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len("ProposalCid"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("ProposalCid")); err != nil {
		return err
	}

	if err := cbg.WriteCidBuf(scratch, w, t.ProposalCid); err != nil {
		return xerrors.Errorf("failed to write cid field t.ProposalCid: %w", err)
	}

	// t.AddFundsCid (cid.Cid) (struct)
	if len("AddFundsCid") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"AddFundsCid\" was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len("AddFundsCid"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("AddFundsCid")); err != nil {
		return err
	}

	if t.AddFundsCid == nil {
		if _, err := w.Write(cbg.CborNull); err != nil {
			return err
		}
	} else {
		if err := cbg.WriteCidBuf(scratch, w, *t.AddFundsCid); err != nil {
			return xerrors.Errorf("failed to write cid field t.AddFundsCid: %w", err)
		}
	}

	// t.PublishCid (cid.Cid) (struct)
	if len("PublishCid") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"PublishCid\" was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len("PublishCid"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("PublishCid")); err != nil {
		return err
	}

	if t.PublishCid == nil {
		if _, err := w.Write(cbg.CborNull); err != nil {
			return err
		}
	} else {
		if err := cbg.WriteCidBuf(scratch, w, *t.PublishCid); err != nil {
			return xerrors.Errorf("failed to write cid field t.PublishCid: %w", err)
		}
	}

	// t.Miner (peer.ID) (string)
	if len("Miner") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"Miner\" was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len("Miner"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("Miner")); err != nil {
		return err
	}

	if len(t.Miner) > cbg.MaxLength {
		return xerrors.Errorf("Value in field t.Miner was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len(t.Miner))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string(t.Miner)); err != nil {
		return err
	}

	// t.Client (peer.ID) (string)
	if len("Client") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"Client\" was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len("Client"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("Client")); err != nil {
		return err
	}

	if len(t.Client) > cbg.MaxLength {
		return xerrors.Errorf("Value in field t.Client was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len(t.Client))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string(t.Client)); err != nil {
		return err
	}

	// t.State (uint64) (uint64)
	if len("State") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"State\" was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len("State"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("State")); err != nil {
		return err
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajUnsignedInt, uint64(t.State)); err != nil {
		return err
	}

	// t.PiecePath (filestore.Path) (string)
	if len("PiecePath") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"PiecePath\" was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len("PiecePath"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("PiecePath")); err != nil {
		return err
	}

	if len(t.PiecePath) > cbg.MaxLength {
		return xerrors.Errorf("Value in field t.PiecePath was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len(t.PiecePath))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string(t.PiecePath)); err != nil {
		return err
	}

	// t.MetadataPath (filestore.Path) (string)
	if len("MetadataPath") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"MetadataPath\" was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len("MetadataPath"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("MetadataPath")); err != nil {
		return err
	}

	if len(t.MetadataPath) > cbg.MaxLength {
		return xerrors.Errorf("Value in field t.MetadataPath was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len(t.MetadataPath))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string(t.MetadataPath)); err != nil {
		return err
	}

	// t.SlashEpoch (abi.ChainEpoch) (int64)
	if len("SlashEpoch") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"SlashEpoch\" was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len("SlashEpoch"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("SlashEpoch")); err != nil {
		return err
	}

	if t.SlashEpoch >= 0 {
		if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajUnsignedInt, uint64(t.SlashEpoch)); err != nil {
			return err
		}
	} else {
		if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajNegativeInt, uint64(-t.SlashEpoch-1)); err != nil {
			return err
		}
	}

	// t.FastRetrieval (bool) (bool)
	if len("FastRetrieval") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"FastRetrieval\" was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len("FastRetrieval"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("FastRetrieval")); err != nil {
		return err
	}

	if err := cbg.WriteBool(w, t.FastRetrieval); err != nil {
		return err
	}

	// t.Message (string) (string)
	if len("Message") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"Message\" was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len("Message"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("Message")); err != nil {
		return err
	}

	if len(t.Message) > cbg.MaxLength {
		return xerrors.Errorf("Value in field t.Message was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len(t.Message))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string(t.Message)); err != nil {
		return err
	}

	// t.FundsReserved (big.Int) (struct)
	if len("FundsReserved") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"FundsReserved\" was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len("FundsReserved"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("FundsReserved")); err != nil {
		return err
	}

	if err := t.FundsReserved.MarshalCBOR(w); err != nil {
		return err
	}

	// t.Ref (storagemarket.DataRef) (struct)
	if len("Ref") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"Ref\" was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len("Ref"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("Ref")); err != nil {
		return err
	}

	if err := t.Ref.MarshalCBOR(w); err != nil {
		return err
	}

	// t.AvailableForRetrieval (bool) (bool)
	if len("AvailableForRetrieval") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"AvailableForRetrieval\" was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len("AvailableForRetrieval"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("AvailableForRetrieval")); err != nil {
		return err
	}

	if err := cbg.WriteBool(w, t.AvailableForRetrieval); err != nil {
		return err
	}

	// t.DealID (abi.DealID) (uint64)
	if len("DealID") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"DealID\" was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len("DealID"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("DealID")); err != nil {
		return err
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajUnsignedInt, uint64(t.DealID)); err != nil {
		return err
	}

	// t.CreationTime (typegen.CborTime) (struct)
	if len("CreationTime") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"CreationTime\" was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len("CreationTime"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("CreationTime")); err != nil {
		return err
	}

	if err := t.CreationTime.MarshalCBOR(w); err != nil {
		return err
	}

	// t.TransferChannelId (datatransfer.ChannelID) (struct)
	if len("TransferChannelId") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"TransferChannelId\" was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len("TransferChannelId"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("TransferChannelId")); err != nil {
		return err
	}

	if err := t.TransferChannelId.MarshalCBOR(w); err != nil {
		return err
	}

	// t.SectorNumber (abi.SectorNumber) (uint64)
	if len("SectorNumber") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"SectorNumber\" was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len("SectorNumber"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("SectorNumber")); err != nil {
		return err
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajUnsignedInt, uint64(t.SectorNumber)); err != nil {
		return err
	}

	// t.InboundCAR (string) (string)
	if len("InboundCAR") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"InboundCAR\" was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len("InboundCAR"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("InboundCAR")); err != nil {
		return err
	}

	if len(t.InboundCAR) > cbg.MaxLength {
		return xerrors.Errorf("Value in field t.InboundCAR was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len(t.InboundCAR))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string(t.InboundCAR)); err != nil {
		return err
	}

	if len("Offset") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"Offset\" was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len("Offset"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("Offset")); err != nil {
		return err
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajUnsignedInt, uint64(t.Offset)); err != nil {
		return err
	}


	if len("Length") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"Length\" was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len("Length"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("Length")); err != nil {
		return err
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajUnsignedInt, uint64(t.Length)); err != nil {
		return err
	}
	return nil
}

func (t *MinerDeal) UnmarshalCBOR(r io.Reader) error {
	*t = MinerDeal{}

	br := cbg.GetPeeker(r)
	scratch := make([]byte, 8)

	maj, extra, err := cbg.CborReadHeaderBuf(br, scratch)
	if err != nil {
		return err
	}
	if maj != cbg.MajMap {
		return fmt.Errorf("cbor input should be of type map")
	}

	if extra > cbg.MaxLength {
		return fmt.Errorf("MinerDeal: map struct too large (%d)", extra)
	}

	var name string
	n := extra

	for i := uint64(0); i < n; i++ {

		{
			sval, err := cbg.ReadStringBuf(br, scratch)
			if err != nil {
				return err
			}

			name = string(sval)
		}

		switch name {
		// t.ClientDealProposal (market.ClientDealProposal) (struct)
		case "ClientDealProposal":

			{

				if err := t.ClientDealProposal.UnmarshalCBOR(br); err != nil {
					return xerrors.Errorf("unmarshaling t.ClientDealProposal: %w", err)
				}

			}
			// t.ProposalCid (cid.Cid) (struct)
		case "ProposalCid":

			{

				c, err := cbg.ReadCid(br)
				if err != nil {
					return xerrors.Errorf("failed to read cid field t.ProposalCid: %w", err)
				}

				t.ProposalCid = c

			}
			// t.AddFundsCid (cid.Cid) (struct)
		case "AddFundsCid":

			{

				b, err := br.ReadByte()
				if err != nil {
					return err
				}
				if b != cbg.CborNull[0] {
					if err := br.UnreadByte(); err != nil {
						return err
					}

					c, err := cbg.ReadCid(br)
					if err != nil {
						return xerrors.Errorf("failed to read cid field t.AddFundsCid: %w", err)
					}

					t.AddFundsCid = &c
				}

			}
			// t.PublishCid (cid.Cid) (struct)
		case "PublishCid":

			{

				b, err := br.ReadByte()
				if err != nil {
					return err
				}
				if b != cbg.CborNull[0] {
					if err := br.UnreadByte(); err != nil {
						return err
					}

					c, err := cbg.ReadCid(br)
					if err != nil {
						return xerrors.Errorf("failed to read cid field t.PublishCid: %w", err)
					}

					t.PublishCid = &c
				}

			}
			// t.Miner (peer.ID) (string)
		case "Miner":

			{
				sval, err := cbg.ReadStringBuf(br, scratch)
				if err != nil {
					return err
				}

				t.Miner = peer.ID(sval)
			}
			// t.Client (peer.ID) (string)
		case "Client":

			{
				sval, err := cbg.ReadStringBuf(br, scratch)
				if err != nil {
					return err
				}

				t.Client = peer.ID(sval)
			}
			// t.State (uint64) (uint64)
		case "State":

			{

				maj, extra, err = cbg.CborReadHeaderBuf(br, scratch)
				if err != nil {
					return err
				}
				if maj != cbg.MajUnsignedInt {
					return fmt.Errorf("wrong type for uint64 field")
				}
				t.State = uint64(extra)

			}
			// t.PiecePath (filestore.Path) (string)
		case "PiecePath":

			{
				sval, err := cbg.ReadStringBuf(br, scratch)
				if err != nil {
					return err
				}

				t.PiecePath = filestore.Path(sval)
			}
			// t.MetadataPath (filestore.Path) (string)
		case "MetadataPath":

			{
				sval, err := cbg.ReadStringBuf(br, scratch)
				if err != nil {
					return err
				}

				t.MetadataPath = filestore.Path(sval)
			}
			// t.SlashEpoch (abi.ChainEpoch) (int64)
		case "SlashEpoch":
			{
				maj, extra, err := cbg.CborReadHeaderBuf(br, scratch)
				var extraI int64
				if err != nil {
					return err
				}
				switch maj {
				case cbg.MajUnsignedInt:
					extraI = int64(extra)
					if extraI < 0 {
						return fmt.Errorf("int64 positive overflow")
					}
				case cbg.MajNegativeInt:
					extraI = int64(extra)
					if extraI < 0 {
						return fmt.Errorf("int64 negative oveflow")
					}
					extraI = -1 - extraI
				default:
					return fmt.Errorf("wrong type for int64 field: %d", maj)
				}

				t.SlashEpoch = abi.ChainEpoch(extraI)
			}
			// t.FastRetrieval (bool) (bool)
		case "FastRetrieval":

			maj, extra, err = cbg.CborReadHeaderBuf(br, scratch)
			if err != nil {
				return err
			}
			if maj != cbg.MajOther {
				return fmt.Errorf("booleans must be major type 7")
			}
			switch extra {
			case 20:
				t.FastRetrieval = false
			case 21:
				t.FastRetrieval = true
			default:
				return fmt.Errorf("booleans are either major type 7, value 20 or 21 (got %d)", extra)
			}
			// t.Message (string) (string)
		case "Message":

			{
				sval, err := cbg.ReadStringBuf(br, scratch)
				if err != nil {
					return err
				}

				t.Message = string(sval)
			}
			// t.FundsReserved (big.Int) (struct)
		case "FundsReserved":

			{

				if err := t.FundsReserved.UnmarshalCBOR(br); err != nil {
					return xerrors.Errorf("unmarshaling t.FundsReserved: %w", err)
				}

			}
			// t.Ref (storagemarket.DataRef) (struct)
		case "Ref":

			{

				b, err := br.ReadByte()
				if err != nil {
					return err
				}
				if b != cbg.CborNull[0] {
					if err := br.UnreadByte(); err != nil {
						return err
					}
					t.Ref = new(storagemarket.DataRef)
					if err := t.Ref.UnmarshalCBOR(br); err != nil {
						return xerrors.Errorf("unmarshaling t.Ref pointer: %w", err)
					}
				}

			}
			// t.AvailableForRetrieval (bool) (bool)
		case "AvailableForRetrieval":

			maj, extra, err = cbg.CborReadHeaderBuf(br, scratch)
			if err != nil {
				return err
			}
			if maj != cbg.MajOther {
				return fmt.Errorf("booleans must be major type 7")
			}
			switch extra {
			case 20:
				t.AvailableForRetrieval = false
			case 21:
				t.AvailableForRetrieval = true
			default:
				return fmt.Errorf("booleans are either major type 7, value 20 or 21 (got %d)", extra)
			}
			// t.DealID (abi.DealID) (uint64)
		case "DealID":

			{

				maj, extra, err = cbg.CborReadHeaderBuf(br, scratch)
				if err != nil {
					return err
				}
				if maj != cbg.MajUnsignedInt {
					return fmt.Errorf("wrong type for uint64 field")
				}
				t.DealID = abi.DealID(extra)

			}
			// t.CreationTime (typegen.CborTime) (struct)
		case "CreationTime":

			{

				if err := t.CreationTime.UnmarshalCBOR(br); err != nil {
					return xerrors.Errorf("unmarshaling t.CreationTime: %w", err)
				}

			}
			// t.TransferChannelId (datatransfer.ChannelID) (struct)
		case "TransferChannelId":

			{

				b, err := br.ReadByte()
				if err != nil {
					return err
				}
				if b != cbg.CborNull[0] {
					if err := br.UnreadByte(); err != nil {
						return err
					}
					t.TransferChannelId = new(datatransfer.ChannelID)
					if err := t.TransferChannelId.UnmarshalCBOR(br); err != nil {
						return xerrors.Errorf("unmarshaling t.TransferChannelId pointer: %w", err)
					}
				}

			}
			// t.SectorNumber (abi.SectorNumber) (uint64)
		case "SectorNumber":

			{

				maj, extra, err = cbg.CborReadHeaderBuf(br, scratch)
				if err != nil {
					return err
				}
				if maj != cbg.MajUnsignedInt {
					return fmt.Errorf("wrong type for uint64 field")
				}
				t.SectorNumber = abi.SectorNumber(extra)

			}
			// t.InboundCAR (string) (string)
		case "InboundCAR":

			{
				sval, err := cbg.ReadStringBuf(br, scratch)
				if err != nil {
					return err
				}

				t.InboundCAR = string(sval)
			}
		case "Offset":
			{

				maj, extra, err = cbg.CborReadHeaderBuf(br, scratch)
				if err != nil {
					return err
				}
				if maj != cbg.MajUnsignedInt {
					return fmt.Errorf("wrong type for uint64 field")
				}
				t.Offset = abi.PaddedPieceSize(extra)

			}
		case "Length":
			{
				maj, extra, err = cbg.CborReadHeaderBuf(br, scratch)
				if err != nil {
					return err
				}
				if maj != cbg.MajUnsignedInt {
					return fmt.Errorf("wrong type for uint64 field")
				}
				t.Length = abi.PaddedPieceSize(extra)

			}

		default:
			// Field doesn't exist on this type, so ignore it
			cbg.ScanForLinks(r, func(cid.Cid) {})
		}
	}

	return nil
}
