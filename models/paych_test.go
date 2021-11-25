package models

import (
	"os"
	"testing"

	"github.com/filecoin-project/venus-market/models/badger"
	"github.com/filecoin-project/venus-market/models/repo"

	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/venus-market/types"
	paychTypes "github.com/filecoin-project/venus/pkg/types/specactors/builtin/paych"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"golang.org/x/xerrors"
)

func TestPaych(t *testing.T) {
	t.Run("mysql", func(t *testing.T) {
		testChannelInfo(t, MysqlDB(t).PaychChannelInfoRepo(), MysqlDB(t).PaychMsgInfoRepo())
		testMsgInfo(t, MysqlDB(t).PaychMsgInfoRepo())
	})

	t.Run("badger", func(t *testing.T) {
		path := "./badger_paych_db"
		db := BadgerDB(t, path)
		defer func() {
			assert.Nil(t, db.Close())
			assert.Nil(t, os.RemoveAll(path))

		}()
		ps := badger.NewPaychRepo(db)
		testChannelInfo(t, repo.PaychChannelInfoRepo(ps), repo.PaychMsgInfoRepo(ps))
		testMsgInfo(t, repo.PaychMsgInfoRepo(ps))
	})
}

func testChannelInfo(t *testing.T, channelRepo repo.PaychChannelInfoRepo, msgRepo repo.PaychMsgInfoRepo) {
	msgInfo := &types.MsgInfo{
		ChannelID: uuid.New().String(),
		MsgCid:    randCid(t),
		Received:  false,
		Err:       "",
	}
	assert.Nil(t, msgRepo.SaveMessage(msgInfo))

	addr := randAddress(t)
	msgCid := randCid(t)
	vouchers := []*types.VoucherInfo{
		{
			Voucher: &paychTypes.SignedVoucher{
				ChannelAddr: addr,
				Nonce:       10,
				Amount:      big.NewInt(100),
				Extra: &paychTypes.ModVerifyParams{
					Actor:  addr,
					Method: 1,
					Data:   nil,
				},
			},
			Proof:     nil,
			Submitted: false,
		},
	}
	ci := &types.ChannelInfo{
		ChannelID:     msgInfo.ChannelID,
		Channel:       &addr,
		Control:       randAddress(t),
		Target:        randAddress(t),
		Direction:     types.DirOutbound,
		Vouchers:      vouchers,
		NextLane:      10,
		Amount:        big.NewInt(10),
		PendingAmount: big.NewInt(100),
		CreateMsg:     &msgCid,
		//AddFundsMsg:   &msgCid,
		Settling: false,
	}

	addr2 := randAddress(t)
	msgCid2 := randCid(t)
	ci2 := &types.ChannelInfo{
		ChannelID: uuid.NewString(),
		Channel:   &addr2,
		//Control:       randAddress(t),
		//Target:        randAddress(t),
		Direction:     types.DirInbound,
		Vouchers:      nil,
		NextLane:      102,
		Amount:        big.NewInt(102),
		PendingAmount: big.NewInt(1002),
		CreateMsg:     &msgCid,
		AddFundsMsg:   &msgCid2,
		Settling:      true,
	}

	ci3 := &types.ChannelInfo{}
	*ci3 = *ci2
	ci3.Channel = nil
	ci3.ChannelID = uuid.NewString()

	assert.Nil(t, channelRepo.SaveChannel(ci))
	assert.Nil(t, channelRepo.SaveChannel(ci2))
	assert.Nil(t, channelRepo.SaveChannel(ci3))

	res, err := channelRepo.GetChannelByChannelID(ci.ChannelID)
	assert.Nil(t, err)
	assert.Equal(t, res, ci)
	res2, err := channelRepo.GetChannelByChannelID(ci2.ChannelID)
	assert.Nil(t, err)
	assert.Equal(t, res2, ci2)
	res_3, err := channelRepo.GetChannelByChannelID(ci3.ChannelID)
	assert.Nil(t, err)
	assert.Equal(t, res_3, ci3)

	res3, err := channelRepo.GetChannelByAddress(*ci.Channel)
	assert.Nil(t, err)
	assert.Equal(t, res3, ci)

	res4, err := channelRepo.GetChannelByMessageCid(msgInfo.MsgCid)
	assert.Nil(t, err)
	assert.Equal(t, res4, ci)

	from, to := randAddress(t), randAddress(t)
	chMsgCid := randCid(t)
	amt := big.NewInt(101)
	ciRes, err := channelRepo.CreateChannel(from, to, chMsgCid, amt)
	assert.Nil(t, err)
	ciRes2, err := channelRepo.GetChannelByChannelID(ciRes.ChannelID)
	assert.Nil(t, err)
	assert.Equal(t, ciRes.Control, ciRes2.Control)
	assert.Equal(t, ciRes.Target, ciRes2.Target)
	assert.Equal(t, ciRes.CreateMsg, ciRes2.CreateMsg)
	assert.Equal(t, ciRes.PendingAmount, ciRes2.PendingAmount)
	msgInfoRes, err := msgRepo.GetMessage(chMsgCid)
	assert.Nil(t, err)
	assert.Equal(t, msgInfoRes.ChannelID, ciRes.ChannelID)

	addrs, err := channelRepo.ListChannel()
	assert.Nil(t, err)
	assert.GreaterOrEqual(t, len(addrs), 2)

	res5, err := channelRepo.WithPendingAddFunds()
	assert.Nil(t, err)
	assert.GreaterOrEqual(t, len(res5), 1)
	//assert.Equal(t, res5[0].ChannelID, ci.ChannelID)

	res6, err := channelRepo.OutboundActiveByFromTo(ci.Control, ci.Target)
	assert.Nil(t, err)
	assert.Equal(t, res6.ChannelID, ci.ChannelID)
}

func testMsgInfo(t *testing.T, msgRepo repo.PaychMsgInfoRepo) {
	info := &types.MsgInfo{
		ChannelID: uuid.New().String(),
		MsgCid:    randCid(t),
		Received:  false,
		Err:       "",
	}

	info2 := &types.MsgInfo{
		ChannelID: uuid.New().String(),
		MsgCid:    randCid(t),
		Received:  true,
		Err:       "err",
	}

	assert.Nil(t, msgRepo.SaveMessage(info))
	assert.Nil(t, msgRepo.SaveMessage(info2))

	res, err := msgRepo.GetMessage(info.MsgCid)
	assert.Nil(t, err)
	assert.Equal(t, res, info)
	res2, err := msgRepo.GetMessage(info2.MsgCid)
	assert.Nil(t, err)
	assert.Equal(t, res2, info2)

	errMsg := xerrors.Errorf("test err")
	assert.Nil(t, msgRepo.SaveMessageResult(info.MsgCid, errMsg))
	res3, err := msgRepo.GetMessage(info.MsgCid)
	assert.Nil(t, err)
	assert.Equal(t, res3.Err, errMsg.Error())
}