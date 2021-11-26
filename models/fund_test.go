package models

import (
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/venus-market/models/badger"
	"github.com/filecoin-project/venus-market/models/repo"
	"github.com/filecoin-project/venus-market/types"
	"github.com/stretchr/testify/assert"
)

func TestFund(t *testing.T) {
	t.Run("mysql", func(t *testing.T) {
		testFund(t, MysqlDB(t).FundRepo())
	})

	t.Run("badger", func(t *testing.T) {
		db := BadgerDB(t)
		testFund(t, repo.FundRepo(badger.NewFundRepo(db)))
	})
}

func testFund(t *testing.T, fundRepo repo.FundRepo) {
	msgCid := randCid(t)
	state := &types.FundedAddressState{
		Addr:        randAddress(t),
		AmtReserved: abi.NewTokenAmount(100),
		MsgCid:      &msgCid,
	}

	state2 := &types.FundedAddressState{
		Addr:        randAddress(t),
		AmtReserved: abi.NewTokenAmount(10),
	}

	assert.Nil(t, fundRepo.SaveFundedAddressState(state))
	assert.Nil(t, fundRepo.SaveFundedAddressState(state2))

	res, err := fundRepo.GetFundedAddressState(state.Addr)
	assert.Nil(t, err)
	assert.Equal(t, res, state)
	res2, err := fundRepo.GetFundedAddressState(state2.Addr)
	assert.Nil(t, err)
	assert.Equal(t, res2, state2)

	res.AmtReserved = abi.NewTokenAmount(101)
	newCid := randCid(t)
	res.MsgCid = &newCid
	assert.Nil(t, fundRepo.SaveFundedAddressState(res))
	res3, err := fundRepo.GetFundedAddressState(res.Addr)
	assert.Nil(t, err)

	assert.Equal(t, res, res3)

	list, err := fundRepo.ListFundedAddressState()
	assert.Nil(t, err)
	assert.GreaterOrEqual(t, len(list), 2)
}
