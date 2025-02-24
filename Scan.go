package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/gagliardetto/solana-go/rpc/jsonrpc"
	"github.com/samber/lo"
	"golang.org/x/time/rate"
	"sync"
	"time"
)

type ScanBlock struct {
	rpcClient *rpc.Client
}

func New() *ScanBlock {
	url := "https://divine-blue-lambo.solana-mainnet.quiknode.pro/6b3c971fe457494b5068a3527d4934a7564fe178"
	rpcClient := rpc.NewWithCustomRPCClient(rpc.NewWithLimiter(
		url,
		rate.Every(time.Second), // time frame
		100,                     // limit of requests per time frame
	))

	return &ScanBlock{rpcClient: rpcClient}
}

func (s *ScanBlock) DealFromTo(ctx context.Context, startBlock, endBlock uint64) (outs map[uint64]*rpc.GetParsedBlockResult, err error) {
	defer func(start time.Time) {
		d := fmt.Sprintf("startBlock:%v endBlock:%v :timeCast:%v", startBlock, endBlock, time.Since(start).String())
		fmt.Printf("%s\n", d)
	}(time.Now())
	outs = make(map[uint64]*rpc.GetParsedBlockResult)
	reqs := make([]*jsonrpc.RPCRequest, 0, int(endBlock-startBlock)+1)
	seqs, _ := s.rpcClient.GetBlocks(context.Background(), startBlock, &endBlock, rpc.CommitmentConfirmed)
	for _, slot := range seqs {

		// 不需要parsed，速度要快
		reqs = append(reqs, &jsonrpc.RPCRequest{ID: slot,
			JSONRPC: "2.0", Method: "getBlock",
			Params: []interface{}{slot,
				//map[string]interface{}{"rewards": false, "maxSupportedTransactionVersion": 0},
				map[string]interface{}{"encoding": solana.EncodingJSONParsed, "rewards": false, "maxSupportedTransactionVersion": 0},
			}})
	}

	ress, err := s.rpcClient.RPCCallBatch(ctx, reqs)
	if err != nil {
		return nil, err
	}
	for _, res := range ress {
		if res.Error != nil {
			return nil, res.Error
		}
	}

	for index, res := range ress {
		if res.Result == nil {
			continue
		}
		//var block = rpc.GetParsedBlockResult{}
		var block = rpc.GetParsedBlockResult{}
		if err := json.Unmarshal(res.Result, &block); err != nil {
			return nil, err
		}

		outs[seqs[uint64(index)]] = &block
	}
	return
}

func (s *ScanBlock) ScanRange() {

	startSlot := 322358723

	gap := 1000

	seqs := lo.RangeFrom(startSlot, gap)

	chunks := lo.Chunk(seqs, 20)

	var wg sync.WaitGroup
	for _, chunk := range chunks {
		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			s.DealFromTo(context.Background(), uint64(start), uint64(end))
		}(lo.Min(chunk), lo.Max(chunk))

	}

	wg.Wait()
}
