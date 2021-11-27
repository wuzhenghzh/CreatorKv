// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	stores := s.filterSuitableStores(cluster)
	sort.Slice(stores, func(i, j int) bool {
		return stores[i].GetRegionSize() > stores[j].GetRegionSize()
	})

	var i int
	var region *core.RegionInfo
	var sourceStore, targetStore *core.StoreInfo

	// Find suitable region
	for i = 0; i < len(stores); i++ {
		region = s.filterSuitableRegion(stores[i], cluster)
		if region != nil {
			sourceStore = stores[i]
			break
		}
	}
	if region == nil {
		return nil
	}

	// Find suitable target store
	storeIds := region.GetStoreIds()
	for j := 0; j > i; j-- {
		if _, existed := storeIds[stores[j].GetID()]; !existed {
			if sourceStore.GetRegionSize()-stores[j].GetRegionSize() >= 2*region.GetApproximateSize() {
				targetStore = stores[j]
				break
			}
		}
	}

	if targetStore == nil {
		return nil
	}

	// Create operator
	newPeer, err := cluster.AllocPeer(targetStore.GetID())
	if err != nil {
		return nil
	}
	fmtDesc := fmt.Sprintf("Move region peer  from %d to %d", sourceStore.GetID(), targetStore.GetID())
	op, err := operator.CreateMovePeerOperator(fmtDesc, cluster, region, operator.OpBalance, sourceStore.GetID(), targetStore.GetID(), newPeer.GetId())
	if err != nil {
		return nil
	}
	return op
}

func (s *balanceRegionScheduler) filterSuitableStores(cluster opt.Cluster) []*core.StoreInfo {
	var result []*core.StoreInfo
	for _, store := range cluster.GetStores() {
		if store.IsUp() && store.DownTime() < cluster.GetMaxStoreDownTime() {
			result = append(result, store)
		}
	}
	return result
}

func (s *balanceRegionScheduler) filterSuitableRegion(sourceStore *core.StoreInfo, cluster opt.Cluster) *core.RegionInfo {
	var region *core.RegionInfo

	// Try select a pending region
	cluster.GetPendingRegionsWithLock(sourceStore.GetID(), func(container core.RegionsContainer) {
		randRegion := container.RandomRegion(nil, nil)
		if randRegion != nil {
			region = randRegion
		}
	})
	if region != nil {
		return region
	}

	// Try select a follower
	cluster.GetFollowersWithLock(sourceStore.GetID(), func(container core.RegionsContainer) {
		randRegion := container.RandomRegion(nil, nil)
		if randRegion != nil {
			region = randRegion
		}
	})
	if region != nil {
		return region
	}

	// Try select a leader
	cluster.GetLeadersWithLock(sourceStore.GetID(), func(container core.RegionsContainer) {
		randRegion := container.RandomRegion(nil, nil)
		if randRegion != nil {
			region = randRegion
		}
	})
	if region != nil {
		return region
	}

	return nil
}
