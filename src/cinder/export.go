package cinder

import (
	"github.com/gophercloud/gophercloud"
)

func (b *BlockStore) Client() *gophercloud.ServiceClient {
	return b.client
}

func (b *BlockStore) WaitForVolumeStatus(id string) error {
	_, err := b.waitForVolumeStatus(id, snapshotStatuses, b.volumeTimeout)
	return err
}

func (b *BlockStore) WaitForSnapshotStatus(id string) error {
	_, err := b.waitForSnapshotStatus(id, snapshotStatuses, b.snapshotTimeout)
	return err
}

func (b *BlockStore) WaitForReplicaStatus(id string) error {
	return nil
}

func (b *BlockStore) WaitForReplicaSyncState(id string) error {
	return nil
}

func (b *BlockStore) WaitForReplicaActiveState(id string) error {
	return nil
}
