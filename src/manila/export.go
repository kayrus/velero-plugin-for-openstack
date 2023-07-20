package manila

import (
	"github.com/gophercloud/gophercloud"
)

func (b *FSStore) Client() *gophercloud.ServiceClient {
	return b.client
}

func (b *FSStore) WaitForVolumeStatus(id string) error {
	_, err := b.waitForShareStatus(id, snapshotStatuses, b.snapshotTimeout)
	return err
}

func (b *FSStore) WaitForSnapshotStatus(id string) error {
	_, err := b.waitForSnapshotStatus(id, snapshotStatuses, b.snapshotTimeout)
	return err
}

func (b *FSStore) WaitForReplicaStatus(id string) error {
	_, err := b.waitForReplicaStatus(id, replicaStatuses, b.replicaTimeout)
	return err
}

func (b *FSStore) WaitForReplicaSyncState(id string) error {
	_, err := b.waitForReplicaState(id, replicaInSyncStates, b.replicaTimeout)
	return err
}

func (b *FSStore) WaitForReplicaActiveState(id string) error {
	_, err := b.waitForReplicaState(id, replicaActiveStates, b.replicaTimeout)
	return err
}
