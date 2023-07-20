package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/Lirt/velero-plugin-for-openstack/src/cinder"
	"github.com/Lirt/velero-plugin-for-openstack/src/manila"
	"github.com/gophercloud/gophercloud"
	vsnapshots "github.com/gophercloud/gophercloud/openstack/blockstorage/v3/snapshots"
	"github.com/gophercloud/gophercloud/openstack/sharedfilesystems/v2/replicas"
	"github.com/gophercloud/gophercloud/openstack/sharedfilesystems/v2/snapshots"
	"github.com/sirupsen/logrus"
)

type Snapshotter interface {
	Init(config map[string]string) error
	CreateVolumeFromSnapshot(snapshotID, volumeType, volumeAZ string, iops *int64) (volumeID string, err error)
	GetVolumeInfo(volumeID, volumeAZ string) (string, *int64, error)
	CreateSnapshot(volumeID, volumeAZ string, tags map[string]string) (snapshotID string, err error)
	DeleteSnapshot(snapshotID string) error

	WaitForVolumeStatus(id string) error
	WaitForSnapshotStatus(id string) error
	WaitForReplicaStatus(id string) error
	WaitForReplicaSyncState(id string) error
	WaitForReplicaActiveState(id string) error
	Client() *gophercloud.ServiceClient
}

var (
	log   = logrus.New()
	store Snapshotter

	nSnap       = flag.Uint("n", 1, "an amount of test snapshots before volume delete")
	sync        = flag.Bool("s", false, "whether to wait for the test replica to be active")
	region      = flag.String("r", "", "region")
	az          = flag.String("z", "", "availability zone for the replica")
	debug       = flag.Bool("d", false, "debug logs")
	volID       = flag.String("v", "", "source share volume ID")
	method      = flag.String("m", "clone", "method for the snapshot")
	storageType = flag.String("t", "manila", "storage type")
)

func main() {
	flag.Parse()

	if *debug {
		log.SetLevel(logrus.DebugLevel)
	}

	if *volID == "" {
		log.Fatalf("source share volume id must be set")
	}

	switch *storageType {
	case "manila":
		store = manila.NewFSStore(log)
	case "cinder":
		store = cinder.NewBlockStore(log)
	default:
		log.Fatalf("unknown storage type: %s", *storageType)
	}

	cfg := map[string]string{
		"cascadeDelete":      "true",
		"cloneTimeout":       "20m",
		"enforceAZ":          "true",
		"ensureDeleted":      "true",
		"ensureDeletedDelay": "10s",
		"method":             *method,
		"region":             *region,
		"replicaTimeout":     "20m",
		"shareTimeout":       "20m",
		"snapshotTimeout":    "20m",
		"backupTimeout":      "20m",
		"imageTimeout":       "20m",
	}
	err := store.Init(cfg)
	if err != nil {
		log.Errorf("error initializing: %v", err)
		return
	}

	snapID, err := store.CreateSnapshot(*volID, "", nil)
	if err != nil {
		log.Errorf("failed to create a snapshot: %v", err)
		return
	}

	defer func() {
		err = store.DeleteSnapshot(snapID)
		if err != nil {
			log.Errorf("failed to delete a snapshot: %v", err)
		}
	}()

	newVolID, err := store.CreateVolumeFromSnapshot(snapID, "", *az, nil)
	if err != nil {
		log.Errorf("failed to create a volume from snapshot: %v", err)
		return
	}

	defer func() {
		err = store.DeleteSnapshot(newVolID)
		if err != nil {
			log.Errorf("failed to delete a volume: %v", err)
		}
	}()

	if *method != "clone" {
		return
	}

	switch *storageType {
	case "manila":
		processManilaClone(newVolID)
	case "cinder":
		processCinderClone(newVolID)
	}
}

func processManilaClone(snapID string) {
	var snapIDs []string
	var i uint
	client := store.Client()
	for i = 1; i <= *nSnap; i++ {
		name := fmt.Sprintf("test snapshot from %s %d", snapID, i)
		log.Infof("creating a %q snapshot", name)
		opts := &snapshots.CreateOpts{
			Name:    name,
			ShareID: snapID,
		}
		snap, err := snapshots.Create(client, opts).Extract()
		if err != nil {
			log.Errorf("failed to create a snapshot: %v", err)
			return
		}
		snapIDs = append(snapIDs, snap.ID)
	}

	for _, id := range snapIDs {
		log.Infof("checking a %q snapshot status", id)
		err := store.WaitForSnapshotStatus(id)
		if err != nil {
			log.Errorf("%v", err)
			return
		}
	}

	// create replicas
	opts := &replicas.CreateOpts{
		ShareID:          snapID,
		AvailabilityZone: *az,
	}
	replica, err := replicas.Create(client, opts).Extract()
	if err != nil {
		log.Errorf("failed to create a replica: %v", err)
		return
	}
	log.Infof("replica %q created, waiting for its available status", replica.ID)
	err = store.WaitForReplicaStatus(replica.ID)
	if err != nil {
		log.Errorf("%v", err)
		return
	}

	if !*sync {
		return
	}

	log.Infof("syncing and activating %q replica", replica.ID)

	// sync replica
	err = replicas.Resync(client, replica.ID).ExtractErr()
	if err != nil {
		log.Errorf("failed to resync a %q share replica: %w", replica.ID, err)
		return
	}
	err = store.WaitForReplicaSyncState(replica.ID)
	if err != nil {
		log.Errorf("failed to wait for a %q share replica state: %w", replica.ID, err)
		return
	}

	// promote replica in a new AZ
	err = replicas.Promote(client, replica.ID, nil).ExtractErr()
	if err != nil {
		log.Errorf("failed to promote a %q share replica: %w", replica.ID, err)
		return
	}
	err = store.WaitForReplicaActiveState(replica.ID)
	if err != nil {
		log.Errorf("failed to wait for a %q share replica state: %w", replica.ID, err)
		return
	}
}

func processCinderClone(snapID string) {
	var snapIDs []string
	var i uint
	client := store.Client()
	for i = 1; i <= *nSnap; i++ {
		name := fmt.Sprintf("test snapshot from %s %d", snapID, i)
		log.Infof("creating a %q snapshot", name)
		opts := &vsnapshots.CreateOpts{
			Name:     name,
			VolumeID: snapID,
		}
		snap, err := vsnapshots.Create(client, opts).Extract()
		if err != nil {
			log.Errorf("failed to create a snapshot: %v", err)
			return
		}
		snapIDs = append(snapIDs, snap.ID)
	}

	for _, id := range snapIDs {
		log.Infof("checking a %q snapshot status", id)
		err := store.WaitForSnapshotStatus(id)
		if err != nil {
			log.Errorf("%v", err)
			return
		}
	}

	time.Sleep(5 * time.Second)

	return
}
