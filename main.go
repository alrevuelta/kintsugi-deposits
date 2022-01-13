package main

import (
	"context"
	"encoding/hex"

	"github.com/jackc/pgx/v4"
	ethpb "github.com/prysmaticlabs/prysm/v2/proto/prysm/v1alpha1"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

var createTable = `
CREATE TABLE IF NOT EXISTS t_deposits (
	 f_index BIGINT NOT NULL PRIMARY KEY,
	 f_key TEXT,
	 f_pool TEXT
);
`

var insertTable = `
INSERT INTO t_deposits(
	f_index,
	f_key,
    f_pool)
VALUES ($1, $2, $3)
ON CONFLICT (f_index)
DO UPDATE SET
    f_key=EXCLUDED.f_key,
	f_pool=EXCLUDED.f_pool
`

func main() {
	conn, err := pgx.Connect(context.Background(), "postgresql://postgres:kCim02FZj7@localhost:5432/postgres")
	if err != nil {
		log.Fatal(err)
	}
	if _, err := conn.Exec(
		context.Background(),
		createTable); err != nil {
		log.Fatal(err)
	}

	dialContext, err := grpc.DialContext(context.Background(), "localhost:4000", grpc.WithInsecure())
	if err != nil {
		log.Fatal("error: ", err)
	}

	beaconClient := ethpb.NewBeaconChainClient(dialContext)

	validators := getAllValidators(beaconClient)

	storeInDb(conn, validators)

	rows, err := conn.Query(context.Background(), "select count(*) FROM t_deposits")
	if err != nil {
		log.Fatal(err)
	}

	count := getDbRowsCount(conn, rows)
	log.Info("Wrote in db a total of ", count, " entries")

	if count != uint64(len(validators)) {
		log.Fatal("Written entries do not match")
	}
	log.Info("Written entries match expected ones. Finished ok!")
}

// Get all validators from the beacon chain, since genesis
func getAllValidators(beaconClient ethpb.BeaconChainClient) []*ethpb.Validators_ValidatorContainer {
	log.Info("Fetching all validators from genesis")
	req := &ethpb.ListValidatorsRequest{
		PageSize: 250,
		QueryFilter: &ethpb.ListValidatorsRequest_Genesis{
			Genesis: true,
		},
	}

	validators := make([]*ethpb.Validators_ValidatorContainer, 0)
	for {
		resp, err := beaconClient.ListValidators(context.Background(), req)
		if err != nil {
			log.Fatal(err)
		}

		validators = append(validators, resp.ValidatorList...)

		if resp.NextPageToken == "" {
			break
		} else {
			req.PageToken = resp.NextPageToken
		}
	}
	log.Info("Fetching validators ok!")
	return validators
}

// Stores the validator index, key and its tag in a postgres db
func storeInDb(conn *pgx.Conn, validators []*ethpb.Validators_ValidatorContainer) {
	log.Info("Storing ", len(validators), " validators in db")

	b := &pgx.Batch{}
	for _, val := range validators {
		b.Queue(
			insertTable,
			val.Index,
			hex.EncodeToString(val.Validator.PublicKey),
			getPoolTagForIndex(uint64(val.Index)))
	}
	batchResults := conn.SendBatch(context.Background(), b)
	_, err := batchResults.Exec()
	if err != nil {
		log.Error(err)
	}
	batchResults.Close()
	log.Info("Stored ok!")
}

// Tags a validator by its index to a name (i.e. pool, client, etc)
func getPoolTagForIndex(index uint64) string {
	if index >= 1 && index < 25_000 {
		return "lighthouse-geth"
	} else if index >= 25_000 && index < 26_000 {
		return "grandine-geth"
	} else if index >= 26_000 && index < 52_000 {
		return "teku-geth"
	} else if index >= 52_000 && index < 54_000 {
		return "lh-nethermind"
	} else if index >= 54_000 && index < 56_000 {
		return "lh-besu"
	} else if index >= 56_000 && index < 58_000 {
		return "lodestar-geth"
	} else if index >= 58_000 && index < 60_000 {
		return "lodestar-nethermind"
	} else if index >= 60_000 && index < 62_000 {
		return "nimbus-geth"
	} else if index >= 62_000 && index < 64_000 {
		return "nimbus-nethermind"
	} else if index >= 64_000 && index < 66_000 {
		return "prysm-geth"
	} else if index >= 66_000 && index < 68_000 {
		return "prysm-nethermind"
	} else if index >= 68_000 && index < 70_000 {
		return "teku-nethermind"
	} else if index >= 70_000 && index < 72_000 {
		return "teku-besu"
	}
	return "unlabeled"
}

func getDbRowsCount(conn *pgx.Conn, rows pgx.Rows) uint64 {
	var dbEntries uint64
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			log.Fatal(err)
		}

		for _, v := range values {
			dbEntries = uint64(v.(int64))
		}
	}
	return dbEntries
}
