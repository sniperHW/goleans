package placement

import (
	"context"
	"testing"
	"time"

	"github.com/sniperHW/clustergo/addr"
	"github.com/sniperHW/goleans/pd"
	"go.uber.org/zap"
)

func init() {
	InitLogger(zap.NewExample())
}

func TestPlacement(t *testing.T) {
	svr, err := NewServer("storage.json")
	if err != nil {
		panic(err)
	}

	err = svr.Start("127.0.0.1:9111")
	if err != nil {
		panic(err)
	}

	node1Addr, _ := addr.MakeAddr("1.1.1", "localhost:28110")
	cli1 := NewCli(node1Addr.LogicAddr(), "127.0.0.1:9111")
	cli1.SetCacheTime(time.Second * 5)
	cli1.SetGetMetric(func() pd.Metric {
		return pd.Metric{}
	})

	err = cli1.Login(context.Background(), []string{"User"})
	if err != nil {
		panic(err)
	}

	node2Addr, _ := addr.MakeAddr("1.1.2", "localhost:28111")
	cli2 := NewCli(node2Addr.LogicAddr(), "127.0.0.1:9111")
	cli2.SetCacheTime(time.Second * 5)
	cli2.SetGetMetric(func() pd.Metric {
		return pd.Metric{}
	})

	siloAddr, err := cli2.GetPlacement(context.Background(), "sniperHW@User")
	if err != nil {
		logger.Sugar().Debug(err)
	} else {
		logger.Sugar().Debugf("silo addr %s", siloAddr.String())
		err = cli1.Place(context.Background(), "sniperHW@User")
		if err != nil {
			logger.Sugar().Debug(err)
		}
	}
}
