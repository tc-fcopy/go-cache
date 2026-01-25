package registry

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"net"
	"time"
)

// 定义etcd客户端配置
type Config struct {
	Endpoints   []string
	DialTimeout time.Duration
}

var DefaultConfig = &Config{
	Endpoints:   []string{"localhost:2379"},
	DialTimeout: 5 * time.Second, // 连接etcd的超时时间
}

func Register(svcName, addr string, stopCh <-chan error) error {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   DefaultConfig.Endpoints,
		DialTimeout: DefaultConfig.DialTimeout,
	})

	if err != nil {
		return fmt.Errorf("the %s failed to create etcd client:%v", svcName, err)
	}
	localIp, err := getLocalIP()
	if err != nil {
		cli.Close()
		return fmt.Errorf("the %s failed to get local ip:%v", svcName, err)
	}
	if addr[0] == ':' {
		addr = fmt.Sprintf("%s:%d", localIp, addr)
	}

	// 创建租约
	lease, err := cli.Grant(context.Background(), 10)
	if err != nil {
		cli.Close()
		return fmt.Errorf("the %s failed to create etcd client:%v", svcName, err)
	}

	// 注册服务，使用完整key协议
	key := fmt.Sprintf("/services/%s/%s", svcName, addr)
	_, err = cli.Put(context.Background(), key, addr, clientv3.WithLease(lease.ID))
	if err != nil {
		cli.Close()
		return fmt.Errorf("the %s failed to create etcd client:%v", svcName, err)
	}

	keepAlivech, err := cli.KeepAlive(context.Background(), lease.ID)
	if err != nil {
		cli.Close()
		return fmt.Errorf("the %s failed to create etcd client:%v", svcName, err)
	}

	go func() {
		defer cli.Close()
		for {
			select {
			case <-stopCh:
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				cli.Revoke(ctx, lease.ID)
				cancel()
				return
			case resp, ok := <-keepAlivech:
				if !ok {
					logrus.Warn("keep alive channel closed")
					return
				}
				logrus.Debugf("successfully renewed lease: %d", resp.ID)
			}
		}
	}()
	logrus.Infof("service registeried: %s at %s", svcName, addr)
	return nil

}

func getLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, address := range addrs {
		if ipNet, ok := address.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				return ipNet.IP.String(), nil
			}
		}
	}
	return "", fmt.Errorf("no valid local ip address found")
}
