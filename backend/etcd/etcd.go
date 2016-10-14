package etcd

import (
        "github.com/coreos/etcd/clientv3"
        "golang.org/x/net/context"
        "time"
        "log"
)

var (
        ONLINE = "ONLINE"
        OFFLINE = "OFFLINE"
        root = "/taiji/clients/"
)

type EtcdBackend struct {
        Urls []string
        Handler *clientv3.Client
}

type Backend interface {
        Init() error
        Register(string) error
        Deregister(string) error
        Disconnect(string) error
        //List() []string
        CheckClient(cid string) bool
        Store(cid string, payload string) error
        Fetch(cid string) []string
}

func (w *EtcdBackend) Init() error {
        cli, err := clientv3.New(clientv3.Config{
                Endpoints:   []string{"localhost:2379"},
                DialTimeout: 5 * time.Second,
        })
        if err != nil {
                log.Printf(err.Error())
                return err
        }
        w.Handler = cli

        log.Println("etcd backend init")
        ctx, cancel := timeout(5)
        defer cancel()
        _, err = w.Handler.Put(ctx, root, "")
        if err != nil {
                log.Println(err.Error())
        }
        return err
}

func (w *EtcdBackend) Register(cid string) error {
        log.Printf("Register %s\n", cid)
        ctx, cancel := timeout(5)
        defer cancel()
        _, err := w.Handler.Put(ctx, root + cid, ONLINE)

        if err != nil {
                log.Println(err.Error())
        }
        return err
}

func (w *EtcdBackend) Disconnect(cid string) error {
        log.Printf("Disconnect %s\n", cid)
        return nil
}

func (w *EtcdBackend) Deregister(cid string) error {
        log.Printf("Dergister %s\n", cid)
        ctx, cancel := timeout(5)
        defer cancel()

        _, err := w.Handler.Get(ctx, root + cid, clientv3.WithPrefix())
        if err == nil {
                _, err = w.Handler.Delete(ctx, root + cid)
        }
        if err != nil {
                log.Println(err.Error())
        }

        return err
}

func (w *EtcdBackend) Store(cid string, payload string) error {
        log.Printf("Store %s: %s\n", cid, payload)
        ctx, cancel := timeout(5)
        defer cancel()

        _, err := w.Handler.Put(ctx, root + cid + "/" + time.Now().Format("20060102030405"), payload)
        if err != nil {
                log.Println(err.Error())
        }
        return err
}

func (w *EtcdBackend) CheckClient(cid string) bool {
        ctx, cancel := timeout(5)
        defer cancel()
        _, err := w.Handler.Get(ctx, root + cid, clientv3.WithPrefix())
        if err == nil {
                return true
        }
        return false
}

func (w *EtcdBackend) Fetch(cid string) []string {
        var msgs []string
        ctx, cancel := timeout(5)
        defer cancel()
        resp, err := w.Handler.Get(ctx, root + cid + "/", clientv3.WithPrefix())
        for _, kv := range resp.Kvs {
                log.Println(string(kv.Key), string(kv.Value))
                msgs = append(msgs, string(kv.Value))
        }

        if err == nil {
                w.Handler.Delete(context.Background(), root + cid + "/", clientv3.WithPrefix())
        } else {
                return nil
        }
        return msgs
}

func timeout(seconds int) (context.Context, context.CancelFunc){
        return context.WithTimeout(context.Background(), seconds * time.Second)
}
