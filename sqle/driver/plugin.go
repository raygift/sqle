package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"actiontech.cloud/sqle/sqle/sqle/driver/proto"
	"actiontech.cloud/sqle/sqle/sqle/log"
	"actiontech.cloud/sqle/sqle/sqle/model"

	goPlugin "github.com/hashicorp/go-plugin"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func NewInitRequest(cfg *Config) *proto.InitRequest {
	if cfg.Inst != nil {
		return &proto.InitRequest{
			InstanceMeta: &proto.InstanceMeta{
				InstanceHost: cfg.Inst.Host,
				InstancePort: cfg.Inst.Port,
				InstanceUser: cfg.Inst.User,
				InstancePass: cfg.Inst.Password,
				DatabaseOpen: cfg.Schema,
			},
			IsOffline: cfg.IsOfflineAudit,
		}
	}
	return &proto.InitRequest{IsOffline: cfg.IsOfflineAudit}
}

// InitPlugins init plugins at plugins directory. It should be called on host process.
func InitPlugins(pluginDir string) error {
	if pluginDir == "" {
		return nil
	}

	getServerHandle := func(path string, closeCh <-chan struct{}) (proto.DriverClient, error) {
		client := goPlugin.NewClient(&goPlugin.ClientConfig{
			HandshakeConfig: handshakeConfig,
			Plugins: goPlugin.PluginSet{
				filepath.Base(path): &driverPlugin{},
			},
			Cmd:              exec.Command(path),
			AllowedProtocols: []goPlugin.Protocol{goPlugin.ProtocolGRPC},
		})
		go func() {
			select {
			case <-closeCh:
				client.Kill()
			}
		}()

		gRPCClient, err := client.Client()
		if err != nil {
			return nil, err
		}
		rawI, err := gRPCClient.Dispense(filepath.Base(path))
		if err != nil {
			return nil, err
		}
		srv := rawI.(proto.DriverClient)
		return srv, nil
	}

	var plugins []os.FileInfo
	filepath.Walk(pluginDir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() || info.Mode()&0111 == 0 {
			return nil
		}
		plugins = append(plugins, info)
		return nil
	})
	for _, p := range plugins {
		binaryPath := filepath.Join(pluginDir, p.Name())

		closeCh := make(chan struct{})
		srv, err := getServerHandle(binaryPath, closeCh)
		if err != nil {
			return err
		}
		pluginMeta, err := srv.Metas(context.TODO(), &proto.Empty{})
		if err != nil {
			return err
		}
		close(closeCh)

		var modelRules []*model.Rule
		for _, rule := range pluginMeta.Rules {
			modelRules = append(modelRules, &model.Rule{
				Typ:       rule.Typ,
				Name:      rule.Name,
				Desc:      rule.Desc,
				Value:     rule.Value,
				Level:     rule.Level,
				IsDefault: rule.IsDefault,

				DBType: pluginMeta.Name,
			})
		}

		Register(pluginMeta.Name,
			func(log *logrus.Entry, config *Config) (Driver, error) {
				pluginCloseCh := make(chan struct{})
				srv, err := getServerHandle(binaryPath, pluginCloseCh)
				if err != nil {
					return nil, err
				}
				_, err = srv.Init(context.TODO(), NewInitRequest(config))
				if err != nil {
					return nil, err
				}
				return &driverPluginClient{srv, pluginCloseCh}, nil
			},
			modelRules)

		log.Logger().WithFields(logrus.Fields{
			"plugin_name": pluginMeta.Name,
		}).Infoln("plugin inited")
	}

	return nil
}

// ServePlugin start plugin process service. It should be called on plugin process.
// initDriver is a closure which should hold the pointer to driver.
func ServePlugin(driver Driver, base BaseDriver, initDriver func(cfg *Config)) {
	name := base.Name()
	goPlugin.Serve(&goPlugin.ServeConfig{
		HandshakeConfig: handshakeConfig,

		Plugins: goPlugin.PluginSet{
			name: &driverPlugin{Srv: &driverGRPCServer{Impl: driver, Base: base, init: initDriver}},
		},

		// A non-nil value here enables gRPC serving for this plugin...
		GRPCServer: goPlugin.DefaultGRPCServer,
	})
}

var handshakeConfig = goPlugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "BASIC_PLUGIN",
	MagicCookieValue: "hello",
}

// driverPluginClient implement Driver. It use for hide gRPC detail, just like DriverGRPCServer.
type driverPluginClient struct {
	plugin proto.DriverClient

	// driverQuitCh pruduce a singal for telling caller that it's time to Client.Kill() plugin process.
	driverQuitCh chan struct{}
}

func (s *driverPluginClient) Close(ctx context.Context) {
	s.plugin.Close(ctx, &proto.Empty{})
	close(s.driverQuitCh)
}

func (s *driverPluginClient) Ping(ctx context.Context) error {
	_, err := s.plugin.Ping(ctx, &proto.Empty{})
	return err
}

type dbDriverResult struct {
	lastInsertId    int64
	lastInsertIdErr string
	rowsAffected    int64
	rowsAffectedErr string
}

func (s *dbDriverResult) LastInsertId() (int64, error) {
	if s.lastInsertIdErr != "" {
		return s.lastInsertId, fmt.Errorf(s.lastInsertIdErr)
	}
	return s.lastInsertId, nil
}

func (s *dbDriverResult) RowsAffected() (int64, error) {
	if s.rowsAffectedErr != "" {
		return s.rowsAffected, fmt.Errorf(s.rowsAffectedErr)
	}
	return s.rowsAffected, nil
}

func (s *driverPluginClient) Exec(ctx context.Context, query string) (driver.Result, error) {
	resp, err := s.plugin.Exec(ctx, &proto.ExecRequest{Query: query})
	if err != nil {
		return nil, err
	}
	return &dbDriverResult{
		lastInsertId:    resp.LastInsertId,
		lastInsertIdErr: resp.LastInsertIdError,
		rowsAffected:    resp.RowsAffected,
		rowsAffectedErr: resp.RowsAffectedError,
	}, nil
}

func (s *driverPluginClient) Tx(ctx context.Context, queries ...string) ([]driver.Result, error) {
	resp, err := s.plugin.Tx(ctx, &proto.TxRequest{Queries: queries})
	if err != nil {
		return nil, err
	}

	var ret []driver.Result
	for _, result := range resp.Resluts {
		ret = append(ret, &dbDriverResult{
			lastInsertId:    result.LastInsertId,
			lastInsertIdErr: result.LastInsertIdError,
			rowsAffected:    result.RowsAffected,
			rowsAffectedErr: result.RowsAffectedError,
		})
	}
	return ret, nil
}

func (s *driverPluginClient) Schemas(ctx context.Context) ([]string, error) {
	resp, err := s.plugin.Databases(ctx, &proto.Empty{})
	if err != nil {
		return nil, err
	}
	return resp.Databases, nil
}

func (s *driverPluginClient) Parse(ctx context.Context, sqlText string) ([]Node, error) {
	resp, err := s.plugin.Parse(ctx, &proto.ParseRequest{SqlText: sqlText})
	if err != nil {
		return nil, err
	}

	var nodes []Node
	for _, node := range resp.Nodes {
		nodes = append(nodes, Node{
			Type:        node.Type,
			Text:        node.Text,
			Fingerprint: node.Fingerprint,
		})
	}
	return nodes, nil
}

func (s *driverPluginClient) Audit(ctx context.Context, rules []*model.Rule, sql string) (*AuditResult, error) {
	var protoRules []*proto.Rule
	for _, rule := range rules {
		protoRules = append(protoRules, &proto.Rule{
			Name:  rule.Name,
			Desc:  rule.Desc,
			Value: rule.Value,
			Level: rule.Level,
			Typ:   rule.Typ,
		})
	}
	resp, err := s.plugin.Audit(ctx, &proto.AuditRequest{Rules: protoRules, Sql: sql})
	if err != nil {
		return nil, err
	}

	ret := &AuditResult{}
	for _, result := range resp.Results {
		ret.results = append(ret.results, &auditResult{
			level:   result.Level,
			message: result.Message,
		})
	}
	return ret, nil
}

func (s *driverPluginClient) GenRollbackSQL(ctx context.Context, sql string) (string, string, error) {
	resp, err := s.plugin.GenRollbackSQL(ctx, &proto.GenRollbackSQLRequest{Sql: sql})
	if err != nil {
		return "", "", err
	}

	return resp.Sql, resp.Reason, nil
}

// driverPlugin use for hide gRPC detail.
type driverGRPCServer struct {
	// todo: func params whould be replcate with DNS.
	init func(cfg *Config)

	// Impl is plugin's Driver implementation by plugin.
	Impl Driver

	// Base provide some plugin info to host process.
	Base BaseDriver
}

func (d *driverGRPCServer) Init(ctx context.Context, req *proto.InitRequest) (*proto.Empty, error) {
	d.init(&Config{
		IsOfflineAudit: req.GetIsOffline(),
		Schema:         req.GetInstanceMeta().GetDatabaseOpen(),
		Inst: &model.Instance{
			Host:     req.GetInstanceMeta().GetInstanceHost(),
			Port:     req.GetInstanceMeta().GetInstancePort(),
			User:     req.GetInstanceMeta().GetInstanceUser(),
			Password: req.GetInstanceMeta().GetInstancePass(),
		}})
	return &proto.Empty{}, nil
}

func (d *driverGRPCServer) Close(ctx context.Context, req *proto.Empty) (*proto.Empty, error) {
	d.Impl.Close(ctx)
	return &proto.Empty{}, nil
}

func (d *driverGRPCServer) Ping(ctx context.Context, req *proto.Empty) (*proto.Empty, error) {
	return &proto.Empty{}, d.Impl.Ping(ctx)
}

func (d *driverGRPCServer) Exec(ctx context.Context, req *proto.ExecRequest) (*proto.ExecResponse, error) {
	result, err := d.Impl.Exec(ctx, req.GetQuery())
	if err != nil {
		return &proto.ExecResponse{}, nil
	}

	resp := &proto.ExecResponse{}
	lastInsertId, lastInsertIdErr := result.LastInsertId()
	resp.LastInsertId = lastInsertId
	if lastInsertIdErr != nil {
		resp.LastInsertIdError = lastInsertIdErr.Error()
	}
	rowsAffected, rowsAffectedErr := result.RowsAffected()
	resp.RowsAffected = rowsAffected
	if rowsAffectedErr != nil {
		resp.RowsAffectedError = rowsAffectedErr.Error()
	}
	return resp, nil
}

func (d *driverGRPCServer) Tx(ctx context.Context, req *proto.TxRequest) (*proto.TxResponse, error) {
	resluts, err := d.Impl.Tx(ctx, req.GetQueries()...)
	if err != nil {
		return &proto.TxResponse{}, nil
	}

	txResp := &proto.TxResponse{}
	for _, result := range resluts {
		resp := &proto.ExecResponse{}

		lastInsertId, lastInsertIdErr := result.LastInsertId()
		resp.LastInsertId = lastInsertId
		if lastInsertIdErr != nil {
			resp.LastInsertIdError = lastInsertIdErr.Error()
		}
		rowsAffected, rowsAffectedErr := result.RowsAffected()
		resp.RowsAffected = rowsAffected
		if rowsAffectedErr != nil {
			resp.RowsAffectedError = rowsAffectedErr.Error()
		}

		txResp.Resluts = append(txResp.Resluts, resp)
	}
	return txResp, nil
}

func (d *driverGRPCServer) Databases(ctx context.Context, req *proto.Empty) (*proto.DatabasesResponse, error) {
	databases, err := d.Impl.Schemas(ctx)
	return &proto.DatabasesResponse{Databases: databases}, err
}

func (d *driverGRPCServer) Parse(ctx context.Context, req *proto.ParseRequest) (*proto.ParseResponse, error) {
	nodes, err := d.Impl.Parse(ctx, req.GetSqlText())
	if err != nil {
		return &proto.ParseResponse{}, err
	}

	resp := &proto.ParseResponse{}
	for _, node := range nodes {
		resp.Nodes = append(resp.Nodes, &proto.Node{
			Text:        node.Text,
			Type:        node.Type,
			Fingerprint: node.Fingerprint,
		})
	}
	return resp, nil
}

func (d *driverGRPCServer) Audit(ctx context.Context, req *proto.AuditRequest) (*proto.AuditResponse, error) {
	var modelRules []*model.Rule
	for _, r := range req.GetRules() {
		modelRules = append(modelRules, &model.Rule{
			Name:  r.Name,
			Desc:  r.Desc,
			Value: r.Value,
			Level: r.Level,
			Typ:   r.Typ,
		})
	}
	auditResluts, err := d.Impl.Audit(ctx, modelRules, req.GetSql())
	if err != nil {
		return &proto.AuditResponse{}, nil
	}

	resp := &proto.AuditResponse{}
	for _, result := range auditResluts.results {
		resp.Results = append(resp.Results, &proto.AuditResult{
			Level:   result.level,
			Message: result.message,
		})
	}
	return resp, nil
}

func (d *driverGRPCServer) GenRollbackSQL(ctx context.Context, req *proto.GenRollbackSQLRequest) (*proto.GenRollbackSQLResponse, error) {
	rollbackSQL, reason, err := d.Impl.GenRollbackSQL(ctx, req.GetSql())
	return &proto.GenRollbackSQLResponse{
		Sql:    rollbackSQL,
		Reason: reason,
	}, err
}

func (d *driverGRPCServer) Metas(ctx context.Context, req *proto.Empty) (*proto.MetasResponse, error) {
	var protoRules []*proto.Rule

	for _, r := range d.Base.Rules() {
		protoRules = append(protoRules, &proto.Rule{
			Name:      r.Name,
			Desc:      r.Desc,
			Level:     r.Level,
			Value:     r.Value,
			Typ:       r.Typ,
			IsDefault: r.IsDefault,
		})
	}

	return &proto.MetasResponse{
		Name:  d.Base.Name(),
		Rules: protoRules,
	}, nil
}

// driverPlugin implements goPlugin.GRPCPlugin
type driverPlugin struct {
	goPlugin.NetRPCUnsupportedPlugin

	Srv *driverGRPCServer
}

func (dp *driverPlugin) GRPCServer(broker *goPlugin.GRPCBroker, s *grpc.Server) error {
	proto.RegisterDriverServer(s, dp.Srv)
	return nil
}

func (dp *driverPlugin) GRPCClient(ctx context.Context, broker *goPlugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return proto.NewDriverClient(c), nil
}