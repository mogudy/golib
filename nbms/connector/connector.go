package connector

import (
	"log"
	"net/http"
	"github.com/koding/multiconfig"
	"fmt"
	"time"
	"errors"
	"github.com/mogudy/golib/nbms/servant"
	"github.com/mogudy/golib/nbms/messenger"
	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"github.com/go-xorm/core"
	"strings"
	"strconv"
)

type(
	ConsulService interface {
		DeRegister()
		CreateDataTable(bean ...interface{})error
		InsertRecord(bean ...interface{})(int64, error)
		UpdateRecord(bean interface{}, conditions ...interface{})(int64, error)
		DeleteRecord(bean interface{})(int64, error)
		FindRecords(bean interface{}, conditions ...interface{})(error)
		RegisterMessageHandler(topic string, callback func([]byte)([]byte))(error)
		RegisterHttpHandler(api string, method string, callback func([]byte)([]byte))
		StartServer(remoteShutdown bool)error
		SendRequest(method string, service string, api string, param string)(error)
	}
	service struct {
		agent     servant.ConsulAgent
		config    *NbConfig
		messenger messenger.Connector
		started   bool
		db        *xorm.Engine
		msgp uint32
	}
)

type(
	NbConfig struct {
		Service NbAgentInfo
		Database NbServerInfo
		Health NbCheckInfo
		Amqp NbServerInfo
		Application map[string]string
	}
	NbAgentInfo struct {
		Server string  `required:"true"`
		Port uint32 `required:"true"`
		Name string `required:"true"`
		Username string
		Password string
		Tags []string
	}
	NbServerInfo struct {
		Address  string
		Port     uint32
		Name     string
		Username string `default:"guest"`
		Password string `default:"guest"`
	}
	NbCheckInfo struct {
		Method string
		Interval string
		Timeout string
	}

	RequestHistory struct {
		Id int64 `xorm:"pk autoincr"`
		Ip int `xorm:"notnull default(0)"`
		Service string `xorm:"notnull"`
		Api string `xorm:"notnull"`
		Param string `xorm:"notnull varchar(1024) default('')"`
		Method string `xorm:"notnull"`
		Direction string `xorm:"notnull"`
		RequestTime time.Time `xorm:"notnull created"`
		Version int `xorm:"notnull version"`
	}
)
const (  // iota is reset to 0
	AMQP = "AMQP"
	GET = "GET"
	POST = "POST"
	PUT = "PUT"
	DELETE = "DELETE"
	HEAD = "HEAD"
	OPTION = "OPTION"
)

func CreateService(filepath string) (ConsulService, error){
	// Loading config
	config := new (NbConfig)
	cfile := multiconfig.NewWithPath(filepath)
	err := cfile.Load(config)
	if err != nil{ return nil, err }
	cfile.MustLoad(config)

	// setup db connection & validate it
	engine, err := xorm.NewEngine("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8",
		config.Database.Username, config.Database.Password,
		config.Database.Address, config.Database.Port, config.Database.Name))
	if err != nil {return nil, err}
	engine.SetMaxIdleConns(10)
	engine.SetMaxOpenConns(10)
	engine.SetMapper(core.GonicMapper{})
	// Init table: request_history
	err = engine.Sync2(new (RequestHistory))
	if err != nil {engine.Close();return nil, err}

	// Register Service & Health check
	agent, err := servant.NewConsulClient(fmt.Sprintf("%s:%d",config.Service.Server,config.Service.Port))
	if err != nil{ engine.Close();return nil, err }
	err = agent.Register(fmt.Sprintf("%s-%s",config.Service.Name,time.Now().Format("2006010215")), config.Service.Tags, 80)
	if err != nil{ agent.DeRegister();engine.Close();return nil, err }

	return &service{agent: agent,config: config,started: false,db:engine,msgp:0},nil
}
func (s *service)DeRegister(){
	if s.db !=nil{ s.db.Close() }
	if s.messenger !=nil{ s.messenger.Close() }
	if s.agent !=nil{ s.agent.DeRegister() }
}
func (s *service)CreateDataTable(bean ...interface{})(error){
	return s.db.Sync2(bean)
}
func (s *service)InsertRecord(bean ...interface{})(int64, error){
	return s.db.Insert(bean)
}
func (s *service)UpdateRecord(bean interface{}, conditions ...interface{})(int64, error){
	return s.db.Update(bean,conditions)
}
func (s *service)DeleteRecord(bean interface{})(int64, error){
	return s.db.Delete(bean)
}
func (s *service)FindRecords(bean interface{}, cond ...interface{})(error){
	return s.db.Find(bean, cond)
}
func (s *service)RegisterMessageHandler(topic string, callback func([]byte)([]byte)) (error){
	// create a mq agent if not exist
	if s.config.Amqp.Port == 0 || s.config.Amqp.Address == "" {
		return errors.New("Amqp Address Configuration is missing.")
	}
	if s.messenger == nil {
		msger, err := messenger.CreateMessenger(s.config.Amqp.Username, s.config.Amqp.Password, s.config.Amqp.Address, s.config.Amqp.Port)
		if err!=nil{
			return err
		}
		s.messenger = msger
	}

	// create & listen to queue/topic if not registered yet
	err := s.messenger.ListenWithFunc(fmt.Sprintf("%s_%s_%d",s.config.Amqp.Name,time.Now().Format("06010215"),s.msgp),s.config.Service.Name,topic, func(param []byte)([]byte){
		s.db.Insert(RequestHistory{Service:s.config.Service.Name,Api:topic,Param:string(param),Method:"amqp",Direction:"in"})
		return callback(param)
	})
	if err==nil{s.msgp = s.msgp+1}
	return err
}
func ConvertToIntIP(ip string) (int) {
	ips := strings.Split(ip, ".")
	if len(ips) != 4 {
		return 0
	}
	var intIP int
	for k, v := range ips {
		i, err := strconv.Atoi(v)
		if err != nil || i > 255 {
			return 0
		}
		intIP = intIP | i<<uint(8*(3-k))
	}
	return intIP
}
func (s *service)RegisterHttpHandler(api string, method string, callback func([]byte)([]byte)){
	// register the api in http interface
	http.HandleFunc(api, func(resp http.ResponseWriter, req *http.Request){
		pp := make([]byte, req.ContentLength)
		_,err := req.Body.Read(pp)
		defer req.Body.Close()

		s.db.Insert(RequestHistory{Ip:ConvertToIntIP(req.RemoteAddr),Service:s.config.Service.Name,Api:api,Param:string(pp),Method:method,Direction:"in"})
		if err != nil && err.Error() != "EOF"{
			log.Printf("API: %s, error: %s, param: %s",api,err,pp)
			return
		}
		resp.Write(callback(pp))
	})
}
func (s *service)StartServer(remoteShutdown bool)error{
	if !s.started {
		svr := &http.Server{
			Addr: ":80",
			Handler:http.DefaultServeMux,
			ReadTimeout:10 * time.Second,
			WriteTimeout: 10 * time.Second,
			MaxHeaderBytes: 1 << 20,
		}
		ch := make(chan int)
		if remoteShutdown{
			http.HandleFunc("/shutdown",func(resp http.ResponseWriter, req *http.Request){
				resp.Write([]byte("{\"result\":\"success\",\"code\":200}"))
				svr.Shutdown(nil)
				ch <- 1
			})
		}
		//err := http.ListenAndServe(":80", nil)
		go svr.ListenAndServe()
		s.started = true
		log.Println("Listen to http(80)")
		select {
		case <-ch:
			log.Println("Shuting down server ... ...")
		}
	}
	return nil
}
func (s *service)SendRequest(method string, service string, api string, param string)(error){
	switch method{
	case AMQP:{
		// amqp
		return s.messenger.SendTo(service,api,param)
	}
	case GET,POST,PUT:{
		// TODO http request

	}
	}
	return errors.New("未知的请求方式")
}