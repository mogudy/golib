package connector

import (
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
	"strconv"
	"log"
	"github.com/mogudy/golib/nbms/logwriter"
	"encoding/json"
	"bytes"
	"io/ioutil"
	consul "github.com/hashicorp/consul/api"
)

type(
	ConsulService interface {
		Config()*NbConfig
		Services()(map[string]*consul.AgentService,error)
		DeRegister()
		CreateDataTable(bean ...interface{})error
		InsertRecord(bean ...interface{})(int64, error)
		UpdateRecord(bean interface{}, conditions ...interface{})(int64, error)
		DeleteRecord(bean interface{})(int64, error)
		FindRecords(bean interface{}, query interface{}, args ...interface{})(error)
		GetFirstRecord(bean interface{})(bool, error)
		RegisterMessageHandler(api string, callback func([]byte)([]byte,error))(error)
		RegisterHttpHandler(api string, method string, callback func([]byte)([]byte,error))
		StartServer(remoteShutdown bool)error
		SendRequest(method string, service string, api string, param string)(error)
	}
	service struct {
		agent     servant.ConsulAgent
		config    *NbConfig
		messenger messenger.Connector
		started   bool
		db        *xorm.Engine
		topicno   uint32
		client    *http.Client
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
		Autoregister bool `default:"false"`
		Server string  `required:"true"`
		Port uint32 `required:"true"`
		Name string `required:"true"`
		Id string `required:"true"`
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
	NbLogger struct {
		Destination string
	}
	RequestHistory struct {
		Id int64 `xorm:"pk autoincr"`
		Ip string `xorm:"notnull default('')"`
		Service string `xorm:"notnull"`
		Api string `xorm:"notnull"`
		Param string `xorm:"notnull varchar(1024) default('')"`
		Result string `xorm:"notnull varchar(1024) default('')"`
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
var logger *log.Logger

func CreateService(filepath string) (ConsulService, error){
	// create access log
	s,err := logwriter.New("access.log")
	if err != nil{ return nil, err }
	logger = log.New(s,"", log.Lshortfile|log.LstdFlags)
	go func(){
		for {
			y, m, d := time.Now().Date()
			time.Sleep(time.Until(time.Date(y, m, d+1, 0, 0, 0, 0, time.Local)))
			s.Rotate()
		}
	}()
	log.Println("Access log file rotated")

	// Loading config
	config := new (NbConfig)
	cfile := multiconfig.NewWithPath(filepath)
	if err = cfile.Load(config);err != nil{ return nil, err }
	cfile.MustLoad(config)
	log.Println("Config file loaded")

	// setup db connection & validate it
	engine, err := xorm.NewEngine("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8",
		config.Database.Username, config.Database.Password,
		config.Database.Address, config.Database.Port, config.Database.Name))
	if err != nil {return nil, err}
	engine.SetMaxIdleConns(10)
	engine.SetMaxOpenConns(10)
	engine.SetMapper(core.GonicMapper{})
	log.Println("Database connected")
	// Init table: request_history
	err = engine.Sync2(new (RequestHistory))
	if err != nil {engine.Close();return nil, err}
	log.Println("RequestHistory data table synchronized")

	// Consul agent registration
	agent, err := servant.NewConsulClient(fmt.Sprintf("%s:%d",config.Service.Server,config.Service.Port))
	if err != nil{ engine.Close();return nil, err }
	log.Println("Consul agent connected.")

	// Consul service registration
	if config.Service.Autoregister {
		svcId,err := agent.Register(config.Service.Name, config.Service.Tags, 80)
		if err != nil{
			log.Printf("Consul service registration failed: %s \n",err.Error())
		}
		config.Service.Id = svcId
		log.Printf("Consul service registered, ServiceId = %s \n",svcId)
	}
	return &service{agent: agent,config: config,started: false,db: engine, topicno: 0,client: &http.Client{}},err
}
func (s *service)Config()*NbConfig{
	tmp := new(NbConfig)
	*tmp = * s.config
	return tmp
}
func (s *service)GetServiceUrl(service string, api string, query interface{})string{
	//service = s.agent.Service(service,nil)
	// TODO get service url
	return ""
}
func (s *service)Services()(map[string]*consul.AgentService,error){
	// TODO get alive service list from consul
	return s.agent.Services()
}
func (s *service)DeRegister(){
	if s.db !=nil{ s.db.Close() }
	log.Println("DB connection closed.")
	if s.messenger !=nil{ s.messenger.Close() }
	log.Println("AMQP listener closed.")
	if s.agent !=nil{ s.agent.DeRegister() }
	log.Println("Consul agent closed.")
}
func (s *service)CreateDataTable(bean ...interface{})(error){
	return s.db.Sync2(bean...)
}
func (s *service)InsertRecord(bean ...interface{})(int64, error){
	return s.db.Insert(bean...)
}
func (s *service)UpdateRecord(bean interface{}, conditions ...interface{})(int64, error){
	return s.db.Update(bean,conditions...)
}
func (s *service)DeleteRecord(bean interface{})(int64, error){
	return s.db.Delete(bean)
}
func (s *service)GetFirstRecord(bean interface{})(bool,error){
	return s.db.Get(bean)
}
func (s *service)FindRecords(bean interface{}, query interface{}, args ...interface{})(error){
	return s.db.Where(query,args).Find(bean)
}
func (s *service)RegisterMessageHandler(api string, callback func([]byte)([]byte,error)) (error){
	// create a mq agent if not exist
	if s.config.Amqp.Port == 0 || s.config.Amqp.Address == "" {
		return errors.New("Amqp address configuration is missing.")
	}
	if s.messenger == nil {
		msger, err := messenger.CreateMessenger(s.config.Amqp.Username, s.config.Amqp.Password, s.config.Amqp.Address, s.config.Amqp.Port)
		if err!=nil{
			return err
		}
		log.Println("AMQP connection established.")
		s.messenger = msger
	}

	// create & listen to queue/topic if not registered yet
	consId := fmt.Sprintf("%s_%s_%d",s.config.Amqp.Name,time.Now().Format("20060102.1504"),s.topicno)
	err := s.messenger.ListenWithFunc(consId,s.config.Amqp.Name,api,
		func(param []byte)([]byte){
			logger.Printf("RECV->AMQP: %s @ %s",api,string(param))
			s.db.Insert(RequestHistory{Service:s.config.Amqp.Name,Api:api,Param:string(param),Method:"amqp",Direction:"in"})
			res,_ := callback(param)
			return res
	})
	if err==nil{
		s.topicno = s.topicno +1
		log.Printf("AMQP listener registered, Exch = %s, Topic = %s, ConsumerId = %s \n",s.config.Amqp.Name,api,consId)
	}
	return err
}
func (s *service)RegisterHttpHandler(api string, method string, callback func([]byte)([]byte,error)){
	// register the api in http interface
	http.HandleFunc(api, func(resp http.ResponseWriter, req *http.Request){
		if req.Method==method{
			var pp []byte
			var err error
			if req.Method == GET{
				// get query parameter
				qry := make(map[string]string)
				for k,v := range req.URL.Query(){
					if len(v) > 0{}
					qry[k] = v[0]
				}
				pp,err = json.Marshal(qry)
			}else{
				// get post body
				pp = make([]byte, req.ContentLength)
				_,err = req.Body.Read(pp)
				defer req.Body.Close()
			}
			logger.Printf("RECV-REQ, %s, %s, %s, %s",req.RemoteAddr,req.Method,api,string(pp))
			if err != nil && err.Error() != "EOF"{
				logger.Printf("RECV-RES, %s, %s, %s, %s",req.RemoteAddr,req.Method,api,err.Error())
				resp.WriteHeader(400)
				return
			}
			s.db.Insert(RequestHistory{Ip:req.RemoteAddr,Service:s.config.Service.Name,Api:api,Param:string(pp),Method:method,Direction:"in"})
			result,err := callback(pp)
			if err!=nil{
				status,_:=strconv.Atoi(err.Error())
				resp.WriteHeader(status)
			}
			logger.Printf("RECV-RES, %s, %s, %s, %s",req.RemoteAddr,req.Method,api,result)
			resp.Write(result)
		}else{
			resp.WriteHeader(404)
			//return &HTTPError{errors.New("No good!"), http.StatusBadRequest}
		}
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
		err:=s.messenger.SendTo(service,api,param)
		logger.Printf("SEND-REQ, %s, AMQP, %s, %s",service,api,param)
		if err!=nil{
			logger.Printf("RECV-RES, %s, AMQP, %s, %s",service,api,err.Error())
			return err
		}
		logger.Printf("RECV-RES, %s, AMQP, %s, SUCCESS",service,api)
		return nil
	}
	case GET,POST,PUT:{
		logger.Printf("SEND-REQ, %s, %s, %s, %s",service,method,api,param)
		var url string
		if method==GET{
			url = s.GetServiceUrl(service,api,param)
		}else{
			url = s.GetServiceUrl(service,api,nil)
		}
		req, _ := http.NewRequest(method, url, bytes.NewBuffer([]byte(param)))
		resp, err := s.client.Do(req)
		if err != nil {
			logger.Printf("RECV-RES, %s, %s, %s, %s",service,method,api,err.Error())
			return err
		}
		defer resp.Body.Close()
		body, _ := ioutil.ReadAll(resp.Body)
		logger.Printf("RECV-RES, %s, %s, %s, %s:%s",service,method,api,resp.Status,body)
	}
	}
	return errors.New("未知的请求方式")
}