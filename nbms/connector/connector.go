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
)

type(
	ConsulService interface {
		Config()*NbConfig
		DeRegister()
		CreateDataTable(bean ...interface{})error
		InsertRecord(bean ...interface{})(int64, error)
		UpdateRecord(bean interface{}, conditions ...interface{})(int64, error)
		DeleteRecord(bean interface{})(int64, error)
		FindRecords(bean interface{}, conditions ...interface{})(error)
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
	log.Println("Query table synchronized")

	// Register Service & Health check
	agent, err := servant.NewConsulClient(fmt.Sprintf("%s:%d",config.Service.Server,config.Service.Port))
	if err != nil{ engine.Close();return nil, err }
	err = agent.Register(fmt.Sprintf("%s-%s",config.Service.Name,time.Now().Format("2006010215")), config.Service.Tags, 80)
	if err != nil{ agent.DeRegister();engine.Close();return nil, err }
	log.Println("Consul agent registered")

	return &service{agent: agent,config: config,started: false,db:engine,msgp:0},nil
}
func (s *service)Config()*NbConfig{
	tmp := new(NbConfig)
	*tmp = * s.config
	return tmp
}
func (s *service)DeRegister(){
	if s.db !=nil{ s.db.Close() }
	if s.messenger !=nil{ s.messenger.Close() }
	if s.agent !=nil{ s.agent.DeRegister() }
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
func (s *service)FindRecords(bean interface{}, cond ...interface{})(error){
	return s.db.Find(bean, cond...)
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
	err := s.messenger.ListenWithFunc(fmt.Sprintf("%s_%s_%d",s.config.Amqp.Name,time.Now().Format("06010215"),s.msgp),
		s.config.Amqp.Name,api,
		func(param []byte)([]byte){
			logger.Printf("RECV->AMQP: %s @ %s",api,string(param))
			s.db.Insert(RequestHistory{Service:s.config.Amqp.Name,Api:api,Param:string(param),Method:"amqp",Direction:"in"})
			res,_ := callback(param)
			return res
	})
	if err==nil{
		s.msgp = s.msgp+1
		log.Printf("AMQP listen to topic: %s \n",api)
	}


	return err
}
func (s *service)RegisterHttpHandler(api string, method string, callback func([]byte)([]byte,error)){
	// register the api in http interface
	http.HandleFunc(api, func(resp http.ResponseWriter, req *http.Request){
		if req.Method==method{
			if req.Method == GET{
				// TODO get query parameter
			}else{
				// TODO get post body
			}
			pp := make([]byte, req.ContentLength)
			_,err := req.Body.Read(pp)
			defer req.Body.Close()
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
		// TODO http request
		logger.Printf("RECV-RES, %s, %s, %s, SUCCESS",service,method,api)
	}
	}
	return errors.New("未知的请求方式")
}