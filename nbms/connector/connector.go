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
)

type(
	ConsulService interface {
		DeRegister()
		//BatchUpdateBatchUpdate(query string, params [][]interface{}, action func()(error))(error)
		//SqlUpdate(query string, params[]interface{})(error)
		//SqlSelect(query string, params[]interface{}, cols []*interface{})
		RegisterMessageHandler(topic string, callback func([]byte)([]byte))(error)
		RegisterHttpHandler(api string, isPost bool, callback func([]byte)([]byte))
		StartServer(remoteShutdown bool)
		SendRequest(service string, api string, param string, method uint32)(error)
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
//func (s *service)BatchUpdate(query string, params [][]interface{}, action func()(error)) (error){
//	// Start Transaction
//	tx, err := s.db.Begin()
//	if err!=nil{
//		return errors.New("Transaction init Error: "+err.Error())
//	}
//	// added to database
//	stmt, err := tx.Prepare(query)
//	if err!=nil{
//		return errors.New("Query preparation Error: "+err.Error())
//	}
//	defer stmt.Close()
//	for _,stms := range params{
//		_, err := stmt.Exec(stms...)
//		if err!=nil{
//			tx.Rollback()
//			return errors.New("Sql execution Error: "+err.Error())
//		}
//	}
//	err = action()
//	if err!=nil{
//		tx.Rollback()
//		return errors.New("Action execution Error: "+err.Error())
//	}
//	tx.Commit()
//	return nil
//}
//func (s *service)SqlUpdate(query string, params[]interface{}) (error){
//	_, err := s.db.Exec("select user,password,host from mysql.user",params...)
//	if err!=nil{
//		return errors.New("Sql execution Error: "+err.Error())
//	}
//	return nil
//}
//func (s *service)SqSelect(query string, params[]interface{}, cols []*interface{}) (error){
//	rows, err := s.db.Query("select user,password,host from mysql.user",params...)
//	if err!=nil{
//		return errors.New("Sql execution Error: "+err.Error())
//	}
//	defer rows.Close()
//	err = rows.Scan(cols...)
//	if err!=nil{
//		return errors.New("Result extract Error: "+err.Error())
//	}
//	return nil
//}
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
func (s *service)RegisterHttpHandler(api string, isPost bool, callback func([]byte)([]byte)){
	// register the api in http interface
	http.HandleFunc(api, func(resp http.ResponseWriter, req *http.Request){
		pp := make([]byte, req.ContentLength)
		_,err := req.Body.Read(pp)
		defer req.Body.Close()
		if err != nil && err.Error() != "EOF"{
			log.Printf("API: %s, error: %s, param: %s",api,err,pp)
			return
		}
		resp.Write(callback(pp))
	})
}
func (s *service)StartServer(remoteShutdown bool){
	if !s.started {
		var haltch = make(chan bool)
		if remoteShutdown{
			http.HandleFunc("/shutdown",func(resp http.ResponseWriter, req *http.Request){
				haltch <- true
				resp.Write([]byte("{\"result\":\"success\",\"code\":200}"))
			})
		}
		go http.ListenAndServe(":80", nil)
		s.started = true

		select {
		case <-haltch:{
			log.Println("System exit")
		}
		}
	}
}
func (s *service)SendRequest(service string, api string, param string, method uint32)(error){
	return nil
}