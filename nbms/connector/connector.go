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
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
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
		db        *sql.DB
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
)
func CreateService(filepath string) (ConsulService, error){
	// Loading config
	config := new (NbConfig)
	cfile := multiconfig.NewWithPath(filepath)
	err := cfile.Load(config)
	if err != nil{ return nil, err }
	cfile.MustLoad(config)

	// setup db connection & validate it
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		config.Database.Username, config.Database.Password,
		config.Database.Address, config.Database.Port, config.Database.Name))
	if err != nil {return nil, err}
	//defer db.Close()
	err = db.Ping()
	if err != nil {return nil, err}

	// Register Service & Health check
	agent, err := servant.NewConsulClient(fmt.Sprintf("%s:%d",config.Service.Server,config.Service.Port))
	if err != nil{ return nil, err }
	agent.Register(fmt.Sprintf("%s-%s",config.Service.Name,time.Now().Format("2006010215")), config.Service.Tags, 80)
	//defer agent.DeRegister()

	return &service{agent: agent,config: config,started: false,db:db,msgp:0},nil
}

func (s *service)DeRegister(){
	defer s.messenger.Close()
	defer s.agent.DeRegister()
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
	err := s.messenger.ListenWithFunc(fmt.Sprintf("%s_%s_%d",s.config.Service.Name,time.Now().Format("06010215"),s.msgp), s.config.Service.Name,topic, callback)
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