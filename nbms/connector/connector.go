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
)

type(
	ConsulService interface {
		RegisterMessageHandler(string, func([]byte)[]byte)(error)
		RegisterHttpHandler(string, bool, func([]byte)([]byte))
		StartServer(bool)
	}
	service struct {
		agent servant.ConsulAgent
		config *NbConfig
		messenger messenger.Connector
		started bool
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
		Server string
		Port uint32
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

	// Register Service & Health check
	agent, err := servant.NewConsulClient(fmt.Sprintf("%s:%d",config.Service.Server,config.Service.Port))
	if err != nil{ return nil, err }
	agent.Register(fmt.Sprintf("%s-%s",config.Service.Name,time.Now().Format("2006010215")), config.Service.Tags, 80)
	defer agent.DeRegister()

	return &service{agent:agent,config:config,started:false},nil
}

func (s *service)RegisterMessageHandler(topic string, callback func([]byte)([]byte)) (error){
	// create a mq agent if not exist
	if s.config.Amqp.Port == 0 || s.config.Amqp.Server == "" {
		return errors.New("Amqp Server Configuration is missing.")
	}
	if s.messenger == nil {
		msger, err := messenger.CreateMessenger(s.config.Service.Name, s.config.Amqp.Username, s.config.Amqp.Password, s.config.Amqp.Server, s.config.Amqp.Port)
		if err!=nil{
			return err
		}
		s.messenger = msger
	}

	// create & listen to queue/topic if not registered yet
	err := s.messenger.ConsumeQueue(topic, callback)
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
		go http.ListenAndServe(":8080", nil)
		s.started = true

		select {
		case <-haltch:{
			log.Println("System exit")
		}
		}
	}
}
