package servant

import (
	"fmt"
	consul "github.com/hashicorp/consul/api"
	"time"
	"log"
)

type ConsulAgent interface {
	Service(service string, tag string) ([]*consul.ServiceEntry, *consul.QueryMeta, error)
	Register(service string, tags []string, port int) (string,error)
	DeRegister() error
	Services()(map[string]*consul.AgentService,error)
}

type agent struct {
	consul *consul.Client
	svcNames []string
	chkNames []string
	aliveService map[string]*consul.AgentService
	aliveSvcErr error
}

//NewConsul returns a Client interface for given consul address
func NewConsulClient(addr string) (ConsulAgent, error) {
	config := consul.DefaultConfig()
	config.Address = addr
	c, err := consul.NewClient(config)
	if err != nil {
		return nil, err
	}
	return &agent{consul: c,svcNames:make([]string,0),chkNames:make([]string,0)}, nil
}

// Register a service with consul local agent
func (c *agent) Register(name string, tags []string, port int) (string,error) {
	//svcid := fmt.Sprintf("%s-%s", name, strings.Join(tags,"."))
	svcid := fmt.Sprintf("%s-%s", name, time.Now().Format("20060102.1504"))
	reg := &consul.AgentServiceRegistration{
		ID:   svcid,
		Name: name,
		Tags: tags,
		Port: port,
	}
	err := c.consul.Agent().ServiceRegister(reg)
	if err != nil {
		return "",err
	}
	c.svcNames = append(c.svcNames,svcid)
	chkid := fmt.Sprintf("%s-ttl", svcid)
	chkr := &consul.AgentCheckRegistration{
		ID: chkid,
		Name: name+"-ttl",
		ServiceID: svcid,
		AgentServiceCheck: consul.AgentServiceCheck{TTL:"30s"},
	}
	err = c.consul.Agent().CheckRegister(chkr)
	if err != nil {
		return svcid,err
	}
	c.chkNames = append(c.chkNames,chkid)
	go c.ttl()
	return svcid,nil
}

func (c *agent)ttl(){
	for {
		// Health check for all registered checks
		sk := 0
		for _,v :=range c.chkNames{
			if c.consul.Agent().PassTTL(v,"") == nil{sk = sk+1}
		}
		log.Printf("TTL: %s / %s \n",sk,len(c.chkNames))
		log.Println("================ Service List ================")
		// Update service list
		healthList,metaList,err := c.consul.Health().State(consul.HealthPassing,nil)
		for _,v :=range healthList{
			log.Printf("Health: %+v",v)
			//svcInfo := c.consul.Catalog().Service()
		}
		log.Println("---------- Meta ----------")
		log.Printf("Meta: %+v",metaList)
		log.Println("---------- Error ----------")
		if err!=nil{log.Printf("Err: %s",err.Error())}
		log.Println("================ Service Ends ================")
		time.Sleep(29 * time.Second)
	}
}

// DeRegister a service with consul local agent
func (c *agent) DeRegister() error {
	//c.consul.Agent().CheckDeregister(c.check)
	//return c.consul.Agent().ServiceDeregister(c.service)
	return nil
}

// Service return a service
func (c *agent) Service(service, tag string) ([]*consul.ServiceEntry, *consul.QueryMeta, error) {
	passingOnly := true
	addrs, meta, err := c.consul.Health().Service(service, tag, passingOnly, nil)
	if len(addrs) == 0 && err == nil {
		return nil, nil, fmt.Errorf("service ( %s ) was not found", service)
	}
	if err != nil {
		return nil, nil, err
	}
	return addrs, meta, nil
}
func (c *agent) Services()(map[string]*consul.AgentService,error){
	return c.aliveService,c.aliveSvcErr
}
