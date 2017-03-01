package servant

import (
	"fmt"
	consul "github.com/hashicorp/consul/api"
	"time"
)

type ConsulAgent interface {
	Service(service string, tag string) ([]*consul.ServiceEntry, *consul.QueryMeta, error)
	Register(service string, tags []string, port int) (string,error)
	DeRegister() error
	Services(query string)(map[string]*consul.AgentService,error)
}

type agent struct {
	consul *consul.Client
	service string
	check string
}

//NewConsul returns a Client interface for given consul address
func NewConsulClient(addr string) (ConsulAgent, error) {
	config := consul.DefaultConfig()
	config.Address = addr
	c, err := consul.NewClient(config)
	if err != nil {
		return nil, err
	}
	return &agent{consul: c}, nil
}

// Register a service with consul local agent
func (c *agent) Register(name string, tags []string, port int) (string,error) {
	//svcid := fmt.Sprintf("%s-%s", name, strings.Join(tags,"."))
	svcid := fmt.Sprintf("%s-%s", name, time.Now().Format("2006010215"))
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
	go ttl(c.consul.Agent(), chkid)
	return svcid,nil
}

func ttl(agent *consul.Agent, id string){
	for {
		agent.PassTTL(id, "")
		time.Sleep(30 * time.Second)
	}
}

// DeRegister a service with consul local agent
func (c *agent) DeRegister() error {
	c.consul.Agent().CheckDeregister(c.check)
	return c.consul.Agent().ServiceDeregister(c.service)
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
func (c *agent) Services(query string)(map[string]*consul.AgentService,error){
	return c.consul.Agent().Services()
}
