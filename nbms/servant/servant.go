package servant

import (
	"fmt"
	consul "github.com/hashicorp/consul/api"
	"strings"
	"time"
)

type ConsulAgent interface {
	Service(string, string) ([]*consul.ServiceEntry, *consul.QueryMeta, error)
	Register(string, []string, int) (error)
	DeRegister() error
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
func (c *agent) Register(name string, tags []string, port int) error {
	svcid := fmt.Sprintf("%s-%s", name, strings.Join(tags,"."))
	reg := &consul.AgentServiceRegistration{
		ID:   svcid,
		Name: name,
		Tags: tags,
		Port: port,
	}
	err := c.consul.Agent().ServiceRegister(reg)
	if err != nil {
		return err
	}
	chkid := fmt.Sprintf("%s-%s-ttl", name, strings.Join(tags,"."))
	chkr := &consul.AgentCheckRegistration{
		ID: chkid,
		Name: name+"-ttl",
		ServiceID: svcid,
		AgentServiceCheck: consul.AgentServiceCheck{TTL:"30s"},
	}
	err = c.consul.Agent().CheckRegister(chkr)
	if err != nil {
		return err
	}
	go ttl(c.consul.Agent(), chkid)
	return nil
}

func ttl(agent *consul.Agent, id string){
	for {
		agent.PassTTL(id, "")
		time.Sleep(30 * time.Second)
	}
}

// DeRegister a service with consul local agent
func (c *agent) DeRegister() error {
	err := c.consul.Agent().CheckDeregister(c.check)
	if err != nil {
		return err
	}
	err = c.consul.Agent().ServiceDeregister(c.service)
	if err != nil {
		return err
	}
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
