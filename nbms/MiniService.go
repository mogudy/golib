package main

import (
	"log"
	"github.com/mogudy/golib/nbms/connector"
	"github.com/bububa/cron"
	"time"
	"encoding/json"
	"github.com/mogudy/golib/nbms/alarm"
	"github.com/mogudy/golib/nbms/logwriter"
	"errors"
	"gopkg.in/validator.v2"
	"github.com/go-xorm/builder"
	consulapi "github.com/hashicorp/consul/api"
)

func exitOnError(err error, msg string){
	if err != nil {
		log.Fatalf("%s: %s", msg, err.Error())
	}
}
func buildResp(code int, msg string, result interface{})([]byte,error){
	stat,_ := json.Marshal(StandardResponse{code,msg,result})
	if code == 200{
		return stat,nil
	}
	return stat,errors.New(string(code))
}

var ca connector.ConsulService
var cr *cron.Cron
var tmm alarm.Timer

type (
	GenericTask struct{
		Id         int64 `xorm:"pk autoincr" json:",omitempty"`
		StartedAt  int64 `xorm:"notnull" addtask:"nonzero"`
		Schedule   string `xorm:"notnull" json:",omitempty"`
		Repetition int32 `xorm:"notnull default(0)" addtask:"nonzero"`
		Count      int32 `xorm:"notnull default(0)" json:",omitempty"`
		Service    string `xorm:"notnull" addtask:"nonzero"`
		Api        string `xorm:"notnull" addtask:"nonzero"`
		Param      string `xorm:"notnull varchar(1024) default('')" json:",omitempty"`
		Method     string `xorm:"notnull" addtask:"nonzero"`
		Summary    string `xorm:"notnull" addtask:"nonzero"`
		CreatedAt  time.Time `xorm:"notnull created" json:",omitempty"`
		HandledAt  time.Time `xorm:"notnull updated" json:",omitempty"`
		HandledBy  string `xorm:"notnull" json:",omitempty"`
		Version    int32 `xorm:"notnull version" json:",omitempty"`
		Uuid       string `xorm:"notnull unique" addtask:"nonzero" deltask:"nonzero"`
		DeletedAt time.Time `xorm:"deleted" json:",omitempty"`
	}
	StandardResponse struct {
		Code int
		Msg string
		Result interface{}
	}
)

// 新增任务 addCron(StartedAt, Schedule, Repetition, Service, Api, param, Method, Summary, Uuid)
var taskVal = validator.NewValidator()
func addPost(param []byte)([]byte,error){
	//task,err := parseTask(param)
	taskVal.SetTag("addtask")
	task := new(GenericTask)
	if err := json.Unmarshal(param, task);err!=nil{
		return buildResp(400,"Param invalid: ",err)
	}
	if err := taskVal.Validate(task); err != nil {
		return buildResp(400,"Param invalid: ",err)
	}

	task.HandledBy = ca.Config().Application["id"]
	if _,err := ca.InsertRecord(task);err!=nil {
		return buildResp(500,"Db query i error: ",err)
	}
	addTask(task)
	return buildResp(200,"Success: ","Cron task added")
}
// 列出定时任务调度器中的所有任务
func listGet([]byte)([]byte,error){
	everyone := make([]GenericTask, 0)
	err := ca.FindRecords(&everyone,nil,nil)
	if err!=nil {
		return buildResp(500,"Db query s error: ",err)
	}
	res,err := json.Marshal(everyone)
	if err!=nil {
		return buildResp(500,"Db result parsing error: ",err)
	}
	return res,nil
}
// 获得定时任务调度器中的某任务详情（Uuid）
func jobGet(param []byte)([]byte,error){
	task := new(GenericTask)
	if err := json.Unmarshal(param, task);err!=nil{
		return buildResp(400,"Param invalid: ",err)
	}
	record,err := ca.GetFirstRecord(task)
	if err!=nil {
		return buildResp(500,"Db query s error: ",err)
	}
	if record{
		return buildResp(200,"Success",task)
	}else{
		return buildResp(204,"No Content","")
	}
}
// 删除调度器中的任务（任务uuid）
func delPost(param []byte)([]byte,error){
	taskVal.SetTag("deltask")
	task := new(GenericTask)
	if err := json.Unmarshal(param, task);err!=nil{
		return buildResp(400,"Param invalid: ",err)
	}
	if err := taskVal.Validate(task); err != nil {
		return buildResp(400,"Param invalid: ",err)
	}

	return delJob(task)
}
func delJob(t *GenericTask)([]byte,error){
	cr.DeleteJob(t.Uuid)
	if _,err := ca.DeleteRecord(t);err!=nil {
		return buildResp(500,"Db query d error: ",err)
	}
	return buildResp(200,"Success: ","Cron task deleted.")
}

func addTask(t *GenericTask){
	// Immediate, Deferred or AMQP
	execTime := time.Unix(t.StartedAt,0)
	nowTime := time.Now()

	if t.Schedule == ""{
		// Single Run
		if execTime.After(nowTime.Add(5*time.Second)){
			// Deferred to run if task was scheduled 5 secs later
			tmm.RunFuncAt(execTime, func(){
				runTask(t)
			}, t.Summary)
		}else{
			// Immediate run if task was scheduled to run in 5 secs
			runTask(t)
		}
	}else{
		// Cron task
		if execTime.After(nowTime.Add(5*time.Second)){
			// Deferred to run if task was scheduled 5 secs later
			tmm.RunFuncAt(execTime.Add(5*time.Second), func(){
				addCron(t)
			}, t.Summary)
		}else{
			// Immediate run if task was scheduled to run in 5 secs
			addCron(t)
		}
	}
}
func addCron(t *GenericTask)error{
	cr.AddFunc(t.Uuid,t.Schedule, func(){
		if t.Repetition > 0 && t.Repetition > t.Count || t.Repetition<=0{
			// repetation limits
			runTask(t)
		}else{
			_,err := delJob(t)
			if err!=nil{log.Printf("Task(uuid): %s deletion failed. \n",t.Uuid)}else{log.Printf("Task(uuid): %s has been deleted. \n",t.Uuid)}
		}
	})
	return nil
}
func runTask(t *GenericTask)error{
	err:=ca.SendRequest(t.Method,t.Service,t.Api,t.Param)
	if err==nil{
		t.Count = t.Count+1
		ca.UpdateRecord(t,GenericTask{Id:t.Id})
	}
	return err
}

func loadTask(handler map[string]*consulapi.AgentService)error{
	// extract service handler names
	keys := make([]string, 0, len(handler))
	for _,v:=range handler{
		keys = append(keys,v.Service)
	}

	// load task that does not belong to existing handler
	tasks := make([]GenericTask,0)
	if err := ca.FindRecords(&tasks,builder.NotIn("handled_by",keys));err!=nil{
		return err
	}
	for _,v:=range tasks{
		// Added task
		v.HandledBy = ca.Config().Application["ID"]
		ca.UpdateRecord(v,GenericTask{Id:v.Id})
		addTask(&v)
		log.Printf("Task(uuid:%s) has been loaded to queue from db.",v.Uuid)
	}
	return nil
}

func main(){
	s,err := logwriter.New("app.log")
	exitOnError(err, "日志创建失败")
	//log.SetOutput(s)

	cr = cron.New()
	cr.Start()
	defer cr.Stop()
	tmm = alarm.CreateAlarm()
	log.Println("定时器加载成功")
	cr.AddFunc("app_rotate_log","0 0 0 * * *",func(){
		if err=s.Rotate();err!=nil{
			log.Println(err.Error())
		}
	})

	ca,err = connector.CreateService("config.toml")
	exitOnError(err, "consul服务创建失败")
	defer ca.DeRegister()
	log.Println("consul服务加载成功")

	err = ca.CreateDataTable(new(GenericTask))
	exitOnError(err, "数据库创建失败")
	log.Println("数据库加载成功")

	err = ca.RegisterMessageHandler("add",func(msg []byte)([]byte,error){
		log.Printf("received message: %s \n",string(msg))
		return []byte(""),nil
	})
	exitOnError(err,"消息队列注册失败")
	log.Printf("消息监听器注册成功：%s \n", ca.Config().Amqp.Name)

	// load unhandled task from DB
	svcs,err := ca.Services()
	loadTask(svcs)
	log.Println("历史任务加载成功")

	ca.RegisterHttpHandler("/add", connector.POST, addPost)
	log.Printf("http接口注册成功：%s \n", "/add")
	ca.RegisterHttpHandler("/del", connector.POST, delPost)
	log.Printf("http接口注册成功：%s \n", "/del")
	ca.RegisterHttpHandler("/list", connector.GET, listGet)
	log.Printf("http接口注册成功：%s \n", "/list")
	ca.RegisterHttpHandler("/job", connector.GET, jobGet)
	log.Printf("http接口注册成功：%s \n", "/job")

	if err = ca.StartServer(true);err!=nil{
		exitOnError(err,"http接口注册失败")
	}
}
