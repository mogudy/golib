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
)

func exitOnError(err error, msg string){
	if err != nil {
		log.Fatalf("%s: %s", msg, err.Error())
	}
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
		stat,_ := json.Marshal(StandardResponse{400,"Param invalid error",err.Error()})
		return stat,errors.New("400")
	}
	if err := taskVal.Validate(task); err != nil {
		stat,_ := json.Marshal(StandardResponse{400,"Param invalid error",err.Error()})
		return stat,errors.New("400")
	}

	task.HandledBy = ca.Config().Application["id"]
	if _,err := ca.InsertRecord(task);err!=nil {
		stat,_ := json.Marshal(StandardResponse{500,"Db insertion error",err.Error()})
		return stat,errors.New("500")
	}
	addTask(task)
	res,_ := json.Marshal(StandardResponse{200,"Success","Cron task added"})
	return res,nil
}
// 列出定时任务调度器中的所有任务
func listGet([]byte)([]byte,error){
	everyone := make([]GenericTask, 0)
	err := ca.FindRecords(&everyone)
	if err!=nil {
		stat,_ := json.Marshal(StandardResponse{500,"Db query error",err.Error()})
		return stat,errors.New("500")
	}
	res,err := json.Marshal(everyone)
	if err!=nil {
		stat,_ := json.Marshal(StandardResponse{500,"Db result parsing error",err.Error()})
		return stat,errors.New("500")
	}
	return res,nil
}
// 获得定时任务调度器中的某任务详情（Uuid）
func jobGet(param []byte)([]byte,error){
	//task,err := parseTask(param)
	task := new(GenericTask)
	if err := json.Unmarshal(param, task);err!=nil{
		stat,_ := json.Marshal(StandardResponse{400,"Param invalid error",err.Error()})
		return stat,errors.New("400")
	}

	record,err := ca.GetFirstRecord(task)
	if err!=nil {
		stat,_ := json.Marshal(StandardResponse{500,"Db query error",err.Error()})
		return stat,errors.New("500")
	}
	res,err := json.Marshal(record)
	if err!=nil {
		stat,_ := json.Marshal(StandardResponse{500,"Db result parsing error",err.Error()})
		return stat,errors.New("500")
	}
	return res,nil
}
// 删除调度器中的任务（任务uuid）
func delPost(param []byte)([]byte,error){
	taskVal.SetTag("deltask")
	task := new(GenericTask)
	if err := json.Unmarshal(param, task);err!=nil{
		stat,_ := json.Marshal(StandardResponse{400,"Param invalid error",err.Error()})
		return stat,errors.New("400")
	}
	if err := taskVal.Validate(task); err != nil {
		stat,_ := json.Marshal(StandardResponse{400,"Param invalid error",err.Error()})
		return stat,errors.New("400")
	}

	if _,err := ca.DeleteRecord(task);err!=nil {
		stat,_ := json.Marshal(StandardResponse{500,"Db record delete error",err.Error()})
		return stat,errors.New("500")
	}
	res,_ := json.Marshal(StandardResponse{200,"Success","Cron task added"})
	return res,nil
}

func addTask(t *GenericTask){
	// Immediate, Deferred or AMQP
	execTime := time.Unix(t.StartedAt,0)
	nowTime := time.Now()

	if t.Schedule == ""{
		// Single Run
		if execTime.After(nowTime.Add(2*time.Second)){
			// Deferred
			tmm.RunFuncAt(execTime, func(){
					// Deferred Single run
					ca.SendRequest(t.Method, t.Service, t.Api, t.Param)
			}, t.Summary)
		}else{
			// Immediate
			ca.SendRequest(t.Method, t.Service, t.Api, t.Param)
		}
	}else{
		// Cron
		if execTime.After(nowTime.Add(2*time.Minute)){
			// Deferred
			tmm.RunFuncAt(execTime.Add(1*time.Minute), func(){
				// Deferred Addto cron
				addCron(t)
			}, t.Summary)
		}else{
			// Immediate
			addCron(t)
		}
	}
}
func addCron(t *GenericTask)error{
	cr.AddFunc(t.Uuid,t.Schedule, func(){
		if t.Repetition > 0 && t.Repetition > t.Count || t.Repetition<=0{
			// repetation limits
			if ca.SendRequest(t.Method,t.Service,t.Api,t.Param)==nil{
				t.Count = t.Count+1
				ca.UpdateRecord(t,GenericTask{Id:t.Id})
			}
		}else{
			cr.DeleteJob(t.Uuid)
			ca.DeleteRecord(t)
			log.Printf("Task(uuid): %s has been deleted. \n",t.Uuid)
		}
	})
	return nil
}
func parseTask(param []byte) (*GenericTask,error){
	task := new(GenericTask)
	err := json.Unmarshal(param, task)
	if err!=nil{
		return nil, err
	}
	return task, nil
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
	exitOnError(err, "服务创建失败")
	defer ca.DeRegister()
	log.Println("服务加载成功")

	err = ca.CreateDataTable(new(GenericTask))
	exitOnError(err, "数据库创建失败")
	log.Println("数据库加载成功")

	// TODO load unhandled task from DB
	log.Println("历史任务加载成功")

	err = ca.RegisterMessageHandler("add",func(msg []byte)([]byte,error){
		log.Printf("received message: %s \n",string(msg))
		return []byte(""),nil
	})
	exitOnError(err,"消息队列注册失败")
	log.Printf("消息监听器注册成功：%s \n", ca.Config().Amqp.Name)
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
