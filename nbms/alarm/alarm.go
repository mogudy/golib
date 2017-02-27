package alarm

import (
	"time"
	"log"
)

type (
	Timer interface {
		RunFuncAt(target time.Time,action func(), name string) (string, error)
		ListAlarms()map[string]string
		CountAlarms()int
	}
	spec struct{
		alive map[string]string
	}
)
func CreateAlarm() (Timer){
	return &spec{map[string]string{}}
}
func (s *spec)RunFuncAt(target time.Time,action func(), name string)(string,error){
	nn := time.Now()
	dd := target.Sub(nn)
	switch {
	case dd <= 0:
			action()
			log.Printf("Execute task: %s", name)
	case dd > 0:
		{
			// TODO deferred execution
			time.AfterFunc(dd, func() {
				action()
				log.Printf("Execute task: %s", name)
				delete(s.alive, name+"-"+nn.String())
			})
			s.alive[name+"-"+nn.String()] = target.String()
			log.Printf("Scheduled task: %s @ %s", name, target)
		}
	}
	return name+"-"+nn.String(),nil
}
func (s *spec)ListAlarms()map[string]string{
	return s.alive
}
func (s *spec)CountAlarms()int{
	return len(s.alive)
}